package strata

import (
	"errors"
	"os"
	"time"
)

func (db *DB) Compact() error {
	if !db.compactMu.TryLock() {
		return nil
	}
	defer db.compactMu.Unlock()
	db.compacting.Store(true)
	defer db.compacting.Store(false)

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return errors.New("strata: database is closed")
	}
	if !db.writer {
		db.mu.Unlock()
		return ErrReadOnly
	}
	if err := db.flushLocked(false); err != nil {
		db.mu.Unlock()
		return err
	}
	oldHead, source := db.head, db.data
	db.fenceSeq = db.appendSeq
	nowSec := db.opts.Now().Unix()
	metas, err := scanWALFrameMetas(source, int64(oldHead.walOffset), db.walPos)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	fineTarget, hourTarget, dayTarget, sealedSeq, sealedKVRev, eligibleFrames := computeCompactWatermarks(metas, oldHead, db.fenceSeq, nowSec)
	var unsealedResetMinEpoch map[uint16]uint64
	for _, m := range metas {
		if m.seq <= sealedSeq || m.typ != frameReset || !m.hasTS {
			continue
		}
		if unsealedResetMinEpoch == nil {
			unsealedResetMinEpoch = make(map[uint16]uint64)
		}
		if prev, ok := unsealedResetMinEpoch[m.series]; !ok || m.resetOldEpoch < prev {
			unsealedResetMinEpoch[m.series] = m.resetOldEpoch
		}
	}
	walRefs, err := collectWALBucketRefs(source, eligibleFrames)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	kvBlocks, err := db.buildKVBlocks(sealedKVRev)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	catalogRaw := encodeKVCatalog(db, sealedKVRev)
	dictSnap := snapshotDictForCompact(db, fineTarget, unsealedResetMinEpoch)
	optional := append([]optionalSection(nil), db.optional...)
	seriesSnaps := make(map[uint16]*seriesSnap, len(db.series))
	for id, s := range db.series {
		seriesSnaps[id] = &seriesSnap{id: id, cfg: s.cfg, rules: append([]resetRule(nil), s.rules...)}
	}
	runBytes := db.compactInvertRunBytes
	if runBytes <= 0 {
		runBytes = clampInt(int(db.policy.compactBytes/4), 64<<10, 2<<20)
	}
	sourceView := db.view.acquire()
	viewReleased := false
	releaseView := func() {
		if !viewReleased {
			viewReleased = true
			sourceView.release()
		}
	}
	defer releaseView()

	plan := &compactPlan{
		dbPath:         db.path,
		oldHead:        oldHead,
		sourceView:     sourceView,
		oldSealed:      append([]sealedMeta(nil), db.sealed...),
		oldInvert:      append([]invertChunkMeta(nil), db.invert...),
		series:         seriesSnaps,
		eligibleFrames: eligibleFrames,
		walRefs:        walRefs,
		nowSec:         nowSec,
		fineTarget:     fineTarget,
		hourTarget:     hourTarget,
		dayTarget:      dayTarget,
		ret1mCutoff:    nowSec - int64(db.opts.Retention1m/time.Second),
		ret1hCutoff:    nowSec - int64(db.opts.Retention1h/time.Second),
		ret1dCutoff:    nowSec - int64(db.opts.Retention1d/time.Second),
		kvBlocks:       kvBlocks,
		catalogRaw:     catalogRaw,
		dictSnap:       dictSnap,
		optional:       optional,
		level:          db.opts.CompressionLevel,
		policy:         db.policy,
		invertRunBytes: runBytes,
	}
	gen, uuid := oldHead.gen+1, oldHead.uuid
	db.mu.Unlock()

	tmp := db.path + ".tmp"
	_ = os.Remove(tmp)
	tf, err := openData(tmp, false)
	if err != nil {
		return err
	}
	fail := func(e error) error {
		_ = tf.Close()
		_ = os.Remove(tmp)
		return e
	}
	if err = writeSupers(tf, superblock{version: 1, gen: gen, walOffset: dataStart, created: oldHead.created, uuid: uuid}); err != nil {
		return fail(err)
	}
	walStart, descs, err := writeCompactFile(tf, plan)
	if err != nil {
		return fail(err)
	}
	if err = syncData(tf); err != nil {
		return fail(err)
	}
	if db.compactHook != nil {
		db.compactHook()
	}

	copyBuf := make([]byte, 64<<10)
	db.mu.RLock()
	pass1Pos := db.walPos
	db.mu.RUnlock()
	newWal, err := copyWALRangeStreaming(tf, source, int64(oldHead.walOffset), pass1Pos, sealedSeq, walStart, copyBuf)
	if err != nil {
		return fail(err)
	}
	if err = syncData(tf); err != nil {
		return fail(err)
	}

	db.mu.Lock()
	if err = db.flushLocked(false); err != nil {
		db.mu.Unlock()
		return fail(err)
	}
	finalSeq, finalPos := db.appendSeq, db.walPos
	newWal, err = copyWALRangeStreaming(tf, source, pass1Pos, finalPos, sealedSeq, newWal, copyBuf)
	if err != nil {
		db.mu.Unlock()
		return fail(err)
	}
	_ = newWal
	newHead := oldHead
	newHead.gen, newHead.sections = gen, descs
	newHead.walOffset, newHead.sealedWalSeq, newHead.sealedKVRev = uint64(walStart), sealedSeq, sealedKVRev
	newHead.watermark, newHead.compacted = fineTarget, db.opts.Now().Unix()
	err = writeSupers(tf, newHead)
	if err == nil {
		err = syncData(tf)
	}
	if err == nil {
		err = tf.Close()
	}
	releaseView()
	if err == nil {
		previousView := db.view
		db.data = nil
		db.mapped = nil
		db.view = nil
		err = renameAtomic(tmp, db.path)
		if err != nil {
			db.data, db.mapped, db.view = previousView.data, previousView.mapped, previousView
		} else {
			db.view = previousView
		}
	}
	if err == nil {
		err = syncParent(filepathDir(db.path))
	}
	if err != nil {
		if db.data == nil {
			oldView := db.view
			db.data, _ = openData(db.path, false)
			if db.data != nil {
				db.mapped, _ = mapPrefix(db.data, db.head.walOffset, db.shouldMmap())
				db.view = newFileView(db.data, db.mapped, db.head.sections[secTSBlocks])
				if oldView != nil {
					oldView.release()
				}
			}
		}
		db.mu.Unlock()
		return err
	}
	oldView := db.view
	newData, openErr := openData(db.path, false)
	if openErr == nil {
		var newMapped mapView
		newMapped, openErr = mapPrefix(newData, newHead.walOffset, db.shouldMmap())
		if openErr != nil {
			_ = newData.Close()
		} else {
			db.data, db.head, db.sealedSeq, db.mapped = newData, newHead, sealedSeq, newMapped
			db.view = newFileView(newData, newMapped, newHead.sections[secTSBlocks])
			if oldView != nil {
				oldView.release()
			}
		}
	}
	err = openErr
	if err == nil {
		err = db.loadLocked()
		db.durableSeq = finalSeq
	}
	db.mu.Unlock()
	return err
}

func filepathDir(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == '\\' {
			if i == 0 {
				return path[:1]
			}
			return path[:i]
		}
	}
	return "."
}
