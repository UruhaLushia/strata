package strata

import (
	"errors"
	"os"
	"path/filepath"
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
	kvPass := canPassthroughKV(oldHead, sealedKVRev)
	var kvBlocks []kvSSTBlock
	var catalogRaw []byte
	if !kvPass {
		kvBlocks, err = db.buildKVBlocks(sealedKVRev)
		if err != nil {
			db.mu.Unlock()
			return err
		}
		catalogRaw = encodeKVCatalog(db, sealedKVRev)
	}
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
		kvPassthrough:  kvPass,
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
	if err != nil {
		_ = tf.Close()
		_ = os.Remove(tmp)
		db.mu.Unlock()
		return err
	}
	previousView := db.view
	db.data = nil
	db.mapped = nil
	db.view = nil
	if err = renameAtomic(tmp, db.path); err != nil {
		if previousView != nil {
			db.data, db.mapped, db.view = previousView.data, previousView.mapped, previousView
		}
		_ = os.Remove(tmp)
		db.mu.Unlock()
		return err
	}
	syncErr := syncParent(filepath.Dir(db.path))
	newData, openErr := openData(db.path, false)
	if openErr == nil {
		var newMapped mapView
		newMapped, openErr = mapPrefix(newData, newHead.walOffset, db.shouldMmap())
		if openErr != nil {
			_ = newData.Close()
		} else {
			db.data, db.head, db.sealedSeq, db.mapped = newData, newHead, sealedSeq, newMapped
			db.view = newFileView(newData, newMapped, newHead.sections[secTSBlocks])
			if previousView != nil {
				previousView.release()
			}
		}
	}
	loadOrOpenErr := openErr
	if loadOrOpenErr == nil {
		if loadOrOpenErr = db.loadLocked(); loadOrOpenErr == nil {
			db.durableSeq = finalSeq
		}
	} else if previousView != nil {
		db.data, db.mapped, db.view = previousView.data, previousView.mapped, previousView
	}
	if loadOrOpenErr != nil {
		db.writer = false
	}
	db.mu.Unlock()
	return errors.Join(loadOrOpenErr, syncErr)
}
