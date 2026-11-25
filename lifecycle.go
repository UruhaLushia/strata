package strata

import (
	"context"
	"errors"
	"slices"
	"time"
)

func (db *DB) Close() error {
	db.loopMu.Lock()
	cancel, done := db.loopCancel, db.loopDone
	db.loopCancel = nil
	db.loopDone = nil
	db.loopMu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	var err error
	if db.writer {
		err = db.flushLocked(true)
		if err == nil {
			db.durableSeq = db.appendSeq
		}
	}
	if err == nil && db.writer {
		err = syncData(db.data)
	}
	db.closed = true
	if db.view != nil {
		db.view.release()
		db.view = nil
	} else {
		if db.mapped != nil {
			_ = db.mapped.close()
		}
		if db.data != nil {
			_ = db.data.Close()
		}
	}
	if db.lock != nil {
		_ = db.lock.Close()
	}
	db.reclaimSlots()
	db.dict.reclaim()
	return err
}

func (db *DB) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	db.loopMu.Lock()
	if db.loopCancel != nil {
		db.loopMu.Unlock()
		return
	}
	c, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	db.loopCancel, db.loopDone = cancel, done
	db.loopMu.Unlock()
	go func() {
		defer close(done)
		flush := time.NewTicker(db.opts.FlushInterval)
		syncT := time.NewTicker(db.opts.SyncInterval)
		compact := time.NewTicker(db.opts.FlushInterval)
		defer flush.Stop()
		defer syncT.Stop()
		defer compact.Stop()
		for {
			select {
			case <-c.Done():
				return
			case <-flush.C:
				_ = db.Flush()
			case <-syncT.C:
				_ = db.Sync()
			case <-compact.C:
				db.autoCompact()
			}
		}
	}()
}

func (db *DB) Refresh() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errors.New("strata: database is closed")
	}
	if db.writer {
		return nil
	}
	probe, err := openData(db.path, true)
	if err != nil {
		return err
	}
	head, err := readSuper(probe)
	if err != nil {
		_ = probe.Close()
		return err
	}
	if head.incompat&^knownIncompat != 0 {
		_ = probe.Close()
		return ErrIncompatFormat
	}
	if head.uuid == db.head.uuid && head.gen == db.head.gen {
		_ = probe.Close()
		pos, max, scanErr := scanWAL(db.data, db.walPos, false, db.applyFrame)
		if scanErr != nil {
			return scanErr
		}
		db.walPos = pos
		if max > db.appendSeq {
			db.appendSeq = max
			db.allocatedSeq = max
			db.durableSeq = max
		}
		db.lastRefreshSize = pos
		return nil
	}
	f := probe
	mapped, err := mapPrefix(f, head.walOffset, db.shouldMmap())
	if err != nil {
		_ = f.Close()
		return err
	}
	next := &DB{
		path:   db.path,
		opts:   db.opts,
		policy: db.policy,
		writer: false,
		data:   f,
		head:   head,
		mapped: mapped,
		view:   newFileView(f, mapped, head.sections[secTSBlocks]),
	}
	if err := next.loadLocked(); err != nil {
		next.view.release()
		return err
	}

	oldSeries := db.series
	mergedSeries := make(map[uint16]*Series, max(len(next.series), len(oldSeries)))
	for id, fresh := range next.series {
		if old := oldSeries[id]; old != nil {
			if old.configInferred && !fresh.configInferred {
				old.cfg = fresh.cfg
				old.configInferred = false
			} else if old.configInferred {
				old.cfg.SummaryOps, old.cfg.RowOps = fresh.cfg.SummaryOps, fresh.cfg.RowOps
			}
			old.resetEpoch = fresh.resetEpoch
			old.rules = fresh.rules
			mergedSeries[id] = old
			continue
		}
		fresh.db = db
		mergedSeries[id] = fresh
	}
	for id, old := range oldSeries {
		if _, ok := mergedSeries[id]; !ok {
			old.resetEpoch = 0
			old.rules = nil
			mergedSeries[id] = old
		}
	}

	oldView := db.view
	db.data = next.data
	db.view = next.view
	db.head = next.head
	db.mapped = next.mapped
	db.series = mergedSeries
	db.dict = next.dict
	db.sealed = next.sealed
	db.sealedByRes = next.sealedByRes
	db.optional = next.optional
	db.live = next.live
	db.appendSeq = next.appendSeq
	db.allocatedSeq = next.allocatedSeq
	db.durableSeq = next.durableSeq
	db.sealedSeq = next.sealedSeq
	db.order = next.order
	db.walPos = next.walPos
	db.pending = next.pending
	db.pendingDict = next.pendingDict
	db.pendingBytes = next.pendingBytes
	db.lastRefreshSize = next.lastRefreshSize
	db.kvCatalog = next.kvCatalog
	db.kvRegions = next.kvRegions
	db.kvOverlay = next.kvOverlay
	db.kvSST = next.kvSST
	db.kvRev = next.kvRev
	db.kvNext = next.kvNext
	db.invert = next.invert
	if oldView != nil {
		oldView.release()
	}
	return nil
}

func (db *DB) Flush() error { db.mu.Lock(); defer db.mu.Unlock(); return db.flushLocked(false) }

func (db *DB) flushLocked(force bool) error {
	if db.closed {
		return errors.New("strata: database is closed")
	}
	if !db.writer {
		return ErrReadOnly
	}
	if len(db.pending) == 0 {
		return nil
	}
	groups := make(map[int64][]*bucket)
	for _, b := range db.pending {
		groups[b.ts] = append(groups[b.ts], b)
	}
	dicts := db.pendingDict
	mins := make([]int64, 0, len(groups))
	for m := range groups {
		mins = append(mins, m)
	}
	slices.Sort(mins)
	prevPos, prevAppend, prevAllocated := db.walPos, db.appendSeq, db.allocatedSeq
	for _, m := range mins {
		bs := groups[m]
		db.allocatedSeq++
		payload := encodeDelta(dicts, bs)
		pos, err := appendFrame(db.data, db.walPos, frameDelta, db.allocatedSeq, payload)
		if err != nil {
			_ = db.data.f.Truncate(prevPos)
			db.walPos, db.appendSeq, db.allocatedSeq = prevPos, prevAppend, prevAllocated
			return err
		}
		db.walPos = pos
		db.appendSeq = db.allocatedSeq
		dicts = nil
	}
	db.pending = make(map[bucketKey]*bucket)
	db.pendingDict = nil
	db.pendingBytes = 0
	db.lastRefreshSize = db.walPos
	if force {
		return syncData(db.data)
	}
	return nil
}

func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errors.New("strata: database is closed")
	}
	if !db.writer {
		return ErrReadOnly
	}
	if err := db.flushLocked(false); err != nil {
		return err
	}
	if err := db.syncFile(); err != nil {
		return err
	}
	db.durableSeq = db.appendSeq
	return nil
}
