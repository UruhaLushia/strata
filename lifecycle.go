package strata

import (
	"context"
	"errors"
	"slices"
	"time"
)

var errDBClosed = errors.New("strata: database is closed")

func (db *DB) closeLoop() <-chan struct{} {
	db.loopMu.Lock()
	defer db.loopMu.Unlock()
	db.loopClosed = true
	db.loopHasNext, db.loopNextCtxDone = false, nil
	if db.loopStop != nil {
		close(db.loopStop)
		db.loopStop = nil
	}
	return db.loopDone
}

func (db *DB) Close() error {
	if done := db.closeLoop(); done != nil {
		<-done
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	var err error
	if db.writer {
		err = db.flushLocked()
		if err == nil {
			err = syncData(db.data)
		}
		if err == nil {
			db.durableSeq = db.appendSeq
		}
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
	if db.opts.FlushInterval <= 0 && db.opts.SyncInterval <= 0 {
		return
	}
	var ctxDone <-chan struct{}
	if ctx != nil {
		ctxDone = ctx.Done()
	}
	select {
	case <-ctxDone:
		return
	default:
	}
	db.loopMu.Lock()
	defer db.loopMu.Unlock()
	if db.loopClosed {
		return
	}
	if db.loopDone != nil {
		current := db.loopCtxDone
		if db.loopHasNext {
			current = db.loopNextCtxDone
		}
		select {
		case <-current:
			db.loopHasNext, db.loopNextCtxDone = true, ctxDone
		default:
		}
		return
	}
	stop, done := make(chan struct{}), make(chan struct{})
	db.loopStop, db.loopDone, db.loopCtxDone = stop, done, ctxDone
	go db.runLoop(ctxDone, stop, done)
}

func (db *DB) runLoop(ctxDone, stop <-chan struct{}, done chan struct{}) {
	var flush, syncT *time.Ticker
	var flushC, syncC <-chan time.Time
	if d := db.opts.FlushInterval; d > 0 {
		flush = time.NewTicker(d)
		flushC = flush.C
	}
	if d := db.opts.SyncInterval; d > 0 {
		syncT = time.NewTicker(d)
		syncC = syncT.C
	}
	for {
	active:
		for {
			select {
			case <-ctxDone:
				break active
			case <-stop:
				break active
			case <-flushC:
				if err := db.Flush(); err == ErrReadOnly || err == errDBClosed {
					break active
				}
				select {
				case <-ctxDone:
					break active
				case <-stop:
					break active
				default:
				}
				db.autoCompact()
			case <-syncC:
				if err := db.Sync(); err == ErrReadOnly || err == errDBClosed {
					break active
				}
			}
		}
		db.loopMu.Lock()
		next, hasNext := db.loopNextCtxDone, db.loopHasNext
		db.loopHasNext, db.loopNextCtxDone = false, nil
		if !db.loopClosed && hasNext {
			select {
			case <-next:
			default:
				ctxDone, db.loopCtxDone = next, next
				db.loopMu.Unlock()
				continue
			}
		}
		if flush != nil {
			flush.Stop()
		}
		if syncT != nil {
			syncT.Stop()
		}
		db.loopStop, db.loopDone, db.loopCtxDone = nil, nil, nil
		close(done)
		db.loopMu.Unlock()
		return
	}
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
	db.order = next.order
	db.walPos = next.walPos
	db.pending = next.pending
	db.pendingDict = next.pendingDict
	db.pendingBytes = next.pendingBytes
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

func (db *DB) Flush() error { db.mu.Lock(); defer db.mu.Unlock(); return db.flushLocked() }

func (db *DB) flushLocked() error {
	if db.closed {
		return errDBClosed
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
	return nil
}

func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.syncLocked()
}
