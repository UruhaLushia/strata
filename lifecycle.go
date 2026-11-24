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
	probe, err := openData(db.path, true)
	if err != nil {
		return err
	}
	head, err := readSuper(probe)
	_ = probe.Close()
	if err != nil {
		return err
	}
	if head.uuid == db.head.uuid && head.gen == db.head.gen {
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
	f, err := openData(db.path, db.opts.ReadOnly)
	if err != nil {
		return err
	}
	oldView := db.view
	mapped, err := mapPrefix(f, head.walOffset, db.shouldMmap())
	if err != nil {
		_ = f.Close()
		return err
	}
	db.data, db.head, db.mapped = f, head, mapped
	db.view = newFileView(f, mapped, head.sections[secTSBlocks])
	if oldView != nil {
		oldView.release()
	}
	return db.loadLocked()
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
	for _, m := range mins {
		bs := groups[m]
		db.allocatedSeq++
		payload := encodeDelta(dicts, bs)
		pos, err := appendFrame(db.data, db.walPos, frameDelta, db.allocatedSeq, payload)
		if err != nil {
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
