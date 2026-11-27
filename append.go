package strata

import (
	"errors"
	"math"
	"time"
)

func (s *Series) Append(ts time.Time, summary Summary, rows ...Row) error {
	return s.append(ts, summary, rows, false)
}

func (s *Series) AppendSync(ts time.Time, summary Summary, rows ...Row) error {
	return s.append(ts, summary, rows, true)
}

func (s *Series) append(ts time.Time, summary Summary, rows []Row, syncNow bool) error {
	if s == nil || s.db == nil {
		return errors.New("strata: nil series")
	}
	db := s.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errors.New("strata: database is closed")
	}
	if !db.writer {
		return ErrReadOnly
	}
	for _, row := range rows {
		if len(row.Tags) != s.cfg.Arity {
			return errors.New("strata: row arity mismatch")
		}
	}
	summary = summary.normalized()
	delta := &bucket{
		series: s.id, ts: minuteTS(ts), epoch: s.resetEpoch,
		arity: uint8(s.cfg.Arity), summaryOps: s.cfg.SummaryOps, rowOps: s.cfg.RowOps,
		summary: summary,
	}
	if len(rows) > 0 {
		delta.rows = make([]idRow, len(rows))
	}
	// Resolve external tags before reserving any new IDs or publishing the bucket.
	for dim := range s.cfg.Arity {
		d := db.dict.dims[dictDimKey{series: s.id, dim: uint8(dim)}]
		if d == nil || d.ext == nil {
			continue
		}
		for i, row := range rows {
			tag := row.Tags[dim]
			if tag == "" {
				continue
			}
			id, fromSST, err := d.lookupExt(tag)
			if err != nil {
				return err
			}
			if fromSST {
				d.cacheHot(tag, id, db.dict.hotBudget)
			}
			delta.rows[i].tags[dim] = id
		}
	}
	db.order++
	delta.order = db.order
	var addedBytes int64 = 128
	for i, row := range rows {
		row = row.normalized()
		r := &delta.rows[i]
		r.initMask = row.Mask
		for j, tag := range row.Tags {
			if r.tags[j] != 0 {
				continue
			}
			id, newID, err := db.dict.internAfterLookup(s.id, uint8(j), tag)
			if err != nil {
				return err
			}
			r.tags[j] = id
			if newID {
				db.pendingDict = append(db.pendingDict, dictEntry{id: id, series: s.id, dim: uint8(j), value: tag})
				addedBytes += int64(len(tag)) + 24
			}
		}
		r.values = row.Values
		addedBytes += 72
	}
	key := bucketKey{series: s.id, minute: delta.ts, epoch: delta.epoch}
	liveExisted := db.live[key] != nil
	db.mergeLive(delta, !liveExisted)
	if p := db.pending[key]; p == nil {
		if !liveExisted {
			db.pending[key] = cloneBucket(delta)
		} else {
			db.pending[key] = delta
		}
	} else {
		mergeSummary(&p.summary, delta.summary, s.cfg.SummaryOps, delta.order, p.order)
		mergeIDRows(&p.rows, delta.rows, s.cfg.RowOps, delta.order, p.order)
		if delta.order > p.order {
			p.order = delta.order
		}
	}
	db.pendingBytes += addedBytes
	if syncNow {
		return db.syncLocked()
	}
	if db.policy.pendingBytes > 0 && db.pendingBytes >= db.policy.pendingBytes {
		return db.flushLocked()
	}
	return nil
}

func (db *DB) syncLocked() error {
	if err := db.flushLocked(); err != nil {
		return err
	}
	if err := db.syncFile(); err != nil {
		return err
	}
	db.durableSeq = db.appendSeq
	return nil
}

func (s *Series) ResetCurrentDay(loc *time.Location) error {
	if loc == nil {
		loc = time.UTC
	}
	db := s.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errors.New("strata: database is closed")
	}
	if !db.writer {
		return ErrReadOnly
	}
	now := db.opts.Now()
	local := now.In(loc)
	y, m, d := local.Date()
	start := time.Date(y, m, d, 0, 0, 0, 0, loc).UTC().Unix()
	to := minuteTS(now) + 60
	return s.resetLocked(start, to)
}

func (s *Series) ResetAll() error {
	db := s.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errors.New("strata: database is closed")
	}
	if !db.writer {
		return ErrReadOnly
	}
	return s.resetLocked(math.MinInt64, math.MaxInt64)
}

func (s *Series) resetLocked(from, to int64) error {
	db := s.db
	if err := db.flushLocked(); err != nil {
		return err
	}
	old := s.resetEpoch
	newEpoch := old + 1
	prevPos, prevAppend, prevAllocated := db.walPos, db.appendSeq, db.allocatedSeq
	db.allocatedSeq++
	frame := resetFrame{series: s.id, seq: db.allocatedSeq, oldEpoch: old, newEpoch: newEpoch, from: from, to: to}
	pos, err := appendFrame(db.data, db.walPos, frameReset, frame.seq, encodeReset(frame))
	if err != nil {
		_ = db.data.f.Truncate(prevPos)
		db.walPos, db.appendSeq = prevPos, prevAppend
		db.allocatedSeq = prevAllocated
		return err
	}
	db.walPos = pos
	db.appendSeq = frame.seq
	if err = db.syncFile(); err != nil {
		_ = db.data.f.Truncate(prevPos)
		db.walPos, db.appendSeq, db.allocatedSeq = prevPos, prevAppend, prevAllocated
		return err
	}
	db.durableSeq = db.appendSeq
	s.resetEpoch = newEpoch
	s.rules = append(s.rules, resetRule{oldEpoch: old, from: from, to: to})
	for k := range db.pending {
		if k.series == s.id {
			delete(db.pending, k)
		}
	}
	if from == math.MinInt64 && to == math.MaxInt64 {
		db.resetSeriesDictAndLiveLocked(s.id, old)
		if len(db.pendingDict) > 0 {
			kept := db.pendingDict[:0]
			for _, e := range db.pendingDict {
				if e.series != s.id {
					kept = append(kept, e)
				}
			}
			clear(db.pendingDict[len(kept):])
			db.pendingDict = kept
		}
		if len(db.pending) == 0 && len(db.pendingDict) == 0 {
			db.pendingDict = nil
			db.pendingBytes = 0
		}
	}
	return nil
}
