package strata

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"iter"
	"math"
	"slices"
	"time"
)

func (db *DB) Region(name string) (*Region, error) {
	if name == "" {
		return nil, ErrInvalidRegion
	}
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return nil, errors.New("strata: database is closed")
	}
	return &Region{db: db, name: name}, nil
}

func (db *DB) NewBatch() *Batch { return &Batch{db: db} }

func (r *Region) NewBatch() *Batch {
	if r == nil || r.db == nil || r.name == "" {
		return &Batch{err: ErrInvalidRegion}
	}
	return r.db.NewBatch().In(r)
}

func (r *Region) Get(key string) ([]byte, bool, error) {
	if r == nil || r.db == nil || r.name == "" {
		return nil, false, ErrInvalidRegion
	}
	db := r.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, false, errors.New("strata: database is closed")
	}
	id := db.kvCatalog[r.name]
	if id == 0 {
		return nil, false, nil
	}
	return db.getKVLocked(id, key)
}

func (r *Region) Put(key string, value []byte) error {
	return r.NewBatch().Put(key, value).Commit()
}

func (r *Region) PutTTL(key string, value []byte, ttl time.Duration) error {
	return r.NewBatch().PutTTL(key, value, ttl).Commit()
}

func (r *Region) Delete(key string) error {
	return r.NewBatch().Delete(key).Commit()
}

func (r *Region) Clear() error {
	return r.NewBatch().Clear().Commit()
}

func (r *Region) PutIfAbsent(key string, value []byte) (bool, error) {
	return r.condWrite(key, true, kvPut, value, func(_ []byte, exists bool) bool {
		return !exists
	})
}

func (r *Region) CompareAndSwap(key string, oldValue, newValue []byte) (bool, error) {
	return r.condWrite(key, false, kvPut, newValue, func(cur []byte, exists bool) bool {
		return exists && bytes.Equal(cur, oldValue)
	})
}

func (r *Region) CompareAndDelete(key string, oldValue []byte) (bool, error) {
	return r.condWrite(key, false, kvDelete, nil, func(cur []byte, exists bool) bool {
		return exists && bytes.Equal(cur, oldValue)
	})
}

func (r *Region) condWrite(key string, createIfMissing bool, kind byte, value []byte, match func(cur []byte, exists bool) bool) (bool, error) {
	if r == nil || r.db == nil || r.name == "" {
		return false, ErrInvalidRegion
	}
	db := r.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return false, errors.New("strata: database is closed")
	}
	if !db.writer {
		return false, ErrReadOnly
	}
	id := db.kvCatalog[r.name]
	var ops []kvOp
	if id == 0 {
		if !createIfMissing {
			return false, nil
		}
		var err error
		id, err = db.findFreeRegionIDLocked(max(db.kvNext, 0x100))
		if err != nil {
			return false, err
		}
		ops = append(ops, kvOp{
			kind:   kvPut,
			region: 0,
			key:    []byte(r.name),
			value:  []byte{byte(id), byte(id >> 8)},
		})
	} else {
		cur, ok, err := db.getKVLocked(id, key)
		if err != nil {
			return false, err
		}
		if !match(cur, ok) {
			return false, nil
		}
	}
	ops = append(ops, kvOp{
		kind:   kind,
		region: id,
		key:    []byte(key),
		value:  append([]byte(nil), value...),
	})
	if err := db.commitKVLocked(ops); err != nil {
		return false, err
	}
	return true, nil
}

type kvOverlayItem struct {
	key string
	val kvValue
}

func (r *Region) Scan(ctx context.Context, q KVScan, fn func(KVItem) error) error {
	if r == nil || r.db == nil || r.name == "" {
		return ErrInvalidRegion
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	lo := q.Prefix
	var hi string
	var hasHi bool
	if q.Prefix != "" {
		if lim := prefixLimit([]byte(q.Prefix)); lim != nil {
			hi = string(lim)
			hasHi = true
		}
	}
	if q.Start != "" && q.Start > lo {
		lo = q.Start
	}
	if q.End != "" && (!hasHi || q.End < hi) {
		hi = q.End
		hasHi = true
	}
	if hasHi && lo >= hi {
		return nil
	}

	db := r.db
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return errors.New("strata: database is closed")
	}
	id := db.kvCatalog[r.name]
	if id == 0 {
		db.mu.RUnlock()
		return nil
	}
	now := db.opts.Now().UTC().UnixNano()
	var clearRev uint64
	if reg := db.kvRegions[id]; reg != nil {
		clearRev = reg.clearRev
	}
	physLo := physicalKVKey(id, []byte(lo))
	var physHi []byte
	if hasHi {
		physHi = physicalKVKey(id, []byte(hi))
	} else {
		physHi = prefixLimit(physicalKVKey(id, nil))
	}
	var metas []kvSSTMeta
	for _, m := range db.kvSST {
		if bytes.Compare(m.last, physLo) < 0 {
			continue
		}
		if physHi != nil && bytes.Compare(m.first, physHi) >= 0 {
			break
		}
		metas = append(metas, m)
	}
	var view *fileView
	var kvBlocksSec, kvBlobsSec sectionDesc
	if len(metas) > 0 {
		view = db.view.acquire()
		kvBlocksSec = db.head.sections[0]
		kvBlobsSec = db.head.sections[1]
	}
	var overlay []kvOverlayItem
	for k, v := range db.kvOverlay {
		if k.region != id || v.modRev <= clearRev {
			continue
		}
		if k.key < lo || (hasHi && k.key >= hi) {
			continue
		}
		overlay = append(overlay, kvOverlayItem{key: k.key, val: v})
	}
	db.mu.RUnlock()

	if view != nil {
		defer view.release()
	}

	slices.SortFunc(overlay, func(a, b kvOverlayItem) int {
		return cmp.Compare(a.key, b.key)
	})

	var emitted int
	emit := func(item KVItem) (bool, error) {
		if err := ctx.Err(); err != nil {
			return true, err
		}
		if fn != nil {
			if err := fn(item); err != nil {
				if errors.Is(err, ErrStopScan) {
					return true, nil
				}
				return true, err
			}
		}
		emitted++
		return q.Limit > 0 && emitted >= q.Limit, nil
	}

	emitOverlay := func(e kvOverlayItem) (bool, error) {
		if e.val.deleted || kvExpired(e.val.expiresAtUnixNano, now) {
			return false, nil
		}
		item := KVItem{Key: e.key}
		if !q.KeysOnly {
			item.Value = append([]byte(nil), e.val.value...)
		}
		return emit(item)
	}

	oi := 0
	stopped := false
	for _, meta := range metas {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, err := readKVBlockFromView(view, kvBlocksSec, meta)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if e.region < id {
				continue
			}
			if e.region > id {
				stopped = true
				break
			}
			ek := string(e.key)
			if ek < lo {
				continue
			}
			if hasHi && ek >= hi {
				stopped = true
				break
			}
			for oi < len(overlay) && overlay[oi].key < ek {
				stop, err := emitOverlay(overlay[oi])
				oi++
				if err != nil {
					return err
				}
				if stop {
					return nil
				}
			}
			if oi < len(overlay) && overlay[oi].key == ek {
				stop, err := emitOverlay(overlay[oi])
				oi++
				if err != nil {
					return err
				}
				if stop {
					return nil
				}
				continue
			}
			if e.modRev <= clearRev || e.deleted || kvExpired(e.expiresAtUnixNano, now) {
				continue
			}
			item := KVItem{Key: ek}
			if !q.KeysOnly {
				if e.external {
					if err := ctx.Err(); err != nil {
						return err
					}
					val, err := readKVBlobFromView(view, kvBlobsSec, e)
					if err != nil {
						return err
					}
					item.Value = val
				} else {
					item.Value = append([]byte(nil), e.value...)
				}
			}
			stop, err := emit(item)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
		}
		if stopped {
			break
		}
	}

	for oi < len(overlay) {
		if err := ctx.Err(); err != nil {
			return err
		}
		stop, err := emitOverlay(overlay[oi])
		oi++
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
	}
	return nil
}

func (r *Region) ScanSeq(ctx context.Context, q KVScan) iter.Seq2[KVItem, error] {
	return func(yield func(KVItem, error) bool) {
		err := r.Scan(ctx, q, func(item KVItem) error {
			if !yield(item, nil) {
				return ErrStopScan
			}
			return nil
		})
		if err != nil {
			yield(KVItem{}, err)
		}
	}
}

func (b *Batch) In(r *Region) *Batch {
	if b == nil {
		return nil
	}
	if b.err != nil {
		return b
	}
	if r == nil || r.db == nil || r.name == "" || (b.db != nil && r.db != b.db) {
		b.err = ErrInvalidRegion
		return b
	}
	b.cur = r
	return b
}

func (b *Batch) Put(key string, value []byte) *Batch {
	if b == nil || b.err != nil {
		return b
	}
	if b.cur == nil {
		b.err = ErrInvalidRegion
		return b
	}
	b.ops = append(b.ops, kvOp{
		kind:  kvPut,
		key:   []byte(key),
		value: append([]byte(nil), value...),
		name:  b.cur.name,
	})
	return b
}

func (b *Batch) PutTTL(key string, value []byte, ttl time.Duration) *Batch {
	if b == nil || b.err != nil {
		return b
	}
	if b.cur == nil {
		b.err = ErrInvalidRegion
		return b
	}
	if ttl <= 0 {
		b.err = ErrInvalidTTL
		return b
	}
	b.ops = append(b.ops, kvOp{
		kind:   kvPut,
		key:    []byte(key),
		value:  append([]byte(nil), value...),
		name:   b.cur.name,
		ttl:    ttl,
		hasTTL: true,
	})
	return b
}

func (b *Batch) Delete(key string) *Batch {
	if b == nil || b.err != nil {
		return b
	}
	if b.cur == nil {
		b.err = ErrInvalidRegion
		return b
	}
	b.ops = append(b.ops, kvOp{
		kind: kvDelete,
		key:  []byte(key),
		name: b.cur.name,
	})
	return b
}

func (b *Batch) Clear() *Batch {
	if b == nil || b.err != nil {
		return b
	}
	if b.cur == nil {
		b.err = ErrInvalidRegion
		return b
	}
	b.ops = append(b.ops, kvOp{
		kind: kvClear,
		name: b.cur.name,
	})
	return b
}

func (b *Batch) Commit() error {
	if b == nil {
		return ErrInvalidRegion
	}
	if b.err != nil {
		return b.err
	}
	if b.db == nil {
		return ErrInvalidRegion
	}
	db := b.db
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errors.New("strata: database is closed")
	}
	if !db.writer {
		return ErrReadOnly
	}
	if len(b.ops) == 0 {
		return nil
	}
	nextID := max(db.kvNext, 0x100)
	var pendingIDs map[string]uint16
	commitOps := make([]kvOp, 0, len(b.ops))
	for _, op := range b.ops {
		if op.name == "" {
			return ErrInvalidRegion
		}
		id := db.kvCatalog[op.name]
		if id == 0 && pendingIDs != nil {
			id = pendingIDs[op.name]
		}
		if id == 0 {
			if op.kind == kvDelete || op.kind == kvClear {
				continue
			}
			var err error
			id, err = db.findFreeRegionIDLocked(nextID)
			if err != nil {
				return err
			}
			nextID = id + 1
			if pendingIDs == nil {
				pendingIDs = make(map[string]uint16)
			}
			pendingIDs[op.name] = id
			commitOps = append(commitOps, kvOp{
				kind:   kvPut,
				region: 0,
				key:    []byte(op.name),
				value:  []byte{byte(id), byte(id >> 8)},
			})
		}
		op.region = id
		commitOps = append(commitOps, op)
	}
	if len(commitOps) == 0 {
		return nil
	}
	return db.commitKVLocked(commitOps)
}

func (db *DB) findFreeRegionIDLocked(start uint16) (uint16, error) {
	id := max(start, 0x100)
	for id <= 0x7fff && db.kvRegions[id] != nil {
		id++
	}
	if id > 0x7fff {
		return 0, ErrInvalidRegion
	}
	return id, nil
}

func (db *DB) commitKVLocked(ops []kvOp) error {
	now := db.opts.Now().UTC()
	for i := range ops {
		if ops[i].hasTTL {
			nowNs := now.UnixNano()
			if nowNs > 0 && int64(ops[i].ttl) > math.MaxInt64-nowNs {
				ops[i].expiresAtUnixNano = math.MaxInt64
			} else {
				ops[i].expiresAtUnixNano = nowNs + int64(ops[i].ttl)
			}
			if ops[i].expiresAtUnixNano == 0 {
				ops[i].expiresAtUnixNano = 1
			}
		}
	}
	rev := db.kvRev + 1
	seq := db.allocatedSeq + 1
	payload := encodeKVBatch(rev, ops)
	prevPos, prevAppend, prevAllocated := db.walPos, db.appendSeq, db.allocatedSeq
	pos, err := appendFrame(db.data, db.walPos, frameKVBatch, seq, payload)
	if err != nil {
		_ = db.data.f.Truncate(prevPos)
		return err
	}
	if err = db.syncFile(); err != nil {
		_ = db.data.f.Truncate(prevPos)
		db.walPos, db.appendSeq, db.allocatedSeq = prevPos, prevAppend, prevAllocated
		return err
	}
	db.walPos, db.appendSeq, db.allocatedSeq = pos, seq, seq
	db.applyKVBatch(rev, ops)
	db.durableSeq = seq
	return nil
}

func (db *DB) applyKVBatch(rev uint64, ops []kvOp) {
	if rev > db.kvRev {
		db.kvRev = rev
	}
	for _, op := range ops {
		if op.region == 0 {
			if op.kind == kvPut && len(op.value) == 2 {
				id := binary.LittleEndian.Uint16(op.value)
				name := string(op.key)
				db.kvCatalog[name] = id
				db.kvRegions[id] = &kvRegion{id: id, name: name}
				if id >= db.kvNext {
					db.kvNext = id + 1
				}
			}
			continue
		}
		r := db.kvRegions[op.region]
		if r == nil {
			r = &kvRegion{id: op.region}
			db.kvRegions[op.region] = r
		}
		k := kvKey{region: op.region, key: string(op.key)}
		switch op.kind {
		case kvPut:
			db.kvOverlay[k] = kvValue{value: append([]byte(nil), op.value...), modRev: rev, expiresAtUnixNano: op.expiresAtUnixNano}
		case kvDelete:
			db.kvOverlay[k] = kvValue{modRev: rev, deleted: true}
		case kvClear:
			r.clearRev = rev
			for key, value := range db.kvOverlay {
				if key.region == op.region && value.modRev <= rev {
					delete(db.kvOverlay, key)
				}
			}
		}
	}
}
