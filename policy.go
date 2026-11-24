package strata

import (
	"context"
	"runtime"

	"github.com/klauspost/compress/zstd"
)

const (
	maxActiveScratchCap   = 512 << 10
	maxRetainedScratchCap = 128 << 10
	maxActiveArenaCap     = 1 << 20
	maxRetainedArenaCap   = 128 << 10
)

type memPolicy struct {
	queryBytes           int64
	compactBytes         int64
	pendingBytes         int64
	walCompactBytes      int64
	queryWorkers         int
	compactWorkers       int
	queryBatchRows       int
	maxConcurrentQueries int
	zstdEncoderWindow    int
	dictPageCacheBytes   int
	dictHotBytes         int
	allowWriterMmap      bool
	constrained          bool
}

func compileMemPolicy(opts Options) memPolicy {
	procs := max(runtime.GOMAXPROCS(0), 1)
	var p memPolicy
	if opts.MemoryProfile == MemoryConstrained {
		p = memPolicy{
			queryBytes:         2 << 20,
			compactBytes:       4 << 20,
			pendingBytes:       256 << 10,
			walCompactBytes:    512 << 10,
			queryWorkers:       1,
			compactWorkers:     1,
			queryBatchRows:     512,
			dictPageCacheBytes: 128 << 10,
			dictHotBytes:       192 << 10,
			constrained:        true,
		}
	} else {
		p = memPolicy{
			queryBytes:         8 << 20,
			compactBytes:       16 << 20,
			pendingBytes:       512 << 10,
			walCompactBytes:    2 << 20,
			queryWorkers:       min(procs, 2),
			compactWorkers:     min(procs, 2),
			queryBatchRows:     1024,
			dictPageCacheBytes: 256 << 10,
			dictHotBytes:       256 << 10,
			allowWriterMmap:    opts.UseMmap,
		}
	}
	b := opts.MemoryBudget
	if b.QueryBytes > 0 {
		p.queryBytes = max(b.QueryBytes, 1<<20)
	}
	if b.CompactBytes > 0 {
		p.compactBytes = max(b.CompactBytes, 2<<20)
	}
	if b.PendingBytes > 0 {
		p.pendingBytes = max(b.PendingBytes, 64<<10)
	}
	if opts.WALCompactBytes > 0 {
		p.walCompactBytes = opts.WALCompactBytes
	}
	if b.QueryWorkers > 0 {
		p.queryWorkers = b.QueryWorkers
	}
	maxWorkersByBudget := max(int(p.queryBytes/(512<<10)), 1)
	p.queryWorkers = clampInt(p.queryWorkers, 1, maxWorkersByBudget)
	if b.CompactWorkers > 0 {
		p.compactWorkers = b.CompactWorkers
	}
	p.compactWorkers = max(p.compactWorkers, 1)
	if b.QueryBatchRows > 0 {
		p.queryBatchRows = b.QueryBatchRows
	}
	p.queryBatchRows = clampInt(p.queryBatchRows, 64, 4096)
	p.maxConcurrentQueries = clampInt(int(p.queryBytes/(3<<20)), 1, 4)
	if p.compactBytes <= 8<<20 {
		p.zstdEncoderWindow = 64 << 10
	} else {
		p.zstdEncoderWindow = 128 << 10
	}
	return p
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func (db *DB) shouldMmap() bool {
	if !db.opts.UseMmap {
		return false
	}
	if db.opts.ReadOnly {
		return true
	}
	return db.policy.allowWriterMmap
}

type querySlot struct {
	colScratch []byte
	colArena   []byte
	tagIDs     [8][]uint32
	values     [4][]int64
	masks      []uint8
	dec        *zstd.Decoder
}

func newQuerySlot() *querySlot {
	return &querySlot{}
}

func (s *querySlot) ensureDec() *zstd.Decoder {
	if s.dec == nil {
		s.dec, _ = zstd.NewReader(nil, zstd.WithDecoderLowmem(true))
	}
	return s.dec
}

func (s *querySlot) decodeZstdInto(raw, dst []byte) ([]byte, error) {
	return s.ensureDec().DecodeAll(raw, dst[:0])
}

func (s *querySlot) decodeZstd(raw []byte) ([]byte, error) {
	plain, err := s.ensureDec().DecodeAll(raw, s.colScratch[:0])
	if cap(plain) <= maxActiveScratchCap {
		s.colScratch = plain[:0]
	} else {
		s.colScratch = nil
	}
	return plain, err
}

func (s *querySlot) ensureBatch(rows, arity int) {
	for dim := range arity {
		if cap(s.tagIDs[dim]) < rows {
			s.tagIDs[dim] = make([]uint32, rows)
		} else {
			s.tagIDs[dim] = s.tagIDs[dim][:rows]
		}
	}
	for m := range 4 {
		if cap(s.values[m]) < rows {
			s.values[m] = make([]int64, rows)
		} else {
			s.values[m] = s.values[m][:rows]
		}
	}
	if cap(s.masks) < rows {
		s.masks = make([]uint8, rows)
	} else {
		s.masks = s.masks[:rows]
	}
}

func (s *querySlot) ensureArena(size int) []byte {
	if cap(s.colArena) < size {
		if size <= maxActiveArenaCap {
			s.colArena = make([]byte, size)
			return s.colArena
		}
		return make([]byte, size)
	}
	s.colArena = s.colArena[:size]
	return s.colArena
}

func (s *querySlot) resetAfterQuery() {
	if cap(s.colScratch) > maxRetainedScratchCap {
		s.colScratch = nil
	} else {
		s.colScratch = s.colScratch[:0]
	}
	if cap(s.colArena) > maxRetainedArenaCap {
		s.colArena = nil
	} else {
		s.colArena = s.colArena[:0]
	}
	for dim := range 8 {
		if cap(s.tagIDs[dim]) > 4096 {
			s.tagIDs[dim] = nil
		}
	}
	for m := range 4 {
		if cap(s.values[m]) > 4096 {
			s.values[m] = nil
		}
	}
	if cap(s.masks) > 4096 {
		s.masks = nil
	}
}

func (s *querySlot) close() {
	if s == nil {
		return
	}
	if s.dec != nil {
		s.dec.Close()
		s.dec = nil
	}
	s.colScratch = nil
	s.colArena = nil
	for dim := range 8 {
		s.tagIDs[dim] = nil
	}
	for m := range 4 {
		s.values[m] = nil
	}
	s.masks = nil
}

func (db *DB) acquireQuery(ctx context.Context) (workers int, batchRows int, slot *querySlot, release func(), err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err = ctx.Err(); err != nil {
		return 0, 0, nil, nil, err
	}
	select {
	case db.querySem <- struct{}{}:
	case <-ctx.Done():
		return 0, 0, nil, nil, ctx.Err()
	}
	select {
	case slot = <-db.querySlots:
	default:
		slot = newQuerySlot()
	}
	workers = db.policy.queryWorkers
	batchRows = db.policy.queryBatchRows
	if db.compacting.Load() {
		workers = 1
		if batchRows > 512 {
			batchRows = 512
		}
	}
	release = func() {
		slot.resetAfterQuery()
		select {
		case db.querySlots <- slot:
		default:
			slot.close()
		}
		<-db.querySem
	}
	return workers, batchRows, slot, release, nil
}

func (db *DB) acquireLightRead(ctx context.Context) (slot *querySlot, release func(), err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err = ctx.Err(); err != nil {
		return nil, nil, err
	}
	select {
	case db.lightReadSem <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	select {
	case slot = <-db.lightReadSlots:
	default:
		slot = newQuerySlot()
	}
	release = func() {
		slot.resetAfterQuery()
		select {
		case db.lightReadSlots <- slot:
		default:
			slot.close()
		}
		<-db.lightReadSem
	}
	return slot, release, nil
}

func drainSlotChan(ch chan *querySlot) {
	if ch == nil {
		return
	}
	for {
		select {
		case slot := <-ch:
			slot.close()
		default:
			return
		}
	}
}

func (db *DB) reclaimSlots() {
	if db == nil {
		return
	}
	drainSlotChan(db.querySlots)
	drainSlotChan(db.lightReadSlots)
}

func (db *DB) reclaim() {
	if db == nil {
		return
	}
	db.reclaimSlots()
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return
	}
	db.dict.reclaim()
	view := db.view.acquire()
	db.mu.RUnlock()
	if view != nil {
		view.adviseDontNeed()
		view.release()
	}
}
