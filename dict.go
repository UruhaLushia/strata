package strata

import (
	"errors"
	"sort"
	"sync"
	"unsafe"
)

const (
	dictMapThreshold         = 65536
	dictSortedMergeThreshold = 64
)

type dictDim struct {
	arena        []byte
	offsets      []uint32
	extra        map[uint32]string
	tailBase     uint32
	nextID       uint32
	count        int
	ext          *dictExtDim
	revMu        sync.RWMutex
	byKey        map[string]uint32
	sorted       []uint32
	sortedNextID uint32
	tailByKey    map[string]uint32
	hot          *dictHotCache
}

type dictDimSnap struct {
	arena        []byte
	offsets      []uint32
	extra        map[uint32]string
	tailBase     uint32
	nextID       uint32
	count        int
	ext          *dictExtDim
	sorted       []uint32
	sortedNextID uint32
}

func hasExtraOverride(extra map[uint32]string, id uint32) bool {
	if extra == nil {
		return false
	}
	_, ok := extra[id]
	return ok
}

type dictionary struct {
	dims           map[dictDimKey]*dictDim
	pageCache      *dictPageCache
	hotBudget      int
	snapshot       bool
	snapshotSeries uint16
	snapshotDims   [8]dictDimSnap
}

func newDict() dictionary {
	return newDictWithPolicy(memPolicy{dictPageCacheBytes: 256 << 10, dictHotBytes: 256 << 10})
}

func newDictWithPolicy(p memPolicy) dictionary {
	return dictionary{
		dims:      make(map[dictDimKey]*dictDim),
		pageCache: newDictPageCache(p.dictPageCacheBytes),
		hotBudget: p.dictHotBytes,
	}
}

func newDictDim() *dictDim {
	return &dictDim{
		offsets: make([]uint32, 2),
		nextID:  1,
	}
}

func (dim *dictDim) resolveArena(id uint32) string {
	if dim == nil || id == 0 || id < dim.tailBase {
		return ""
	}
	idx := int(id - dim.tailBase)
	if idx < 0 || idx+1 >= len(dim.offsets) {
		return ""
	}
	lo, hi := dim.offsets[idx], dim.offsets[idx+1]
	if lo == hi {
		return ""
	}
	return unsafe.String(&dim.arena[lo], hi-lo)
}

func (dim *dictDim) resolve(id uint32) string {
	if dim == nil || id == 0 {
		return ""
	}
	if dim.extra != nil {
		if v, ok := dim.extra[id]; ok {
			return v
		}
	}
	if dim.ext != nil && id < dim.tailBase {
		return dim.ext.resolve(id)
	}
	return dim.resolveArena(id)
}

func (s *dictDimSnap) resolveArena(id uint32) string {
	if id == 0 || id < s.tailBase {
		return ""
	}
	idx := int(id - s.tailBase)
	if idx < 0 || idx+1 >= len(s.offsets) {
		return ""
	}
	lo, hi := s.offsets[idx], s.offsets[idx+1]
	if lo == hi {
		return ""
	}
	return unsafe.String(&s.arena[lo], hi-lo)
}

func (s *dictDimSnap) resolve(id uint32) string {
	if id == 0 {
		return ""
	}
	if s.extra != nil {
		if v, ok := s.extra[id]; ok {
			return v
		}
	}
	if s.ext != nil && id < s.tailBase {
		return s.ext.resolve(id)
	}
	return s.resolveArena(id)
}

func (dim *dictDim) appendValue(id uint32, value string) {
	if len(dim.offsets) == 0 {
		if dim.ext != nil {
			dim.offsets = []uint32{0}
		} else {
			dim.offsets = []uint32{0, 0}
			dim.nextID = 1
		}
	}
	if id == dim.nextID {
		dim.arena = append(dim.arena, value...)
		dim.offsets = append(dim.offsets, uint32(len(dim.arena)))
		dim.nextID = id + 1
		if value != "" {
			dim.count++
		}
		return
	}
	if id > dim.nextID {
		wantOffs := int(id-dim.tailBase) + 1
		curEnd := uint32(len(dim.arena))
		for len(dim.offsets) < wantOffs {
			dim.offsets = append(dim.offsets, curEnd)
		}
		dim.arena = append(dim.arena, value...)
		dim.offsets = append(dim.offsets, uint32(len(dim.arena)))
		dim.nextID = id + 1
		if value != "" {
			dim.count++
		}
		return
	}
	prev := dim.resolve(id)
	if prev == value {
		return
	}
	if dim.extra == nil {
		dim.extra = make(map[uint32]string)
	}
	dim.extra[id] = value
	if prev == "" && value != "" {
		dim.count++
	}
}

func (dim *dictDim) ensureReverseLocked() {
	if dim.sorted != nil {
		return
	}
	if dim.count > dictMapThreshold {
		dim.buildSorted()
		return
	}
	if dim.byKey != nil {
		return
	}
	dim.byKey = make(map[string]uint32, dim.count)
	for id := uint32(1); id < dim.nextID; id++ {
		if v := dim.resolve(id); v != "" {
			dim.byKey[v] = id
		}
	}
}

func (dim *dictDim) lookupLocked(value string) uint32 {
	for id, v := range dim.extra {
		if v == value {
			return id
		}
	}
	if dim.sorted != nil {
		idx := sort.Search(len(dim.sorted), func(i int) bool {
			return dim.resolveArena(dim.sorted[i]) >= value
		})
		for i := idx; i < len(dim.sorted); i++ {
			id := dim.sorted[i]
			if dim.resolveArena(id) != value {
				break
			}
			if hasExtraOverride(dim.extra, id) {
				continue
			}
			return id
		}
		startID := max(dim.sortedNextID, uint32(1))
		for id := startID; id < dim.nextID; id++ {
			if hasExtraOverride(dim.extra, id) {
				continue
			}
			if v := dim.resolveArena(id); len(v) == len(value) && v == value {
				return id
			}
		}
		return 0
	}
	if id := dim.byKey[value]; id != 0 && !hasExtraOverride(dim.extra, id) {
		return id
	}
	return 0
}

func (dim *dictDim) lookup(value string) uint32 {
	if dim == nil || dim.count == 0 || value == "" {
		return 0
	}
	if dim.ext != nil {
		return dim.lookupExt(value)
	}
	dim.revMu.RLock()
	if dim.sorted != nil || dim.byKey != nil {
		id := dim.lookupLocked(value)
		dim.revMu.RUnlock()
		return id
	}
	dim.revMu.RUnlock()

	dim.revMu.Lock()
	dim.ensureReverseLocked()
	id := dim.lookupLocked(value)
	dim.revMu.Unlock()
	return id
}

func (d *dictionary) getDim(key dictDimKey) *dictDim {
	if d.dims == nil {
		d.dims = make(map[dictDimKey]*dictDim)
	}
	dim := d.dims[key]
	if dim == nil {
		dim = newDictDim()
		d.dims[key] = dim
	}
	return dim
}

func (d *dictionary) dimCount(series uint16, dimIdx uint8) int {
	if d.snapshot && series == d.snapshotSeries && dimIdx < 8 {
		return d.snapshotDims[dimIdx].count
	}
	if d.dims == nil {
		return 0
	}
	if dim := d.dims[dictDimKey{series: series, dim: dimIdx}]; dim != nil {
		return dim.count
	}
	return 0
}

func (d *dictionary) dimMaxID(series uint16, dimIdx int) uint32 {
	if dimIdx < 0 || dimIdx >= 8 {
		return 0
	}
	if d.snapshot && series == d.snapshotSeries {
		if next := d.snapshotDims[dimIdx].nextID; next > 0 {
			return next - 1
		}
		return 0
	}
	if dim := d.dims[dictDimKey{series: series, dim: uint8(dimIdx)}]; dim != nil && dim.nextID > 0 {
		return dim.nextID - 1
	}
	return 0
}

func (d *dictionary) intern(series uint16, dimIdx uint8, value string) (uint32, bool) {
	if value == "" {
		return 0, false
	}
	dim := d.getDim(dictDimKey{series: series, dim: dimIdx})
	if dim.ext != nil {
		return d.internExt(dim, value)
	}
	if id := dim.lookup(value); id != 0 {
		return id, false
	}
	id := dim.nextID
	if id == 0 {
		id = 1
	}
	dim.appendValue(id, value)
	if dim.sorted != nil {
		if dim.nextID > dim.sortedNextID && dim.nextID-dim.sortedNextID >= dictSortedMergeThreshold {
			dim.mergeTailIntoSorted()
		}
	} else if dim.count > dictMapThreshold {
		dim.buildSorted()
	} else if dim.byKey != nil {
		dim.byKey[value] = id
	}
	return id, true
}

func (d *dictionary) apply(e dictEntry) {
	if e.id == 0 {
		return
	}
	dim := d.getDim(dictDimKey{series: e.series, dim: e.dim})
	isNewTail := e.id >= dim.nextID
	prev := ""
	if !isNewTail && dim.byKey != nil {
		prev = dim.resolve(e.id)
	}
	dim.appendValue(e.id, e.value)
	if dim.ext != nil {
		if isNewTail && e.value != "" {
			if dim.tailByKey == nil {
				dim.tailByKey = make(map[string]uint32)
			}
			dim.tailByKey[e.value] = e.id
		}
		return
	}
	if dim.sorted != nil {
		if dim.nextID > dim.sortedNextID && dim.nextID-dim.sortedNextID >= dictSortedMergeThreshold {
			dim.mergeTailIntoSorted()
		}
	} else if dim.byKey != nil {
		if dim.count > dictMapThreshold {
			dim.buildSorted()
		} else {
			if prev != "" && prev != e.value && dim.byKey[prev] == e.id {
				delete(dim.byKey, prev)
			}
			if e.value != "" {
				dim.byKey[e.value] = e.id
			}
		}
	}
}

func (d *dictionary) lookup(series uint16, dimIdx uint8, value string) uint32 {
	if d.snapshot && series == d.snapshotSeries && dimIdx < 8 {
		return d.snapshotDims[dimIdx].lookup(value)
	}
	if d.dims == nil {
		return 0
	}
	return d.dims[dictDimKey{series: series, dim: dimIdx}].lookup(value)
}

func (d *dictionary) resolve(series uint16, dimIdx uint8, id uint32) string {
	if d.snapshot && series == d.snapshotSeries && dimIdx < 8 {
		return d.snapshotDims[dimIdx].resolve(id)
	}
	if d.dims == nil {
		return ""
	}
	return d.dims[dictDimKey{series: series, dim: dimIdx}].resolve(id)
}

func (s *Series) DictStats() ([]DictDimStats, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("strata: nil series")
	}
	db := s.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, errors.New("strata: database is closed")
	}
	arity := s.cfg.Arity
	out := make([]DictDimStats, arity)
	for dim := range arity {
		st := DictDimStats{Dim: dim}
		if dimData := db.dict.dims[dictDimKey{series: s.id, dim: uint8(dim)}]; dimData != nil {
			if dimData.nextID > 0 {
				st.MaxID = dimData.nextID - 1
			}
			if dimData.count > 0 {
				st.LiveCount = uint32(dimData.count)
			}
			if st.MaxID > st.LiveCount {
				st.DeadCount = st.MaxID - st.LiveCount
			}
			st.InlineBytes = uint64(len(dimData.arena))
			if dimData.ext != nil {
				st.ExternalBytes = dimData.ext.fwdTotalBytes + dimData.ext.sstTotalBytes
				st.ForwardPages = uint32(len(dimData.ext.fwdPages))
				st.ReverseBlocks = uint32(len(dimData.ext.sstBlocks))
			}
		}
		out[dim] = st
	}
	return out, nil
}
