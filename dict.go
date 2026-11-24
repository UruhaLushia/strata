package strata

import (
	"sort"
	"sync"
	"unsafe"
)

const (
	dictMapThreshold   = 65536
	dictPendingMaxKeys = 4096
)

type dictDim struct {
	arena     []byte
	offsets   []uint32
	extra     map[uint32]string
	tailBase  uint32
	nextID    uint32
	count     int
	ext       *dictExtDim
	revMu     sync.RWMutex
	byKey     map[string]uint32
	sorted    []uint32
	pending   map[string]uint32
	tailByKey map[string]uint32
	hot       *dictHotCache
}

type dictDimSnap struct {
	arena    []byte
	offsets  []uint32
	extra    map[uint32]string
	tailBase uint32
	nextID   uint32
	ext      *dictExtDim
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
		if dim.pending == nil {
			dim.pending = make(map[string]uint32)
		}
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
	if dim.sorted != nil {
		if id := dim.pending[value]; id != 0 {
			return id
		}
		idx := sort.Search(len(dim.sorted), func(i int) bool {
			return dim.resolve(dim.sorted[i]) >= value
		})
		if idx < len(dim.sorted) && dim.resolve(dim.sorted[idx]) == value {
			return dim.sorted[idx]
		}
		return 0
	}
	return dim.byKey[value]
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
		dim.pending[value] = id
		if len(dim.pending) >= dictPendingMaxKeys {
			dim.flushPendingIntoSorted()
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
	dim.appendValue(e.id, e.value)
	if dim.ext != nil {
		if dim.tailByKey == nil {
			dim.tailByKey = make(map[string]uint32)
		}
		dim.tailByKey[e.value] = e.id
		return
	}
	if dim.sorted != nil {
		dim.pending[e.value] = e.id
		if len(dim.pending) >= dictPendingMaxKeys {
			dim.flushPendingIntoSorted()
		}
	} else if dim.byKey != nil {
		if dim.count > dictMapThreshold {
			dim.buildSorted()
		} else {
			dim.byKey[e.value] = e.id
		}
	}
}

func (d *dictionary) lookup(series uint16, dimIdx uint8, value string) uint32 {
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
