package strata

import (
	"errors"
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

func (dim *dictDim) resolve(id uint32) (string, error) {
	if dim == nil || id == 0 {
		return "", nil
	}
	if dim.extra != nil {
		if v, ok := dim.extra[id]; ok {
			return v, nil
		}
	}
	if dim.ext != nil && id < dim.tailBase {
		return dim.ext.resolve(id)
	}
	return dim.resolveArena(id), nil
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

func (s *dictDimSnap) resolve(id uint32) (string, error) {
	if id == 0 {
		return "", nil
	}
	if s.extra != nil {
		if v, ok := s.extra[id]; ok {
			return v, nil
		}
	}
	if s.ext != nil && id < s.tailBase {
		return s.ext.resolve(id)
	}
	return s.resolveArena(id), nil
}

func (dim *dictDim) appendValue(id uint32, value string) error {
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
		return nil
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
		return nil
	}
	prev, err := dim.resolve(id)
	if err != nil {
		return err
	}
	if prev == value {
		return nil
	}
	if dim.extra == nil {
		dim.extra = make(map[uint32]string)
	}
	dim.extra[id] = value
	if prev == "" && value != "" {
		dim.count++
	} else if prev != "" && value == "" && dim.count > 0 {
		dim.count--
	}
	return nil
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
		v, overridden := dim.extra[id]
		if !overridden {
			v = dim.resolveArena(id)
		}
		if v != "" {
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
		snap := dictDimSnap{
			arena:        dim.arena,
			offsets:      dim.offsets,
			extra:        dim.extra,
			tailBase:     dim.tailBase,
			nextID:       dim.nextID,
			count:        dim.count,
			sorted:       dim.sorted,
			sortedNextID: dim.sortedNextID,
		}
		return snap.lookupInline(value)
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

func (d *dictionary) internAfterLookup(series uint16, dimIdx uint8, value string) (uint32, bool, error) {
	if value == "" {
		return 0, false, nil
	}
	dim := d.getDim(dictDimKey{series: series, dim: dimIdx})
	if dim.ext != nil {
		return d.internExtTail(dim, value)
	}
	if id := dim.lookup(value); id != 0 {
		return id, false, nil
	}
	id := dim.nextID
	if id == 0 {
		id = 1
	}
	if err := dim.appendValue(id, value); err != nil {
		return 0, false, err
	}
	if dim.sorted != nil {
		if dim.nextID > dim.sortedNextID && dim.nextID-dim.sortedNextID >= dictSortedMergeThreshold {
			dim.mergeTailIntoSorted()
		}
	} else if dim.count > dictMapThreshold {
		dim.buildSorted()
	} else if dim.byKey != nil {
		dim.byKey[value] = id
	}
	return id, true, nil
}

func (d *dictionary) apply(e dictEntry) error {
	if e.id == 0 {
		return nil
	}
	dim := d.getDim(dictDimKey{series: e.series, dim: e.dim})
	isNewTail := e.id >= dim.nextID
	previous := ""
	if !isNewTail && dim.byKey != nil {
		prev, err := dim.resolve(e.id)
		if err != nil {
			return err
		}
		previous = prev
	}
	if err := dim.appendValue(e.id, e.value); err != nil {
		return err
	}
	if dim.ext != nil {
		if isNewTail && e.value != "" {
			if dim.tailByKey == nil {
				dim.tailByKey = make(map[string]uint32)
			}
			dim.tailByKey[e.value] = e.id
		}
		return nil
	}
	if dim.sorted != nil {
		if dim.nextID > dim.sortedNextID && dim.nextID-dim.sortedNextID >= dictSortedMergeThreshold {
			dim.mergeTailIntoSorted()
		}
	} else if dim.byKey != nil {
		if dim.count > dictMapThreshold {
			dim.buildSorted()
		} else {
			if previous != "" && previous != e.value && dim.byKey[previous] == e.id {
				delete(dim.byKey, previous)
			}
			if e.value != "" {
				dim.byKey[e.value] = e.id
			}
		}
	}
	return nil
}

func (d *dictionary) lookup(series uint16, dimIdx uint8, value string) (uint32, error) {
	if d.snapshot && series == d.snapshotSeries && dimIdx < 8 {
		return d.snapshotDims[dimIdx].lookup(value)
	}
	dim := d.dims[dictDimKey{series: series, dim: dimIdx}]
	if dim != nil && dim.ext != nil && value != "" {
		id, _, err := dim.lookupExt(value)
		return id, err
	}
	return dim.lookup(value), nil
}

func (d *dictionary) resolve(series uint16, dimIdx uint8, id uint32) (string, error) {
	if d.snapshot && series == d.snapshotSeries && dimIdx < 8 {
		return d.snapshotDims[dimIdx].resolve(id)
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
				for _, p := range dimData.ext.fwdPages {
					if p.length > 0 {
						st.ForwardPages++
					}
				}
				st.ReverseBlocks = uint32(len(dimData.ext.sstBlocks))
			}
		}
		out[dim] = st
	}
	return out, nil
}
