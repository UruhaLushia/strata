package strata

import (
	"bytes"
	"unsafe"
)

// dictTagReader is a query-local tag resolver for TopK tie-breaking and materialization.
// For external high-cardinality dimensions it reads forward pages into a private single-page
// buffer using the query's decoder, bypassing the global FIFO dictPageCache completely.
type dictTagReader struct {
	snap       dictDimSnap
	slot       *querySlot
	ownedSlot  bool
	err        error
	extPageIdx int
	extPageBuf []byte
}

func newDictTagReader(d dictionary, series uint16, dim uint8, slot *querySlot) dictTagReader {
	var snap dictDimSnap
	if d.snapshot && series == d.snapshotSeries && dim < 8 {
		snap = d.snapshotDims[dim]
	} else if d.dims != nil {
		if dimData := d.dims[dictDimKey{series: series, dim: dim}]; dimData != nil {
			snap = dictDimSnap{
				arena:    dimData.arena,
				offsets:  dimData.offsets,
				extra:    dimData.extra,
				tailBase: dimData.tailBase,
				nextID:   dimData.nextID,
				ext:      dimData.ext,
			}
		}
	}
	ownedSlot := slot == nil && snap.ext != nil
	if ownedSlot {
		slot = &querySlot{codecs: snap.ext.view.codecs}
	}
	return dictTagReader{
		snap:       snap,
		slot:       slot,
		ownedSlot:  ownedSlot,
		extPageIdx: -1,
	}
}

func (r *dictTagReader) close() {
	if r.ownedSlot {
		r.slot.close()
	}
	r.extPageBuf = nil
	r.extPageIdx = -1
}

// resolveBytes returns a read-only byte slice for id using the exact same 3-tier
// priority as dictDimSnap.resolve: extra > external forward pages > tail/inline arena.
// The returned slice may be invalidated by subsequent resolveBytes calls on the same reader.
func (r *dictTagReader) resolveBytes(id uint32) []byte {
	if id == 0 || r.err != nil {
		return nil
	}
	// Priority 1: extra overrides.
	if r.snap.extra != nil {
		if v, ok := r.snap.extra[id]; ok {
			if len(v) == 0 {
				return nil
			}
			return unsafe.Slice(unsafe.StringData(v), len(v))
		}
	}
	// Priority 2: external forward pages (bypasses global FIFO dictPageCache).
	if r.snap.ext != nil && id < r.snap.tailBase {
		pageIdx := int(id-1) / dictFwdPageSize
		slotIdx := int(id-1) % dictFwdPageSize
		if pageIdx < 0 || pageIdx >= len(r.snap.ext.fwdPages) || r.snap.ext.fwdPages[pageIdx].length == 0 {
			return nil
		}
		if pageIdx != r.extPageIdx {
			plain, err := r.snap.ext.readPageInto(r.snap.ext.fwdStartOff, r.snap.ext.fwdPages[pageIdx], &r.extPageBuf, r.slot)
			if err != nil {
				r.err = err
				r.extPageIdx = -1
				return nil
			}
			r.extPageBuf = plain
			r.extPageIdx = pageIdx
		}
		return decodeFwdPageSlice(r.extPageBuf, slotIdx)
	}
	// Priority 3: tail / inline arena.
	idx := int(id - r.snap.tailBase)
	if idx < 0 || idx+1 >= len(r.snap.offsets) {
		return nil
	}
	lo, hi := r.snap.offsets[idx], r.snap.offsets[idx+1]
	if lo == hi {
		return nil
	}
	return r.snap.arena[lo:hi]
}

type topKHeap struct {
	items  []groupRankEntry
	limit  int
	reader *dictTagReader
}

func newTopKHeap(limit int, reader *dictTagReader) topKHeap {
	return topKHeap{
		items:  make([]groupRankEntry, 0, min(limit, aggPartitionSize)),
		limit:  limit,
		reader: reader,
	}
}

func (h *topKHeap) push(id uint32, sum Summary) {
	if h.limit <= 0 || h.reader.err != nil {
		return
	}
	if len(h.items) < h.limit {
		tb := h.reader.resolveBytes(id)
		cp := make([]byte, len(tb), max(len(tb), 32))
		copy(cp, tb)
		h.items = append(h.items, groupRankEntry{id: id, sum: sum, tag: cp})
		heapSiftUp(h.items, len(h.items)-1)
		return
	}
	top := &h.items[0]
	cmp := compareMetricRank(sum, top.sum)
	if cmp < 0 {
		return
	}
	candTag := h.reader.resolveBytes(id)
	if cmp == 0 && bytes.Compare(candTag, top.tag) >= 0 {
		return
	}
	top.id = id
	top.sum = sum
	top.tag = append(top.tag[:0], candTag...)
	heapSiftDown(h.items, 0)
}

func (h *topKHeap) mergeEntry(e groupRankEntry) {
	if h.limit <= 0 {
		return
	}
	if len(h.items) < h.limit {
		h.items = append(h.items, e)
		heapSiftUp(h.items, len(h.items)-1)
		return
	}
	if compareEntryRank(e, h.items[0]) <= 0 {
		return
	}
	h.items[0] = e
	heapSiftDown(h.items, 0)
}

func (h *topKHeap) toGroupRows() []GroupRow {
	rows := getGroupRowSlice(len(h.items))
	for _, e := range h.items {
		rows = append(rows, GroupRow{Tag: string(e.tag), Summary: e.sum})
	}
	sortTopKGroupRows(rows)
	return rows
}
