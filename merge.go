package strata

import "sort"

func mergeSummary(dst *Summary, src Summary, ops [4]MetricOp, srcOrder, dstOrder uint64) {
	src.Mask = normalizeMask(src.Mask)
	for i := range 4 {
		bit := uint8(1 << i)
		if src.Mask&bit == 0 {
			continue
		}
		if dst.Mask&bit == 0 {
			dst.Values[i] = src.Values[i]
			dst.Mask |= bit
			continue
		}
		switch ops[i] {
		case OpMin:
			if src.Values[i] < dst.Values[i] {
				dst.Values[i] = src.Values[i]
			}
		case OpMax:
			if src.Values[i] > dst.Values[i] {
				dst.Values[i] = src.Values[i]
			}
		case OpLast:
			if srcOrder >= dstOrder {
				dst.Values[i] = src.Values[i]
			}
		default:
			dst.Values[i] += src.Values[i]
		}
	}
}

func mergeIDRows(dst *[]idRow, src []idRow, ops [4]MetricOp, srcOrder, dstOrder uint64) {
	hasLast := opsHasLast(ops) && srcOrder >= dstOrder
	var index map[[8]uint32]int
	if len(*dst)+len(src) > 32 {
		index = make(map[[8]uint32]int, len(*dst)+len(src))
		for i, row := range *dst {
			index[row.tags] = i
		}
	}
	var moved []bool
	if hasLast && len(*dst) > 0 && len(src) > 0 {
		moved = make([]bool, len(*dst))
	}
	for _, r := range src {
		r.initMask = normalizeMask(r.initMask)
		idx := -1
		if index != nil {
			if found, ok := index[r.tags]; ok {
				idx = found
			}
		} else {
			for i := range *dst {
				if (i >= len(moved) || !moved[i]) && (*dst)[i].tags == r.tags {
					idx = i
					break
				}
			}
		}
		if idx < 0 {
			r2 := r
			*dst = append(*dst, r2)
			if index != nil {
				index[r.tags] = len(*dst) - 1
			}
			continue
		}
		d := &(*dst)[idx]
		for j := range 4 {
			bit := uint8(1 << j)
			if r.initMask&bit == 0 {
				continue
			}
			if d.initMask&bit == 0 {
				d.values[j] = r.values[j]
				d.initMask |= bit
				continue
			}
			switch ops[j] {
			case OpMin:
				if r.values[j] < d.values[j] {
					d.values[j] = r.values[j]
				}
			case OpMax:
				if r.values[j] > d.values[j] {
					d.values[j] = r.values[j]
				}
			case OpLast:
				if srcOrder >= dstOrder {
					d.values[j] = r.values[j]
				}
			default:
				d.values[j] += r.values[j]
			}
		}
		if hasLast && idx < len(moved) {
			updated := *d
			moved[idx] = true
			*dst = append(*dst, updated)
			if index != nil {
				index[r.tags] = len(*dst) - 1
			}
		}
	}
	if len(moved) > 0 {
		out := (*dst)[:0]
		for i, row := range *dst {
			if i < len(moved) && moved[i] {
				continue
			}
			out = append(out, row)
		}
		*dst = out
	}
}

func cloneBucket(b *bucket) *bucket {
	if b == nil {
		return nil
	}
	x := *b
	x.rows = append([]idRow(nil), b.rows...)
	return &x
}

func cloneRows(rows []idRow) []idRow { return append([]idRow(nil), rows...) }

func sortBlocks(b []sealedBlock) {
	sort.Slice(b, func(i, j int) bool {
		if b[i].series != b[j].series {
			return b[i].series < b[j].series
		}
		if b[i].start != b[j].start {
			return b[i].start < b[j].start
		}
		if b[i].res != b[j].res {
			return b[i].res < b[j].res
		}
		return b[i].epoch < b[j].epoch
	})
}

func sortSealedMetas(b []sealedMeta) {
	sort.Slice(b, func(i, j int) bool {
		if b[i].series != b[j].series {
			return b[i].series < b[j].series
		}
		if b[i].start != b[j].start {
			return b[i].start < b[j].start
		}
		if b[i].res != b[j].res {
			return b[i].res < b[j].res
		}
		return b[i].epoch < b[j].epoch
	})
}

func ruleIntersects(r []resetRule, start, end int64) bool {
	for _, x := range r {
		if x.to > start && x.from < end {
			return true
		}
	}
	return false
}
