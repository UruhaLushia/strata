package strata

const (
	denseGroupLimitUnfiltered = 16384
	denseGroupLimitFiltered   = 4096
)

type aggregateGroups struct {
	dense          []Summary
	denseLastOrder []uint64
	denseOrder     [][4]uint64
	open           openAddrGroups
	lastCount      uint8
	lastIdx        int8
}

func opsHasLast(ops [4]MetricOp) bool {
	return ops[0] == OpLast || ops[1] == OpLast || ops[2] == OpLast || ops[3] == OpLast
}

func countLastOps(ops [4]MetricOp) (uint8, int8) {
	var count uint8
	lastIdx := int8(-1)
	for i := range 4 {
		if ops[i] == OpLast {
			count++
			lastIdx = int8(i)
		}
	}
	return count, lastIdx
}

func orderEntryBytes(ops [4]MetricOp) int64 {
	c, _ := countLastOps(ops)
	switch c {
	case 0:
		return 0
	case 1:
		return 8
	default:
		return 32
	}
}

func newAggregateGroups(d dictionary, series uint16, dim int, filtered bool, ops [4]MetricOp, groupUpper int, budget int64) aggregateGroups {
	lastCount, lastIdx := countLastOps(ops)
	maxID := d.dimMaxID(series, dim)
	g := aggregateGroups{lastCount: lastCount, lastIdx: lastIdx}
	if maxID > 128 && groupUpper > 0 && int64(groupUpper)*2 < int64(maxID) {
		slots := 64
		entryBytes := 40 + orderEntryBytes(ops)
		for int64(slots)*7 <= int64(groupUpper)*10 && int64(slots)*2*entryBytes <= budget {
			slots *= 2
		}
		g.open.init(slots, lastCount)
		return g
	}
	limit := uint32(denseGroupLimitUnfiltered)
	if filtered {
		limit = denseGroupLimitFiltered
	}
	if maxID <= limit {
		n := int(maxID) + 1
		g.dense = make([]Summary, n)
		if lastCount == 1 {
			g.denseLastOrder = make([]uint64, n)
		} else if lastCount >= 2 {
			g.denseOrder = make([][4]uint64, n)
		}
		return g
	}
	g.open.lastCount = lastCount
	return g
}

func estimateGroupBytes(d dictionary, series uint16, groupDims []int, filtered bool, ops [4]MetricOp) int64 {
	var total int64
	limit := uint32(denseGroupLimitUnfiltered)
	if filtered {
		limit = denseGroupLimitFiltered
	}
	entryBytes := int64(40) + orderEntryBytes(ops)
	for _, dim := range groupDims {
		maxID := d.dimMaxID(series, dim)
		if maxID <= limit {
			total += int64(maxID+1) * entryBytes
		} else {
			est := int64(maxID)
			if filtered && est > 16384 {
				est = 16384
			}
			total += est * entryBytes * 3 / 2
		}
	}
	return total
}

func addSumToSummary(dst *Summary, v0, v1, v2, v3 int64, mask uint8) {
	if mask == 0x0f {
		dst.Mask = 0x0f
		dst.Values[0] += v0
		dst.Values[1] += v1
		dst.Values[2] += v2
		dst.Values[3] += v3
		return
	}
	dst.Mask |= mask
	if mask&1 != 0 {
		dst.Values[0] += v0
	}
	if mask&2 != 0 {
		dst.Values[1] += v1
	}
	if mask&4 != 0 {
		dst.Values[2] += v2
	}
	if mask&8 != 0 {
		dst.Values[3] += v3
	}
}

func mergeSummarySingleLast(dst *Summary, dstOrder *uint64, src Summary, srcOrder uint64, ops [4]MetricOp, lastIdx int) {
	src.Mask = normalizeMask(src.Mask)
	for i := range 4 {
		bit := uint8(1 << i)
		if src.Mask&bit == 0 {
			continue
		}
		if dst.Mask&bit == 0 {
			dst.Values[i] = src.Values[i]
			dst.Mask |= bit
			if i == lastIdx {
				*dstOrder = srcOrder
			}
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
			if srcOrder >= *dstOrder {
				dst.Values[i] = src.Values[i]
				*dstOrder = srcOrder
			}
		default:
			dst.Values[i] += src.Values[i]
		}
	}
}

func mergeSummaryWithOrder(dst *Summary, dstOrder *[4]uint64, src Summary, srcOrder [4]uint64, ops [4]MetricOp) {
	src.Mask = normalizeMask(src.Mask)
	for i := range 4 {
		bit := uint8(1 << i)
		if src.Mask&bit == 0 {
			continue
		}
		if dst.Mask&bit == 0 {
			dst.Values[i] = src.Values[i]
			dst.Mask |= bit
			if dstOrder != nil {
				dstOrder[i] = srcOrder[i]
			}
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
			if dstOrder == nil || srcOrder[i] >= dstOrder[i] {
				dst.Values[i] = src.Values[i]
				if dstOrder != nil {
					dstOrder[i] = srcOrder[i]
				}
			}
		default:
			dst.Values[i] += src.Values[i]
		}
	}
}

func (g *aggregateGroups) addSum(id uint32, v0, v1, v2, v3 int64, mask uint8) {
	if g.dense != nil {
		if id >= uint32(len(g.dense)) {
			return
		}
		addSumToSummary(&g.dense[id], v0, v1, v2, v3, mask)
		return
	}
	idx := g.open.findOrInsert(id)
	sl := &g.open.slots[idx]
	if mask == 0x0f {
		sl.mask = 0x0f
		sl.values[0] += v0
		sl.values[1] += v1
		sl.values[2] += v2
		sl.values[3] += v3
		return
	}
	sl.mask |= mask
	if mask&1 != 0 {
		sl.values[0] += v0
	}
	if mask&2 != 0 {
		sl.values[1] += v1
	}
	if mask&4 != 0 {
		sl.values[2] += v2
	}
	if mask&8 != 0 {
		sl.values[3] += v3
	}
}

func (g *aggregateGroups) add(id uint32, src Summary, ops [4]MetricOp, order uint64) {
	if g.dense != nil {
		if id >= uint32(len(g.dense)) {
			return
		}
		switch g.lastCount {
		case 0:
			mergeSummary(&g.dense[id], src, ops, order, 0)
		case 1:
			mergeSummarySingleLast(&g.dense[id], &g.denseLastOrder[id], src, order, ops, int(g.lastIdx))
		default:
			mergeSummaryWithOrder(&g.dense[id], &g.denseOrder[id], src, [4]uint64{order, order, order, order}, ops)
		}
		return
	}
	idx := g.open.findOrInsert(id)
	dst := Summary{Values: g.open.slots[idx].values, Mask: g.open.slots[idx].mask}
	switch g.lastCount {
	case 0:
		mergeSummary(&dst, src, ops, order, 0)
	case 1:
		mergeSummarySingleLast(&dst, &g.open.lastOrders[idx], src, order, ops, int(g.lastIdx))
	default:
		mergeSummaryWithOrder(&dst, &g.open.orders[idx], src, [4]uint64{order, order, order, order}, ops)
	}
	g.open.slots[idx].values = dst.Values
	g.open.slots[idx].mask = dst.Mask
}

func (g *aggregateGroups) mergeFrom(src *aggregateGroups, ops [4]MetricOp) {
	if src.dense != nil {
		for id := uint32(0); id < uint32(len(src.dense)); id++ {
			if src.dense[id].Mask == 0 {
				continue
			}
			var singleOrd uint64
			var multiOrd [4]uint64
			if src.lastCount == 1 {
				singleOrd = src.denseLastOrder[id]
			} else if src.lastCount >= 2 {
				multiOrd = src.denseOrder[id]
			}
			g.mergeEntry(id, src.dense[id], singleOrd, multiOrd, ops)
		}
		return
	}
	for i := range src.open.slots {
		sl := &src.open.slots[i]
		if sl.key == 0 || sl.mask == 0 {
			continue
		}
		var singleOrd uint64
		var multiOrd [4]uint64
		if src.lastCount == 1 {
			singleOrd = src.open.lastOrders[i]
		} else if src.lastCount >= 2 {
			multiOrd = src.open.orders[i]
		}
		g.mergeEntry(sl.key-1, Summary{Values: sl.values, Mask: sl.mask}, singleOrd, multiOrd, ops)
	}
}

func (g *aggregateGroups) mergeEntry(id uint32, src Summary, singleOrd uint64, multiOrd [4]uint64, ops [4]MetricOp) {
	if g.dense != nil {
		if id >= uint32(len(g.dense)) {
			return
		}
		switch g.lastCount {
		case 0:
			mergeSummary(&g.dense[id], src, ops, 1, 0)
		case 1:
			mergeSummarySingleLast(&g.dense[id], &g.denseLastOrder[id], src, singleOrd, ops, int(g.lastIdx))
		default:
			mergeSummaryWithOrder(&g.dense[id], &g.denseOrder[id], src, multiOrd, ops)
		}
		return
	}
	idx := g.open.findOrInsert(id)
	dst := Summary{Values: g.open.slots[idx].values, Mask: g.open.slots[idx].mask}
	switch g.lastCount {
	case 0:
		mergeSummary(&dst, src, ops, 1, 0)
	case 1:
		mergeSummarySingleLast(&dst, &g.open.lastOrders[idx], src, singleOrd, ops, int(g.lastIdx))
	default:
		mergeSummaryWithOrder(&dst, &g.open.orders[idx], src, multiOrd, ops)
	}
	g.open.slots[idx].values = dst.Values
	g.open.slots[idx].mask = dst.Mask
}

func (g *aggregateGroups) countNonEmpty() int {
	if g.dense != nil {
		n := 0
		for id := 0; id < len(g.dense); id++ {
			if g.dense[id].Mask != 0 {
				n++
			}
		}
		return n
	}
	return g.open.count
}

func (g *aggregateGroups) each(fn func(uint32, Summary)) {
	if g.dense != nil {
		for id := uint32(0); id < uint32(len(g.dense)); id++ {
			if g.dense[id].Mask != 0 {
				fn(id, g.dense[id])
			}
		}
		return
	}
	for i := range g.open.slots {
		sl := &g.open.slots[i]
		if sl.key != 0 && sl.mask != 0 {
			fn(sl.key-1, Summary{Values: sl.values, Mask: sl.mask})
		}
	}
}
