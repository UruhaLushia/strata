package strata

func aggregateEncodedBlock(block sealedBlock, slotIdx, arity int, filters []compiledFilter, groups []aggregateGroups, groupDims []int, ops [4]MetricOp, slot *querySlot, arenaBudget int) (Summary, bool, error) {
	allowDim0BS := !opsHasLast(ops)
	if !isStreamBlock(block.encoded) {
		return aggregateSmallEncodedBlock(block.encoded, block.codecID, block.summary, slotIdx, arity, filters, groups, groupDims, ops, allowDim0BS, slot)
	}
	view, err := parseColumnarBlock(block.encoded, block.codecID)
	if err != nil {
		return Summary{}, false, err
	}
	if view.count == 0 {
		if filters == nil {
			return block.summary, true, nil
		}
		return Summary{}, false, nil
	}
	allSum := ops == [4]MetricOp{OpSum, OpSum, OpSum, OpSum}
	fastSumOnly := len(groupDims) == 0 && allSum && view.descs[12].mode == modeConst && normalizeMask(byte(view.descs[12].value)) == 0x0f
	colMask := uint16(0x1f) << 8 // metrics 8..11 + mask 12
	if fastSumOnly {
		colMask = uint16(0x0f) << 8
	}
	for _, dim := range groupDims {
		if dim >= 0 && dim < arity && dim < 8 {
			colMask |= uint16(1) << dim
		}
	}
	cache := newBlockColCache(view, colMask, filterColMask(filters), arenaBudget, slot)
	sel, err := cache.filter(filters, allowDim0BS)
	if err != nil {
		return Summary{}, false, err
	}
	defer sel.release()
	total := sel.count()
	if total == 0 {
		if filters == nil {
			return block.summary, true, nil
		}
		return Summary{}, false, nil
	}

	// Fast path: filtered summary-only OpSum with full constant mask.
	if fastSumOnly {
		if filters == nil {
			return block.summary, true, nil
		}
		sum, err := sumStreamMetricsCached(&cache, sel)
		return sum, true, err
	}

	cache.prepareOutput(colMask, total, sel.mode == selRange)

	var summary Summary
	var prevOrder uint64
	var rowIdxBuf []int32
	if cache.fullBlock {
		if err := cache.loadAll(colMask); err != nil {
			return Summary{}, false, err
		}
		if sel.mode == selRange {
			aggregateRangeCols(&cache.cols, sel.lo, sel.hi, slotIdx, 0, arity, filters != nil, groups, groupDims, ops, &summary, &prevOrder)
		} else {
			const idxChunk = 4096
			for start := 0; start < total; start += idxChunk {
				n := min(idxChunk, total-start)
				_, _, rows := sel.sliceRows(start, n, rowIdxBuf)
				if cap(rows) > cap(rowIdxBuf) {
					rowIdxBuf = rows[:0]
				}
				aggregateIndexedCols(&cache.cols, rows, slotIdx, start, arity, filters != nil, groups, groupDims, ops, &summary, &prevOrder)
			}
		}
		if filters == nil {
			summary = block.summary
		}
		return summary, true, nil
	}

	rowBytes := cache.packRowBytes(colMask, sel.mode == selRange)
	avail := cache.availPackBytes()
	chunkRows := total
	if rowBytes > 0 && total*rowBytes > avail {
		chunkRows = max(avail/rowBytes, 1)
	}
	for start := 0; start < total; start += chunkRows {
		n := min(chunkRows, total-start)
		isRange, rangeLo, rows := sel.sliceRows(start, n, rowIdxBuf)
		if !isRange && cap(rows) > cap(rowIdxBuf) {
			rowIdxBuf = rows[:0]
		}
		cols, packErr := cache.packChunk(colMask, isRange, rangeLo, rows, n)
		if packErr != nil {
			return Summary{}, false, packErr
		}
		aggregatePackedCols(&cols, n, slotIdx, start, arity, filters != nil, groups, groupDims, ops, &summary, &prevOrder)
	}
	if filters == nil {
		summary = block.summary
	}
	return summary, true, nil
}

func sumStreamMetricsCached(cache *blockColCache, sel rowSelection) (Summary, error) {
	var sum Summary
	sum.Mask = 0x0f
	total := sel.count()
	var rowIdxBuf []int32
	isRange, rangeLo, rows := sel.sliceRows(0, total, rowIdxBuf)
	for m := range 4 {
		col, err := cache.load(8 + m)
		if err != nil {
			return Summary{}, err
		}
		if col.mode == modeZero {
			continue
		}
		if col.mode == modeConst {
			sum.Values[m] = int64(col.val) * int64(total)
			continue
		}
		var acc int64
		if isRange {
			for i := range total {
				acc += col.valAt(rangeLo + i)
			}
		} else {
			for _, r := range rows {
				acc += col.valAt(int(r))
			}
		}
		sum.Values[m] = acc
	}
	return sum, nil
}

func aggregateSmallEncodedBlock(raw []byte, id uint8, blockSummary Summary, slotIdx, arity int, filters []compiledFilter, groups []aggregateGroups, groupDims []int, ops [4]MetricOp, allowDim0BS bool, slot *querySlot) (Summary, bool, error) {
	count, cols, parseErr := decodeSmallBlockCols(raw, id, slot)
	if parseErr != nil {
		return Summary{}, false, parseErr
	}
	if count == 0 {
		if filters == nil {
			return blockSummary, true, nil
		}
		return Summary{}, false, nil
	}
	sel, err := filterSmallCols(count, &cols, filters, allowDim0BS)
	if err != nil {
		return Summary{}, false, err
	}
	defer sel.release()
	total := sel.count()
	if total == 0 {
		if filters == nil {
			return blockSummary, true, nil
		}
		return Summary{}, false, nil
	}
	var summary Summary
	var prevOrder uint64
	isRange, rangeLo, rows := sel.sliceRows(0, total, nil)
	if isRange {
		aggregateRangeCols(&cols, rangeLo, rangeLo+total, slotIdx, 0, arity, filters != nil, groups, groupDims, ops, &summary, &prevOrder)
	} else {
		aggregateIndexedCols(&cols, rows, slotIdx, 0, arity, filters != nil, groups, groupDims, ops, &summary, &prevOrder)
	}
	if filters == nil {
		summary = blockSummary
	}
	return summary, true, nil
}

func aggregatePackedCols(cols *[streamColumnCount]decodedCol, n, slotIdx, orderOffset, arity int, filtered bool, groups []aggregateGroups, groupDims []int, ops [4]MetricOp, summary *Summary, prevOrder *uint64) {
	aggregateRangeCols(cols, 0, n, slotIdx, orderOffset, arity, filtered, groups, groupDims, ops, summary, prevOrder)
}

func aggregateRangeCols(cols *[streamColumnCount]decodedCol, lo, hi, slotIdx, orderOffset, arity int, filtered bool, groups []aggregateGroups, groupDims []int, ops [4]MetricOp, summary *Summary, prevOrder *uint64) {
	allSum := ops == [4]MetricOp{OpSum, OpSum, OpSum, OpSum}
	c0, c1, c2, c3, cm := cols[8], cols[9], cols[10], cols[11], cols[12]

	if !filtered && len(groupDims) == 1 && len(groups) == 1 &&
		groupDims[0] >= 0 && groupDims[0] < arity && groups[0].dense != nil &&
		cm.mode == modeConst && normalizeMask(byte(cm.val)) == 0x0f {
		if allSum {
			aggregateRangeDenseAllSum(cols, lo, hi, groupDims[0], &groups[0])
		} else {
			aggregateRangeDenseMixed(cols, lo, hi, slotIdx, orderOffset, groupDims[0], &groups[0], ops)
		}
		return
	}

	if allSum {
		constMask := cm.mode == modeConst
		fixedMask := byte(0)
		if constMask {
			fixedMask = normalizeMask(byte(cm.val))
		}
		for row := lo; row < hi; row++ {
			v0, v1, v2, v3 := c0.valAt(row), c1.valAt(row), c2.valAt(row), c3.valAt(row)
			mask := fixedMask
			if !constMask {
				mask = normalizeMask(cm.maskAt(row))
			}
			if filtered {
				addSumToSummary(summary, v0, v1, v2, v3, mask)
			}
			for gi, dim := range groupDims {
				if dim < 0 || dim >= arity || gi >= len(groups) {
					continue
				}
				groups[gi].addSum(cols[dim].tagAt(row), v0, v1, v2, v3, mask)
			}
		}
		return
	}

	slotBase := uint64(slotIdx+1) << 32
	for pos, row := 0, lo; row < hi; pos, row = pos+1, row+1 {
		current := Summary{
			Values: [4]int64{c0.valAt(row), c1.valAt(row), c2.valAt(row), c3.valAt(row)},
			Mask:   cm.maskAt(row),
		}
		order := slotBase | uint64(orderOffset+pos+1)
		if filtered {
			mergeSummary(summary, current, ops, order, *prevOrder)
			*prevOrder = order
		}
		for gi, dim := range groupDims {
			if dim < 0 || dim >= arity || gi >= len(groups) {
				continue
			}
			groups[gi].add(cols[dim].tagAt(row), current, ops, order)
		}
	}
}

func aggregateIndexedCols(cols *[streamColumnCount]decodedCol, rows []int32, slotIdx, orderOffset, arity int, filtered bool, groups []aggregateGroups, groupDims []int, ops [4]MetricOp, summary *Summary, prevOrder *uint64) {
	allSum := ops == [4]MetricOp{OpSum, OpSum, OpSum, OpSum}
	c0, c1, c2, c3, cm := cols[8], cols[9], cols[10], cols[11], cols[12]
	if allSum {
		constMask := cm.mode == modeConst
		fixedMask := byte(0)
		if constMask {
			fixedMask = normalizeMask(byte(cm.val))
		}
		for _, r := range rows {
			row := int(r)
			v0, v1, v2, v3 := c0.valAt(row), c1.valAt(row), c2.valAt(row), c3.valAt(row)
			mask := fixedMask
			if !constMask {
				mask = normalizeMask(cm.maskAt(row))
			}
			if filtered {
				addSumToSummary(summary, v0, v1, v2, v3, mask)
			}
			for gi, dim := range groupDims {
				if dim < 0 || dim >= arity || gi >= len(groups) {
					continue
				}
				groups[gi].addSum(cols[dim].tagAt(row), v0, v1, v2, v3, mask)
			}
		}
		return
	}
	slotBase := uint64(slotIdx+1) << 32
	for pos, r := range rows {
		row := int(r)
		current := Summary{
			Values: [4]int64{c0.valAt(row), c1.valAt(row), c2.valAt(row), c3.valAt(row)},
			Mask:   cm.maskAt(row),
		}
		order := slotBase | uint64(orderOffset+pos+1)
		if filtered {
			mergeSummary(summary, current, ops, order, *prevOrder)
			*prevOrder = order
		}
		for gi, dim := range groupDims {
			if dim < 0 || dim >= arity || gi >= len(groups) {
				continue
			}
			groups[gi].add(cols[dim].tagAt(row), current, ops, order)
		}
	}
}

func aggregateDecodedRows(rows []idRow, blockSummary Summary, slotIdx int, filters []compiledFilter, groups []aggregateGroups, groupDims []int, arity int, ops [4]MetricOp) (Summary, bool) {
	if filters == nil && len(rows) == 0 {
		return blockSummary, true
	}
	var summary Summary
	matched := filters == nil
	slotBase := uint64(slotIdx+1) << 32
	var prevOrder uint64
	for rowIdx, row := range rows {
		if filters != nil && !matchesCompiledRow(row, filters) {
			continue
		}
		matched = true
		order := slotBase | uint64(rowIdx+1)
		current := Summary{Values: row.values, Mask: row.initMask}
		if filters != nil {
			mergeSummary(&summary, current, ops, order, prevOrder)
			prevOrder = order
		}
		for gi, dim := range groupDims {
			if dim < 0 || dim >= arity || gi >= len(groups) {
				continue
			}
			groups[gi].add(row.tags[dim], current, ops, order)
		}
	}
	if !matched {
		return Summary{}, false
	}
	if filters == nil {
		summary = blockSummary
	}
	return summary, true
}
