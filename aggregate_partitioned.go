package strata

import (
	"bytes"
	"context"
)

const (
	aggPartitionShift = 14
	aggPartitionSize  = 1 << aggPartitionShift
)

type partState struct {
	dim            int
	ops            [4]MetricOp
	allSum         bool
	lastCount      uint8
	lastIdx        int8
	loID           uint32
	hiID           uint32
	numParts       int
	dense          []Summary
	denseLastOrder []uint64
	denseOrder     [][4]uint64
	bitsRow        []uint64
	trackSummary   bool
	summary        Summary
	prevOrder      uint64
}

func (ps *partState) markBit(id uint32) {
	if ps.bitsRow == nil {
		return
	}
	if pt := int(id >> aggPartitionShift); pt < ps.numParts {
		ps.bitsRow[pt>>6] |= uint64(1) << (pt & 63)
	}
}

func (ps *partState) addPartSummary(idx uint32, cur Summary, order uint64) {
	switch ps.lastCount {
	case 0:
		mergeSummary(&ps.dense[idx], cur, ps.ops, order, 0)
	case 1:
		mergeSummarySingleLast(&ps.dense[idx], &ps.denseLastOrder[idx], cur, order, ps.ops, int(ps.lastIdx))
	default:
		mergeSummaryWithOrder(&ps.dense[idx], &ps.denseOrder[idx], cur, [4]uint64{order, order, order, order}, ps.ops)
	}
}

func (ps *partState) addRow(id uint32, v0, v1, v2, v3 int64, mask uint8, order uint64) {
	ps.markBit(id)
	inPart := id >= ps.loID && id < ps.hiID
	if !inPart && !ps.trackSummary {
		return
	}
	if ps.allSum {
		if ps.trackSummary {
			addSumToSummary(&ps.summary, v0, v1, v2, v3, mask)
		}
		if inPart {
			addSumToSummary(&ps.dense[id-ps.loID], v0, v1, v2, v3, mask)
		}
		return
	}
	cur := Summary{Values: [4]int64{v0, v1, v2, v3}, Mask: mask}
	if ps.trackSummary {
		mergeSummary(&ps.summary, cur, ps.ops, order, ps.prevOrder)
		ps.prevOrder = order
	}
	if inPart {
		ps.addPartSummary(id-ps.loID, cur, order)
	}
}

func executePartitionedTopK(ctx context.Context, pq *preparedQuery, out *AggregateResult, dim, topK, maxWorkers int, slot *querySlot) ([]GroupRow, error) {
	s, db, plans, filters := &pq.execSeries, pq.execDB, pq.plans, pq.compiled
	if len(plans) == 0 {
		return make([]GroupRow, 0), nil
	}
	lastCount, lastIdx := countLastOps(s.cfg.RowOps)
	allSum := s.cfg.RowOps == [4]MetricOp{OpSum, OpSum, OpSum, OpSum}
	maxID := db.dict.dimMaxID(s.id, dim)
	numParts := int(maxID>>aggPartitionShift) + 1
	partWords := (numParts + 63) / 64
	partBits := make([]uint64, len(plans)*partWords)

	dense := make([]Summary, aggPartitionSize)
	var denseLastOrder []uint64
	var denseOrder [][4]uint64
	if lastCount == 1 {
		denseLastOrder = make([]uint64, aggPartitionSize)
	} else if lastCount >= 2 {
		denseOrder = make([][4]uint64, aggPartitionSize)
	}
	dimU8 := uint8(dim)
	reader := newDictTagReader(db.dict, s.id, dimU8, slot)
	defer reader.close()
	h := newTopKHeap(topK, &reader)

	arenaBudget := clampInt(int(db.policy.queryBytes/8), 128<<10, 1<<20)
	out.Segments = getSegmentSummarySlice(len(plans))
	var resultOrder uint64
	ps := partState{
		dim: dim, ops: s.cfg.RowOps, allSum: allSum, lastCount: lastCount, lastIdx: lastIdx,
		loID: 0, hiID: aggPartitionSize, numParts: numParts,
		dense: dense, denseLastOrder: denseLastOrder, denseOrder: denseOrder,
	}

	// Pass 0: compute segments, summary, partition 0 groups, and per-block presence bitmap.
	for index := range plans {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		block, bufPtr, err := mergeSlot(db, plans[index].block, pq.sources, s)
		if err != nil {
			putReadBuf(bufPtr)
			return nil, err
		}
		ps.bitsRow = partBits[index*partWords : (index+1)*partWords]
		ps.trackSummary = filters != nil
		ps.summary, ps.prevOrder = Summary{}, 0
		sum, matched, err := aggregateBlockPart(block, index, filters, &ps, slot, arenaBudget)
		putReadBuf(bufPtr)
		if err != nil {
			return nil, err
		}
		if matched {
			out.Segments = append(out.Segments, SegmentSummary{Meta: plans[index].meta, Summary: sum})
			resultOrder++
			mergeSummary(&pq.qres.Summary, sum, s.cfg.SummaryOps, resultOrder, resultOrder-1)
		}
	}
	pushDenseToHeap(&h, dense, 0)

	var activeParts []int
	for part := 1; part < numParts; part++ {
		wIdx, bit := part>>6, uint64(1)<<(part&63)
		for index := range plans {
			if partBits[index*partWords+wIdx]&bit != 0 {
				activeParts = append(activeParts, part)
				break
			}
		}
	}
	if len(activeParts) > 0 {
		if err := runRemainingPartitions(ctx, pq, activeParts, partBits, partWords, dim, topK, maxWorkers, arenaBudget, &ps, &h, slot); err != nil {
			return nil, err
		}
	}
	return h.toGroupRows(), nil
}

func pushDenseToHeap(h *topKHeap, dense []Summary, loID uint32) {
	for i := range dense {
		if dense[i].Mask == 0 {
			continue
		}
		h.push(loID+uint32(i), dense[i])
	}
}

func aggregateBlockPart(block sealedBlock, slotIdx int, filters []compiledFilter, ps *partState, slot *querySlot, arenaBudget int) (Summary, bool, error) {
	if block.encoded == nil {
		sum, matched := aggregateRowsPartMatched(block.rows, block.summary, slotIdx, filters, ps)
		return sum, matched, nil
	}
	allowDim0BS := ps.lastCount == 0
	if !bytes.HasPrefix(block.encoded, blockStreamsMagic[:]) {
		count, cols, err := decodeSmallBlockCols(block.encoded, slot)
		if err != nil {
			return Summary{}, false, err
		}
		if count == 0 {
			return block.summary, filters == nil, nil
		}
		sel, fErr := filterSmallCols(count, &cols, filters, allowDim0BS)
		if fErr != nil {
			return Summary{}, false, fErr
		}
		defer sel.release()
		total := sel.count()
		if total == 0 {
			return block.summary, filters == nil, nil
		}
		accSelectionPart(&cols, sel, slotIdx, ps)
		if filters == nil {
			return block.summary, true, nil
		}
		return ps.summary, true, nil
	}
	view, err := parseColumnarBlock(block.encoded)
	if err != nil {
		return Summary{}, false, err
	}
	if view.count == 0 {
		return block.summary, filters == nil, nil
	}
	colMask := (uint16(0x1f) << 8) | (uint16(1) << ps.dim)
	cache := newBlockColCache(view, colMask, filterColMask(filters), arenaBudget, slot)
	sel, err := cache.filter(filters, allowDim0BS)
	if err != nil {
		return Summary{}, false, err
	}
	defer sel.release()
	total := sel.count()
	if total == 0 {
		return block.summary, filters == nil, nil
	}

	cache.prepareOutput(colMask, total, sel.mode == selRange)

	if cache.fullBlock {
		if err := cache.loadAll(colMask); err != nil {
			return Summary{}, false, err
		}
		accSelectionPart(&cache.cols, sel, slotIdx, ps)
	} else {
		rowBytes := cache.packRowBytes(colMask, sel.mode == selRange)
		avail := cache.availPackBytes()
		chunkRows := total
		if rowBytes > 0 && total*rowBytes > avail {
			chunkRows = max(avail/rowBytes, 1)
		}
		var rowIdxBuf []int32
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
			accColsPartRange(&cols, 0, n, slotIdx, start, ps)
		}
	}
	if filters == nil {
		return block.summary, true, nil
	}
	return ps.summary, true, nil
}

func accSelectionPart(cols *[streamColumnCount]decodedCol, sel rowSelection, slotIdx int, ps *partState) {
	if sel.mode == selRange {
		accColsPartRange(cols, sel.lo, sel.hi, slotIdx, 0, ps)
		return
	}
	total := sel.count()
	var rowIdxBuf []int32
	for start := 0; start < total; start += 4096 {
		n := min(4096, total-start)
		_, _, rows := sel.sliceRows(start, n, rowIdxBuf)
		if cap(rows) > cap(rowIdxBuf) {
			rowIdxBuf = rows[:0]
		}
		accColsPartIndexed(cols, rows, slotIdx, start, ps)
	}
}

func aggregateRowsPartMatched(rows []idRow, blockSummary Summary, slotIdx int, filters []compiledFilter, ps *partState) (Summary, bool) {
	if filters == nil && len(rows) == 0 {
		return blockSummary, true
	}
	matched := filters == nil
	slotBase := uint64(slotIdx+1) << 32
	for rowIdx, row := range rows {
		if filters != nil && !matchesCompiledRow(row, filters) {
			continue
		}
		matched = true
		ps.addRow(row.tags[ps.dim], row.values[0], row.values[1], row.values[2], row.values[3], normalizeMask(row.initMask), slotBase|uint64(rowIdx+1))
	}
	if !matched {
		return Summary{}, false
	}
	if filters == nil {
		return blockSummary, true
	}
	return ps.summary, true
}
