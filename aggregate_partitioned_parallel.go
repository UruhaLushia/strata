package strata

import (
	"context"
	"sync"
	"sync/atomic"
)

func calcPartitionWorkers(policy memPolicy, maxWorkers, activeCount int, ops [4]MetricOp, arenaBudget int) int {
	if policy.constrained || policy.queryBytes <= 4<<20 || maxWorkers <= 1 || activeCount < 2 {
		return 1
	}
	orderBytes := orderEntryBytes(ops)
	perWorker := int64(aggPartitionSize)*(40+orderBytes) + int64(arenaBudget) + (64 << 10)
	if perWorker <= 0 {
		return 1
	}
	return clampInt(int((policy.queryBytes-(2<<20))/perWorker), 1, min(maxWorkers, activeCount, 4))
}

func runRemainingPartitions(ctx context.Context, pq *preparedQuery, activeParts []int, partBits []uint64, partWords, dim, topK, maxWorkers, arenaBudget int, ps *partState, h *topKHeap, slot *querySlot) error {
	s, db := &pq.execSeries, pq.execDB
	partWorkers := calcPartitionWorkers(db.policy, maxWorkers, len(activeParts), s.cfg.RowOps, arenaBudget)
	ps.bitsRow = nil
	ps.trackSummary = false

	if partWorkers <= 1 {
		for _, part := range activeParts {
			if err := runOnePartition(ctx, pq, part, partBits, partWords, nil, ps, h, slot, arenaBudget); err != nil {
				return err
			}
		}
		return nil
	}

	dimU8 := uint8(dim)
	workerItems := make([][]groupRankEntry, partWorkers)
	var next atomic.Uint32
	var failed atomic.Bool
	var errMu sync.Mutex
	var firstErr error
	setErr := func(err error) {
		if err == nil {
			return
		}
		failed.Store(true)
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	var wg sync.WaitGroup
	for w := 1; w < partWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ws := &querySlot{codecs: pq.execDB.codecs}
			defer ws.close()
			wReader := newDictTagReader(db.dict, s.id, dimU8, ws)
			defer wReader.close()
			wh := newTopKHeap(topK, &wReader)
			wDense := make([]Summary, aggPartitionSize)
			var wLastOrd []uint64
			var wMultiOrd [][4]uint64
			if ps.lastCount == 1 {
				wLastOrd = make([]uint64, aggPartitionSize)
			} else if ps.lastCount >= 2 {
				wMultiOrd = make([][4]uint64, aggPartitionSize)
			}
			wps := partState{
				dim: dim, ops: ps.ops, allSum: ps.allSum,
				lastCount: ps.lastCount, lastIdx: ps.lastIdx, numParts: ps.numParts,
				dense: wDense, denseLastOrder: wLastOrd, denseOrder: wMultiOrd,
			}
			if err := runPartitionWorker(ctx, pq, activeParts, partBits, partWords, &next, &failed, &wps, &wh, ws, arenaBudget); err != nil {
				setErr(err)
				return
			}
			workerItems[workerID] = wh.items
		}(w)
	}

	if err := runPartitionWorker(ctx, pq, activeParts, partBits, partWords, &next, &failed, ps, h, slot, arenaBudget); err != nil {
		setErr(err)
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	for w := 1; w < partWorkers; w++ {
		for _, e := range workerItems[w] {
			h.mergeEntry(e)
		}
	}
	return nil
}

func runPartitionWorker(ctx context.Context, pq *preparedQuery, activeParts []int, partBits []uint64, partWords int, next *atomic.Uint32, failed *atomic.Bool, ps *partState, h *topKHeap, slot *querySlot, arenaBudget int) error {
	for {
		if failed.Load() {
			return nil
		}
		idx := int(next.Add(1) - 1)
		if idx >= len(activeParts) {
			return nil
		}
		if err := runOnePartition(ctx, pq, activeParts[idx], partBits, partWords, failed, ps, h, slot, arenaBudget); err != nil {
			return err
		}
	}
}

func runOnePartition(ctx context.Context, pq *preparedQuery, part int, partBits []uint64, partWords int, failed *atomic.Bool, ps *partState, h *topKHeap, slot *querySlot, arenaBudget int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	clear(ps.dense)
	if ps.lastCount == 1 {
		clear(ps.denseLastOrder)
	} else if ps.lastCount >= 2 {
		clear(ps.denseOrder)
	}
	ps.loID = uint32(part) << aggPartitionShift
	ps.hiID = ps.loID + aggPartitionSize
	wIdx, bit := part>>6, uint64(1)<<(part&63)
	s, db, plans, filters := &pq.execSeries, pq.execDB, pq.plans, pq.compiled
	for index := range plans {
		if partBits[index*partWords+wIdx]&bit == 0 {
			continue
		}
		if failed != nil && failed.Load() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		block, bufPtr, err := mergeSlot(db, plans[index].block, pq.sources, s)
		if err != nil {
			putReadBuf(bufPtr)
			return err
		}
		_, _, err = aggregateBlockPart(block, index, filters, ps, slot, arenaBudget)
		putReadBuf(bufPtr)
		if err != nil {
			return err
		}
	}
	pushDenseToHeap(h, ps.dense, ps.loID)
	return h.reader.err
}

func updatePartDenseFullMask(ps *partState, idx uint32, v0, v1, v2, v3 int64, ord uint64) {
	dst := &ps.dense[idx]
	switch dst.Mask {
	case 0x0f:
		applyMetricOp(&dst.Values[0], v0, ps.ops[0])
		applyMetricOp(&dst.Values[1], v1, ps.ops[1])
		applyMetricOp(&dst.Values[2], v2, ps.ops[2])
		applyMetricOp(&dst.Values[3], v3, ps.ops[3])
		if ps.lastCount == 1 {
			ps.denseLastOrder[idx] = ord
		} else if ps.lastCount >= 2 {
			ps.denseOrder[idx] = [4]uint64{ord, ord, ord, ord}
		}
	case 0:
		dst.Mask = 0x0f
		dst.Values = [4]int64{v0, v1, v2, v3}
		if ps.lastCount == 1 {
			ps.denseLastOrder[idx] = ord
		} else if ps.lastCount >= 2 {
			ps.denseOrder[idx] = [4]uint64{ord, ord, ord, ord}
		}
	default:
		ps.addPartSummary(idx, Summary{Values: [4]int64{v0, v1, v2, v3}, Mask: 0x0f}, ord)
	}
}

func accColsPartRange(cols *[streamColumnCount]decodedCol, lo, hi, slotIdx, orderOffset int, ps *partState) {
	tagCol := cols[ps.dim]
	c0, c1, c2, c3, cm := cols[8], cols[9], cols[10], cols[11], cols[12]
	constMask := cm.mode == modeConst
	fixedMask := byte(0)
	if constMask {
		fixedMask = normalizeMask(byte(cm.val))
	}
	loID, hiID := ps.loID, ps.hiID
	dense := ps.dense
	var tagBuf [aggTileRows]uint32

	if !ps.trackSummary {
		if ps.allSum {
			for base := lo; base < hi; base += aggTileRows {
				n := min(aggTileRows, hi-base)
				decodeTagRange(tagBuf[:n], tagCol, base)
				for i := range n {
					id := tagBuf[i]
					if ps.bitsRow != nil {
						ps.markBit(id)
					}
					if id < loID || id >= hiID {
						continue
					}
					row := base + i
					mask := fixedMask
					if !constMask {
						mask = normalizeMask(cm.maskAt(row))
					}
					addSumToSummary(&dense[id-loID], c0.valAt(row), c1.valAt(row), c2.valAt(row), c3.valAt(row), mask)
				}
			}
			return
		}
		slotBase := uint64(slotIdx+1) << 32
		fullConstMask := constMask && fixedMask == 0x0f
		for pos, base := 0, lo; base < hi; pos, base = pos+aggTileRows, base+aggTileRows {
			n := min(aggTileRows, hi-base)
			decodeTagRange(tagBuf[:n], tagCol, base)
			baseOrd := slotBase | uint64(orderOffset+pos+1)
			for i := range n {
				id := tagBuf[i]
				if ps.bitsRow != nil {
					ps.markBit(id)
				}
				if id < loID || id >= hiID {
					continue
				}
				row := base + i
				idx := id - loID
				ord := baseOrd + uint64(i)
				v0, v1, v2, v3 := c0.valAt(row), c1.valAt(row), c2.valAt(row), c3.valAt(row)
				if fullConstMask {
					updatePartDenseFullMask(ps, idx, v0, v1, v2, v3, ord)
					continue
				}
				mask := fixedMask
				if !constMask {
					mask = normalizeMask(cm.maskAt(row))
				}
				ps.addPartSummary(idx, Summary{Values: [4]int64{v0, v1, v2, v3}, Mask: mask}, ord)
			}
		}
		return
	}

	slotBase := uint64(slotIdx+1) << 32
	for pos, base := 0, lo; base < hi; pos, base = pos+aggTileRows, base+aggTileRows {
		n := min(aggTileRows, hi-base)
		decodeTagRange(tagBuf[:n], tagCol, base)
		baseOrd := slotBase | uint64(orderOffset+pos+1)
		for i := range n {
			row := base + i
			mask := fixedMask
			if !constMask {
				mask = normalizeMask(cm.maskAt(row))
			}
			ps.addRow(tagBuf[i], c0.valAt(row), c1.valAt(row), c2.valAt(row), c3.valAt(row), mask, baseOrd+uint64(i))
		}
	}
}

func accColsPartIndexed(cols *[streamColumnCount]decodedCol, rows []int32, slotIdx, orderOffset int, ps *partState) {
	tagCol := cols[ps.dim]
	c0, c1, c2, c3, cm := cols[8], cols[9], cols[10], cols[11], cols[12]
	constMask := cm.mode == modeConst
	fixedMask := byte(0)
	if constMask {
		fixedMask = normalizeMask(byte(cm.val))
	}
	fullConstMask := constMask && fixedMask == 0x0f
	loID, hiID := ps.loID, ps.hiID
	slotBase := uint64(slotIdx+1) << 32
	var tagBuf [aggTileRows]uint32
	for base := 0; base < len(rows); base += aggTileRows {
		n := min(aggTileRows, len(rows)-base)
		sub := rows[base : base+n]
		decodeTagIndexed(tagBuf[:n], tagCol, sub)
		baseOrd := slotBase | uint64(orderOffset+base+1)
		if !ps.trackSummary {
			for i := range n {
				id := tagBuf[i]
				if ps.bitsRow != nil {
					ps.markBit(id)
				}
				if id < loID || id >= hiID {
					continue
				}
				row := int(sub[i])
				v0, v1, v2, v3 := c0.valAt(row), c1.valAt(row), c2.valAt(row), c3.valAt(row)
				idx := id - loID
				if ps.allSum {
					mask := fixedMask
					if !constMask {
						mask = normalizeMask(cm.maskAt(row))
					}
					addSumToSummary(&ps.dense[idx], v0, v1, v2, v3, mask)
					continue
				}
				ord := baseOrd + uint64(i)
				if fullConstMask {
					updatePartDenseFullMask(ps, idx, v0, v1, v2, v3, ord)
					continue
				}
				mask := fixedMask
				if !constMask {
					mask = normalizeMask(cm.maskAt(row))
				}
				ps.addPartSummary(idx, Summary{Values: [4]int64{v0, v1, v2, v3}, Mask: mask}, ord)
			}
			continue
		}
		for i := range n {
			row := int(sub[i])
			mask := fixedMask
			if !constMask {
				mask = normalizeMask(cm.maskAt(row))
			}
			ps.addRow(tagBuf[i], c0.valAt(row), c1.valAt(row), c2.valAt(row), c3.valAt(row), mask, baseOrd+uint64(i))
		}
	}
}
