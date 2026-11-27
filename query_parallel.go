package strata

import (
	"context"
	"sync"
	"sync/atomic"
)

type aggSlotResult struct {
	meta    SegmentMeta
	summary Summary
	matched bool
	err     error
}

func executeAggregateSlots(ctx context.Context, res *queryResult, out *AggregateResult, plans []plannedSlot, sources querySources, s *Series, db *DB, filters []compiledFilter, groupDims []int, maxWorkers, candidateUpper int, slot *querySlot) ([]aggregateGroups, error) {
	filtered := filters != nil
	var groupUpper int
	if filtered && len(groupDims) == 1 && candidateUpper > 0 {
		groupUpper = candidateUpper
		for _, f := range filters {
			if f.dim == groupDims[0] && !f.negate && len(f.ids) > 0 && f.ids[0] != 0 {
				groupUpper = min(groupUpper, len(f.ids))
			}
		}
	}
	workers := 1
	groupBudget := max(db.policy.queryBytes-(1<<20), 512<<10)
	makeGroups := func() []aggregateGroups {
		if len(groupDims) == 0 {
			return nil
		}
		g := make([]aggregateGroups, len(groupDims))
		for i, dim := range groupDims {
			g[i] = newAggregateGroups(db.dict, s.id, dim, filtered, s.cfg.RowOps, groupUpper, groupBudget/int64(workers))
		}
		return g
	}
	if len(plans) == 0 {
		return makeGroups(), nil
	}
	estRows := 0
	for _, plan := range plans {
		estRows += int(plan.block.rowCount)
	}
	if len(plans) >= 8 && estRows >= 4096 && maxWorkers > 1 {
		workers = min(maxWorkers, len(plans))
		if len(groupDims) > 0 {
			perWorker := estimateGroupBytes(db.dict, s.id, groupDims, filtered, s.cfg.RowOps)
			if perWorker > 0 {
				workers = clampInt(int(groupBudget/perWorker), 1, workers)
			}
		}
	}
	arenaBudget := clampInt(int(db.policy.queryBytes/int64(max(workers*4, 8))), 128<<10, 1<<20)

	results := make([]aggSlotResult, len(plans))
	runSlot := func(index int, localGroups []aggregateGroups, workerSlot *querySlot) {
		if err := ctx.Err(); err != nil {
			results[index].err = err
			return
		}
		plan := plans[index]
		block, bufPtr, err := mergeSlot(db, plan.block, sources, s)
		if bufPtr != nil {
			defer putReadBuf(bufPtr)
		}
		if err != nil {
			results[index].err = err
			return
		}
		var summary Summary
		var matched bool
		if block.encoded != nil {
			summary, matched, err = aggregateEncodedBlock(block, index, s.cfg.Arity, filters, localGroups, groupDims, s.cfg.RowOps, workerSlot, arenaBudget)
		} else {
			summary, matched = aggregateDecodedRows(block.rows, block.summary, index, filters, localGroups, groupDims, s.cfg.Arity, s.cfg.RowOps)
		}
		results[index] = aggSlotResult{meta: plan.meta, summary: summary, matched: matched, err: err}
	}

	var mergedGroups []aggregateGroups
	if workers <= 1 {
		mergedGroups = makeGroups()
		for i := range plans {
			runSlot(i, mergedGroups, slot)
			if results[i].err != nil {
				return nil, results[i].err
			}
		}
	} else {
		workerGroups := make([][]aggregateGroups, workers)
		var next atomic.Uint32
		var wg sync.WaitGroup
		wg.Add(workers)
		for w := range workers {
			go func(workerID int) {
				defer wg.Done()
				ws := slot
				if workerID > 0 {
					ws = &querySlot{codecs: db.codecs}
					defer ws.close()
				}
				var local []aggregateGroups
				for {
					index := int(next.Add(1) - 1)
					if index >= len(plans) {
						return
					}
					if local == nil && len(groupDims) > 0 {
						local = makeGroups()
						workerGroups[workerID] = local
					}
					runSlot(index, local, ws)
				}
			}(w)
		}
		wg.Wait()
		if len(groupDims) > 0 {
			for _, wgLocal := range workerGroups {
				if wgLocal == nil {
					continue
				}
				if mergedGroups == nil {
					mergedGroups = wgLocal
					continue
				}
				for i := range mergedGroups {
					mergedGroups[i].mergeFrom(&wgLocal[i], s.cfg.RowOps)
				}
			}
			if mergedGroups == nil {
				mergedGroups = makeGroups()
			}
		}
	}

	out.Segments = getSegmentSummarySlice(len(plans))
	var order uint64
	for _, sr := range results {
		if sr.err != nil {
			return nil, sr.err
		}
		if !sr.matched {
			continue
		}
		out.Segments = append(out.Segments, SegmentSummary{Meta: sr.meta, Summary: sr.summary})
		order++
		mergeSummary(&res.Summary, sr.summary, s.cfg.SummaryOps, order, order-1)
	}
	return mergedGroups, nil
}
