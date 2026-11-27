package strata

import (
	"context"
	"sync"
	"sync/atomic"
)

type aggSlotResult struct {
	summary Summary
	matched bool
	err     error
}

func executeAggregateSlots(ctx context.Context, res *queryResult, out *AggregateResult, plans []plannedSlot, sources querySources, s *Series, db *queryExec, filters []compiledFilter, groupDims []int, maxWorkers, candidateUpper int, slot *querySlot) ([]aggregateGroups, error) {
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

	runSlot := func(index int, localGroups []aggregateGroups, workerSlot *querySlot) aggSlotResult {
		if err := ctx.Err(); err != nil {
			return aggSlotResult{err: err}
		}
		block, bufPtr, err := mergeSlot(db.view, plans[index].block, sources, s)
		if bufPtr != nil {
			defer putReadBuf(bufPtr)
		}
		if err != nil {
			return aggSlotResult{err: err}
		}
		var summary Summary
		var matched bool
		if block.encoded != nil {
			summary, matched, err = aggregateEncodedBlock(block, index, s.cfg.Arity, filters, localGroups, groupDims, s.cfg.RowOps, workerSlot, arenaBudget)
		} else {
			summary, matched = aggregateDecodedRows(block.rows, block.summary, index, filters, localGroups, groupDims, s.cfg.Arity, s.cfg.RowOps)
		}
		return aggSlotResult{summary: summary, matched: matched, err: err}
	}

	out.Segments = getSegmentSummarySlice(len(plans))
	merge := func(index int, sr aggSlotResult) error {
		if sr.err != nil {
			return sr.err
		}
		if sr.matched {
			out.Segments = append(out.Segments, SegmentSummary{Meta: plans[index].meta, Summary: sr.summary})
			order := uint64(len(out.Segments))
			mergeSummary(&res.Summary, sr.summary, s.cfg.SummaryOps, order, order-1)
		}
		return nil
	}
	if workers <= 1 {
		groups := makeGroups()
		for i := range plans {
			if err := merge(i, runSlot(i, groups, slot)); err != nil {
				return nil, err
			}
		}
		return groups, nil
	}

	results := make([]aggSlotResult, len(plans))
	workerGroups := make([][]aggregateGroups, workers)
	var next atomic.Uint32
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			ws := slot
			if w > 0 {
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
					workerGroups[w] = local
				}
				results[index] = runSlot(index, local, ws)
			}
		})
	}
	wg.Wait()
	for i, sr := range results {
		if err := merge(i, sr); err != nil {
			return nil, err
		}
	}
	var mergedGroups []aggregateGroups
	for _, local := range workerGroups {
		if local == nil {
			continue
		}
		if mergedGroups == nil {
			mergedGroups = local
			continue
		}
		for i := range mergedGroups {
			mergedGroups[i].mergeFrom(&local[i], s.cfg.RowOps)
		}
	}
	return mergedGroups, nil
}
