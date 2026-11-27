package strata

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"slices"
	"time"
)

func emptyAggregateGroups(n int) [][]GroupRow {
	if n == 0 {
		return nil
	}
	out := make([][]GroupRow, n)
	for i := range out {
		out[i] = make([]GroupRow, 0)
	}
	return out
}

func (s *Series) Aggregate(ctx context.Context, q AggregateQuery) (*AggregateResult, error) {
	return s.aggregateWithDefaultWindow(ctx, q, 0)
}

func (s *Series) aggregateWithDefaultWindow(ctx context.Context, q AggregateQuery, defaultWindow time.Duration) (*AggregateResult, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("strata: nil series")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db := s.db
	opts := queryOptions{RequireExact: q.RequireExact, PreferFine: q.PreferFine}

	// Summary-only fast path: zero dict snapshot, zero file view, zero disk I/O, bypasses querySem.
	if len(q.GroupDims) == 0 && len(q.Filters) == 0 {
		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			return nil, errors.New("strata: database is closed")
		}
		from, to, emptyWin, winErr := resolveTimeWindow(db, s.id, q.From, q.To, q.Window, q.AllTime, defaultWindow)
		if winErr != nil {
			db.mu.RUnlock()
			return nil, winErr
		}
		reqFrom, reqTo := time.Unix(from, 0).UTC(), time.Unix(to, 0).UTC()
		out := &AggregateResult{
			RequestedFrom: reqFrom, RequestedTo: reqTo,
			EffectiveFrom: reqFrom, EffectiveTo: reqTo, Exact: true,
		}
		if emptyWin {
			db.mu.RUnlock()
			return out, nil
		}
		sources := buildSummarySourcesLocked(db, s, from, to)
		execSeries := *s
		execSeries.rules = sources.fine.rules
		db.mu.RUnlock()

		qres := queryResult{
			RequestedFrom: reqFrom, RequestedTo: reqTo,
			EffectiveFrom: reqFrom, EffectiveTo: reqTo, Exact: true,
		}
		if err := aggregateSummaryQueryPlan(&qres, from, to, sources, &execSeries, opts, out); err != nil {
			out.Release()
			return nil, err
		}
		out.RequestedFrom, out.RequestedTo = qres.RequestedFrom, qres.RequestedTo
		out.EffectiveFrom, out.EffectiveTo = qres.EffectiveFrom, qres.EffectiveTo
		out.Exact, out.Summary = qres.Exact, qres.Summary
		return out, nil
	}

	maxWorkers, _, slot, release, err := db.acquireQuery(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	pq, err := prepareQuery(db, s, windowSpec{
		from: q.From, to: q.To, window: q.Window, allTime: q.AllTime, defaultWindow: defaultWindow,
	}, q.Filters, opts, slot)
	if err != nil {
		return nil, err
	}
	defer pq.release()

	reqFrom, reqTo := time.Unix(pq.from, 0).UTC(), time.Unix(pq.to, 0).UTC()
	out := &AggregateResult{
		RequestedFrom: reqFrom, RequestedTo: reqTo,
		EffectiveFrom: reqFrom, EffectiveTo: reqTo, Exact: true,
	}
	if pq.empty {
		out.Groups = emptyAggregateGroups(len(q.GroupDims))
		return out, nil
	}

	partition := shouldPartitionTopK(pq.execDB.dict, s.id, q.GroupDims, pq.execSeries.cfg.Arity, q.TopK)
	var candidateUpper int
	var upperKnown bool
	if partition || (len(q.GroupDims) == 1 && pq.compiled != nil && pq.execDB.dict.dimMaxID(s.id, q.GroupDims[0]) > 128) {
		candidateUpper, upperKnown = estimateCandidateRowUpperBound(pq.plans, pq.sources, &pq.execSeries)
	}
	if partition && (!upperKnown || candidateUpper > maxSinglePassGroups(pq.execDB.policy, pq.execSeries.cfg.RowOps)) {
		rows, err := executePartitionedTopK(ctx, &pq, out, q.GroupDims[0], q.TopK, maxWorkers, slot)
		if err != nil {
			out.Release()
			return nil, err
		}
		out.RequestedFrom, out.RequestedTo = pq.qres.RequestedFrom, pq.qres.RequestedTo
		out.EffectiveFrom, out.EffectiveTo = pq.qres.EffectiveFrom, pq.qres.EffectiveTo
		out.Exact, out.Summary = pq.qres.Exact, pq.qres.Summary
		out.Groups = [][]GroupRow{rows}
		return out, nil
	}

	groups, err := executeAggregateSlots(ctx, &pq.qres, out, pq.plans, pq.sources, &pq.execSeries, pq.execDB, pq.compiled, q.GroupDims, maxWorkers, candidateUpper, slot)
	if err != nil {
		out.Release()
		return nil, err
	}
	out.RequestedFrom, out.RequestedTo = pq.qres.RequestedFrom, pq.qres.RequestedTo
	out.EffectiveFrom, out.EffectiveTo = pq.qres.EffectiveFrom, pq.qres.EffectiveTo
	out.Exact, out.Summary = pq.qres.Exact, pq.qres.Summary
	if len(q.GroupDims) == 0 {
		return out, nil
	}
	out.Groups = make([][]GroupRow, len(groups))
	for i := range groups {
		out.Groups[i] = materializeGroupRows(&groups[i], pq.execDB.dict, s.id, q.GroupDims[i], q.TopK, slot)
	}
	return out, nil
}

// Release clears internal slice references and returns bounded working buffers to internal pools.
// It is nil-safe and idempotent.
func (r *AggregateResult) Release() {
	if r == nil || r.released {
		return
	}
	r.released = true
	for i := range r.Groups {
		putGroupRowSlice(r.Groups[i])
		r.Groups[i] = nil
	}
	r.Groups = nil
	putSegmentSummarySlice(r.Segments)
	r.Segments = nil
}

type groupRankEntry struct {
	id  uint32
	sum Summary
	tag []byte
}

func compareMetricRank(a, b Summary) int {
	sa := a.Values[0] + a.Values[1]
	sb := b.Values[0] + b.Values[1]
	if sa != sb {
		if sa > sb {
			return 1
		}
		return -1
	}
	if a.Values[2] != b.Values[2] {
		if a.Values[2] > b.Values[2] {
			return 1
		}
		return -1
	}
	if a.Values[3] != b.Values[3] {
		if a.Values[3] > b.Values[3] {
			return 1
		}
		return -1
	}
	return 0
}

func compareEntryRank(a, b groupRankEntry) int {
	if c := compareMetricRank(a.sum, b.sum); c != 0 {
		return c
	}
	return -bytes.Compare(a.tag, b.tag)
}

func heapSiftUp(items []groupRankEntry, i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if compareEntryRank(items[i], items[parent]) >= 0 {
			break
		}
		items[i], items[parent] = items[parent], items[i]
		i = parent
	}
}

func heapSiftDown(items []groupRankEntry, i int) {
	n := len(items)
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		smallest := left
		if right := left + 1; right < n && compareEntryRank(items[right], items[left]) < 0 {
			smallest = right
		}
		if compareEntryRank(items[i], items[smallest]) <= 0 {
			break
		}
		items[i], items[smallest] = items[smallest], items[i]
		i = smallest
	}
}

func materializeGroupRows(g *aggregateGroups, d dictionary, series uint16, dim, topK int, slot *querySlot) []GroupRow {
	dimU8 := uint8(dim)
	if topK <= 0 {
		n := g.countNonEmpty()
		rows := getGroupRowSlice(n)
		if g.dense != nil {
			g.each(func(id uint32, sum Summary) {
				rows = append(rows, GroupRow{Tag: d.resolve(series, dimU8, id), Summary: sum})
			})
		} else {
			entries := make([]groupRankEntry, 0, n)
			g.each(func(id uint32, sum Summary) {
				entries = append(entries, groupRankEntry{id: id, sum: sum})
			})
			slices.SortFunc(entries, func(a, b groupRankEntry) int {
				return cmp.Compare(a.id, b.id)
			})
			reader := newDictTagReader(d, series, dimU8, slot)
			defer reader.close()
			for _, e := range entries {
				rows = append(rows, GroupRow{Tag: string(reader.resolveBytes(e.id)), Summary: e.sum})
			}
		}
		slices.SortFunc(rows, func(a, b GroupRow) int {
			return cmp.Compare(a.Tag, b.Tag)
		})
		return rows
	}
	reader := newDictTagReader(d, series, dimU8, slot)
	defer reader.close()
	h := newTopKHeap(topK, &reader)
	if g.dense != nil {
		g.each(func(id uint32, sum Summary) {
			h.push(id, sum)
		})
	} else {
		n := g.countNonEmpty()
		entries := make([]groupRankEntry, 0, n)
		g.each(func(id uint32, sum Summary) {
			entries = append(entries, groupRankEntry{id: id, sum: sum})
		})
		slices.SortFunc(entries, func(a, b groupRankEntry) int {
			return cmp.Compare(a.id, b.id)
		})
		for _, e := range entries {
			h.push(e.id, e.sum)
		}
	}
	return h.toGroupRows()
}

func sortTopKGroupRows(rows []GroupRow) {
	slices.SortFunc(rows, func(a, b GroupRow) int {
		if c := compareMetricRank(b.Summary, a.Summary); c != 0 {
			return c
		}
		return cmp.Compare(a.Tag, b.Tag)
	})
}
