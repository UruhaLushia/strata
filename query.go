package strata

import "time"

type plannedSlot struct {
	block sealedMeta
	meta  SegmentMeta
}

type windowSpec struct {
	from, to      time.Time
	window        time.Duration
	allTime       bool
	defaultWindow time.Duration
	fixed         bool
	fixedFrom     int64
	fixedTo       int64
	fixedPlanFrom int64
}

type queryExec struct {
	dict   dictionary
	policy memPolicy
	codecs *codecTable
	view   *fileView
}

type preparedQuery struct {
	from, to   int64
	exec       queryExec
	execSeries Series
	sources    querySources
	plans      []plannedSlot
	compiled   []compiledFilter
	qres       queryResult
	empty      bool
}

func (p *preparedQuery) release() {
	p.exec.view.release()
	p.exec.view = nil
}

func prepareQuery(db *DB, s *Series, ws windowSpec, filters []Filter, opts queryOptions, slot *querySlot) (preparedQuery, error) {
	var pq preparedQuery
	snap, planFrom, empty, err := captureQuerySnapshot(db, s, ws, filters)
	if err != nil {
		return pq, err
	}
	defer snap.release()
	pq.from, pq.to = snap.from, snap.to
	if empty {
		pq.empty = true
		return pq, nil
	}
	pq.qres = queryResult{
		RequestedFrom: time.Unix(planFrom, 0).UTC(), RequestedTo: time.Unix(pq.to, 0).UTC(),
		EffectiveFrom: time.Unix(planFrom, 0).UTC(), EffectiveTo: time.Unix(pq.to, 0).UTC(),
		Exact: true,
	}

	fineIdx := sealedRangeIndexes(snap.sealed, snap.fineAll, planFrom, pq.to)
	hourIdx := sealedRangeIndexes(snap.sealed, snap.hourAll, planFrom, pq.to)
	dayIdx := sealedRangeIndexes(snap.sealed, snap.dayAll, planFrom, pq.to)
	var sealedBits *blockBitmap
	if len(filters) > 0 {
		ordered := filters
		var compiled []compiledFilter
		hasLiveCandidate := snap.hasLiveMatch
		if snap.livePreFiltered {
			id := snap.liveTagID
			compiled = []compiledFilter{{dim: filters[0].Dim, ids: []uint32{id}, single: id}}
		} else {
			compiled, err = compileFiltersSnap(snap.dict, snap.series.id, snap.series.cfg.Arity, filters, slot)
			if err != nil {
				return preparedQuery{}, err
			}
			ordered, compiled = orderCompiledFilters(filters, compiled)
		}
		for i, f := range ordered {
			if !f.Negate && len(compiled[i].ids) == 0 {
				pq.empty = true
				return pq, nil
			}
		}
		if !snap.livePreFiltered {
			hasLiveCandidate = filterLiveCandidates(snap.liveFine, compiled)
		}
		emptyCand := !hasLiveCandidate
		if len(fineIdx)+len(hourIdx)+len(dayIdx) != 0 {
			sealedBits, emptyCand, err = searchCandidatesSnap(&snap, [3][]uint32{fineIdx, hourIdx, dayIdx}, ordered, compiled, hasLiveCandidate, slot)
			if err != nil {
				return preparedQuery{}, err
			}
		}
		if emptyCand {
			pq.empty = true
			return pq, nil
		}
		pq.compiled = compiled
		pq.qres.sparse = true
	}

	pq.sources = buildQuerySourcesFromSnap(&snap, fineIdx, hourIdx, dayIdx, planFrom, pq.to, sealedBits)
	pq.execSeries = snap.series
	plans, err := planQuerySlots(&pq.qres, planFrom, pq.to, pq.sources, &pq.execSeries, opts, sealedBits)
	if err != nil {
		return preparedQuery{}, err
	}
	pq.plans = plans
	pq.exec = queryExec{dict: snap.dict, policy: snap.policy, codecs: snap.codecs, view: snap.view}
	snap.view = nil
	return pq, nil
}

func resolveTimeWindow(db *DB, seriesID uint16, from, to time.Time, window time.Duration, allTime bool, defaultWindow time.Duration) (int64, int64, bool, error) {
	if allTime {
		if !from.IsZero() || !to.IsZero() || window != 0 {
			return 0, 0, false, ErrInvalidWindow
		}
		minTS, maxTS := searchBounds(db, seriesID)
		if maxTS <= minTS {
			return minTS, maxTS, true, nil
		}
		return minTS, maxTS, false, nil
	}
	if window < 0 {
		return 0, 0, false, ErrInvalidWindow
	}
	now := db.opts.Now().UTC()
	if !from.IsZero() || !to.IsZero() {
		if !from.IsZero() && !to.IsZero() {
			if window != 0 || to.Before(from) {
				return 0, 0, false, ErrInvalidWindow
			}
			if to.Equal(from) {
				return from.Unix(), to.Unix(), true, nil
			}
			return from.Unix(), to.Unix(), false, nil
		}
		if !from.IsZero() {
			if window != 0 {
				return 0, 0, false, ErrInvalidWindow
			}
			to = now
			if to.Before(from) {
				return 0, 0, false, ErrInvalidWindow
			}
			if to.Equal(from) {
				return from.Unix(), to.Unix(), true, nil
			}
			return from.Unix(), to.Unix(), false, nil
		}
		// Only To is non-zero.
		if window <= 0 {
			return 0, 0, false, ErrWindowRequired
		}
		from = to.Add(-window)
		return from.Unix(), to.Unix(), false, nil
	}
	if window > 0 {
		return now.Add(-window).Unix(), now.Unix(), false, nil
	}
	if defaultWindow > 0 {
		return now.Add(-defaultWindow).Unix(), now.Unix(), false, nil
	}
	return 0, 0, false, ErrWindowRequired
}

func walkQuerySlots(res *queryResult, from, to int64, sources querySources, s *Series, opts queryOptions, onSlot func(chosen sealedMeta, meta SegmentMeta) error) error {
	cursor := from
	fineCursor := blockCursor{src: sources.fine}
	hourCursor := blockCursor{src: sources.hours}
	dayCursor := blockCursor{src: sources.days}
	for cursor < to {
		var chosen sealedMeta
		hasChosen := false
		if !opts.PreferFine {
			if b, ok := dayCursor.partial(cursor, &fineCursor, &hourCursor, s); ok {
				chosen, hasChosen = b, true
			} else if b, ok := dayCursor.aligned(cursor, to, 86400, s); ok {
				chosen, hasChosen = b, true
			} else if b, ok := hourCursor.aligned(cursor, to, 3600, s); ok {
				chosen, hasChosen = b, true
			}
		}
		if !hasChosen {
			if b, ok := fineCursor.covering(cursor); ok {
				chosen, hasChosen = b, true
			}
		}
		if !hasChosen && !opts.PreferFine {
			if b, ok := hourCursor.coveringCoarse(cursor, s); ok {
				chosen, hasChosen = b, true
			} else if b, ok := dayCursor.coveringCoarse(cursor, s); ok {
				chosen, hasChosen = b, true
			}
		} else if hasChosen && chosen.res == resMinuteHR && !opts.PreferFine && !res.sparse {
			if b, ok := hourCursor.coveringCoarse(cursor, s); ok && !fineCursor.candidateCoversUntil(cursor, b.end()) {
				chosen = b
			} else if b, ok := dayCursor.coveringCoarse(cursor, s); ok && !fineCursor.candidateCoversUntil(cursor, b.end()) {
				chosen = b
			}
		}
		if res.sparse && hasChosen && chosen.res != resMinuteHR && sparseFinerGapCursor(chosen.res, cursor, &fineCursor, &hourCursor) {
			hasChosen = false
		}
		if !hasChosen {
			if coarseCutByRuleCursor(cursor, s, &hourCursor, &dayCursor) {
				return ErrResetRequiresFineData
			}
			next := fineCursor.nextCandidate(cursor)
			if next == cursor {
				next = minCursorNext(hourCursor.nextCandidate(cursor), dayCursor.nextCandidate(cursor))
			}
			if res.sparse {
				next = minCursorNext(fineCursor.nextCandidate(cursor), hourCursor.nextCandidate(cursor))
				next = minCursorNext(next, dayCursor.nextCandidate(cursor))
			}
			if next > cursor {
				if opts.RequireExact && !res.sparse {
					return ErrResolutionTooCoarse
				}
				if !res.sparse {
					res.Exact = false
				}
				if next > to {
					next = to
				}
				cursor = next
				continue
			}
			if opts.RequireExact && !res.sparse {
				return ErrResolutionTooCoarse
			}
			if !res.sparse {
				res.Exact = false
			}
			break
		}
		chosenEnd := chosen.end()
		segmentFrom, segmentTo := cursor, chosenEnd
		if segmentTo > to {
			segmentTo = to
		}
		exact := segmentFrom == chosen.start && segmentTo == chosenEnd
		if !exact {
			res.Exact = false
			if opts.RequireExact {
				return ErrResolutionTooCoarse
			}
		}
		meta := SegmentMeta{
			RequestedFrom: time.Unix(segmentFrom, 0).UTC(),
			RequestedTo:   time.Unix(segmentTo, 0).UTC(),
			CoveredFrom:   time.Unix(chosen.start, 0).UTC(),
			CoveredTo:     time.Unix(chosenEnd, 0).UTC(),
			Resolution:    time.Minute,
			Exact:         exact,
		}
		switch chosen.res {
		case resHour:
			meta.Resolution = time.Hour
		case resDay:
			meta.Resolution = 24 * time.Hour
		}
		if !exact {
			if meta.CoveredFrom.Before(res.EffectiveFrom) {
				res.EffectiveFrom = meta.CoveredFrom
			}
			if meta.CoveredTo.After(res.EffectiveTo) {
				res.EffectiveTo = meta.CoveredTo
			}
		}
		if err := onSlot(chosen, meta); err != nil {
			return err
		}
		cursor = segmentTo
	}
	if cursor < to && !res.sparse {
		if opts.RequireExact {
			return ErrResolutionTooCoarse
		}
		res.Exact = false
	}
	return nil
}

func planQuerySlots(res *queryResult, from, to int64, sources querySources, s *Series, opts queryOptions, candidates *blockBitmap) ([]plannedSlot, error) {
	est := 0
	for _, src := range [...]blockSource{sources.fine, sources.hours, sources.days} {
		if src.candIdx != nil {
			est += len(src.candIdx)
		} else {
			est += len(src.idx)
		}
	}
	if candidates != nil {
		est = min(est, candidates.count)
	}
	est = max(16, min(est+len(sources.fine.live), 256))
	plans := make([]plannedSlot, 0, est)
	if err := walkQuerySlots(res, from, to, sources, s, opts, func(chosen sealedMeta, meta SegmentMeta) error {
		plans = append(plans, plannedSlot{block: chosen, meta: meta})
		return nil
	}); err != nil {
		return nil, err
	}
	return plans, nil
}
