package strata

import "time"

type plannedSlot struct {
	block   sealedMeta
	liveIdx int
	meta    SegmentMeta
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

type preparedQuery struct {
	from, to   int64
	execDB     *DB
	execSeries Series
	sources    querySources
	plans      []plannedSlot
	compiled   []compiledFilter
	qres       queryResult
	view       *fileView
	empty      bool
}

func (p *preparedQuery) release() {
	if p.view != nil {
		p.view.release()
		p.view = nil
	}
}

func prepareQuery(db *DB, s *Series, ws windowSpec, filters []Filter, opts queryOptions, slot *querySlot) (preparedQuery, error) {
	var pq preparedQuery
	snap, planFrom, empty, err := captureQuerySnapshot(db, s, ws, filters)
	if err != nil {
		return pq, err
	}
	pq.from, pq.to = snap.from, snap.to
	if empty {
		pq.empty = true
		return pq, nil
	}
	releaseSnap := true
	defer func() {
		if releaseSnap {
			snap.release()
		}
	}()

	pq.qres = queryResult{
		RequestedFrom: time.Unix(planFrom, 0).UTC(), RequestedTo: time.Unix(pq.to, 0).UTC(),
		EffectiveFrom: time.Unix(planFrom, 0).UTC(), EffectiveTo: time.Unix(pq.to, 0).UTC(),
		Exact: true,
	}

	var sealedBits *blockBitmap
	if len(filters) > 0 {
		compiled := compileFiltersSnap(snap.dict, snap.series.id, snap.series.cfg.Arity, filters)
		ordered, orderedCompiled := orderCompiledFilters(filters, compiled)
		for i, f := range ordered {
			if !f.Negate && len(orderedCompiled[i].ids) == 0 {
				pq.empty = true
				return pq, nil
			}
		}
		hasLiveCandidate := filterLiveCandidates(snap.liveFine, orderedCompiled)
		var emptyCand bool
		sealedBits, emptyCand, err = searchCandidatesSnap(&snap, ordered, orderedCompiled, hasLiveCandidate, slot)
		if err != nil {
			return preparedQuery{}, err
		}
		if emptyCand {
			pq.empty = true
			return pq, nil
		}
		pq.compiled = orderedCompiled
		pq.qres.sparse = true
	}

	pq.sources = buildQuerySourcesFromSnap(&snap, planFrom, pq.to, sealedBits)
	pq.execSeries = snap.series
	pq.execDB = snap.buildExecDB()

	plans, err := planQuerySlots(&pq.qres, planFrom, pq.to, pq.sources, &pq.execSeries, opts)
	if err != nil {
		return preparedQuery{}, err
	}
	pq.plans = plans
	pq.view = snap.view
	releaseSnap = false
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

func planQuerySlots(res *queryResult, from, to int64, sources querySources, s *Series, opts queryOptions) ([]plannedSlot, error) {
	cursor := from
	est := max(len(sources.days.idx)+min(len(sources.hours.idx), 256)+len(sources.fine.live), 16)
	plans := make([]plannedSlot, 0, est)
	fineCursor := blockCursor{src: sources.fine}
	hourCursor := blockCursor{src: sources.hours}
	dayCursor := blockCursor{src: sources.days}
	for cursor < to {
		var chosen sealedMeta
		hasChosen := false
		liveIdx := -1
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
			if b, li, ok := fineCursor.covering(cursor); ok {
				chosen, liveIdx, hasChosen = b, li, true
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
				chosen, liveIdx = b, -1
			} else if b, ok := dayCursor.coveringCoarse(cursor, s); ok && !fineCursor.candidateCoversUntil(cursor, b.end()) {
				chosen, liveIdx = b, -1
			}
		}
		if res.sparse && hasChosen && chosen.res != resMinuteHR && sparseFinerGapCursor(chosen.res, cursor, &fineCursor, &hourCursor) {
			hasChosen = false
		}
		if !hasChosen {
			if coarseCutByRuleCursor(cursor, s, &hourCursor, &dayCursor) {
				return nil, ErrResetRequiresFineData
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
					return nil, ErrResolutionTooCoarse
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
				return nil, ErrResolutionTooCoarse
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
				return nil, ErrResolutionTooCoarse
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
		plans = append(plans, plannedSlot{block: chosen, liveIdx: liveIdx, meta: meta})
		cursor = segmentTo
	}
	if cursor < to && !res.sparse {
		if opts.RequireExact {
			return nil, ErrResolutionTooCoarse
		}
		res.Exact = false
	}
	return plans, nil
}
