package strata

import (
	"errors"
	"time"
)

func aggregateSummaryQueryPlan(res *queryResult, from, to int64, sources querySources, s *Series, opts queryOptions, out *AggregateResult) error {
	estSegs := len(sources.days.idx) + min(len(sources.hours.idx), 256) + len(sources.fine.live)
	if estSegs > 0 {
		out.Segments = getSegmentSummarySlice(estSegs)
	}
	cursor := from
	var resultOrder uint64
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
			if b, _, ok := fineCursor.covering(cursor); ok {
				chosen, hasChosen = b, true
			}
		}
		if !hasChosen && !opts.PreferFine {
			if b, ok := hourCursor.coveringCoarse(cursor, s); ok {
				chosen, hasChosen = b, true
			} else if b, ok := dayCursor.coveringCoarse(cursor, s); ok {
				chosen, hasChosen = b, true
			}
		} else if hasChosen && chosen.res == resMinuteHR && !opts.PreferFine {
			if b, ok := hourCursor.coveringCoarse(cursor, s); ok && !fineCursor.candidateCoversUntil(cursor, b.end()) {
				chosen = b
			} else if b, ok := dayCursor.coveringCoarse(cursor, s); ok && !fineCursor.candidateCoversUntil(cursor, b.end()) {
				chosen = b
			}
		}
		if !hasChosen {
			if coarseCutByRuleCursor(cursor, s, &hourCursor, &dayCursor) {
				return ErrResetRequiresFineData
			}
			next := fineCursor.nextCandidate(cursor)
			if next == cursor {
				next = minCursorNext(hourCursor.nextCandidate(cursor), dayCursor.nextCandidate(cursor))
			}
			if next > cursor {
				if opts.RequireExact {
					return ErrResolutionTooCoarse
				}
				res.Exact = false
				if next > to {
					next = to
				}
				cursor = next
				continue
			}
			if opts.RequireExact {
				return ErrResolutionTooCoarse
			}
			res.Exact = false
			break
		}
		mergedSum, ok := summaryMetaSlot(chosen, sources, s)
		if !ok {
			return errors.New("strata: selected block disappeared")
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
		out.Segments = append(out.Segments, SegmentSummary{Meta: meta, Summary: mergedSum})
		resultOrder++
		mergeSummary(&res.Summary, mergedSum, s.cfg.SummaryOps, resultOrder, resultOrder-1)
		cursor = segmentTo
	}
	if cursor < to {
		if opts.RequireExact {
			return ErrResolutionTooCoarse
		}
		res.Exact = false
	}
	return nil
}

func summaryMetaSlot(chosen sealedMeta, sources querySources, s *Series) (Summary, bool) {
	var source *blockSource
	switch chosen.res {
	case resMinuteHR:
		source = &sources.fine
	case resHour:
		source = &sources.hours
	case resDay:
		source = &sources.days
	default:
		return Summary{}, false
	}
	var merged Summary
	var mergeOrder uint64
	found := false
	addSum := func(sum Summary) {
		if !found {
			merged = sum
			mergeOrder = 1
			found = true
			return
		}
		mergeOrder++
		mergeSummary(&merged, sum, s.cfg.SummaryOps, mergeOrder, mergeOrder-1)
	}
	source.eachSlotCandidate(chosen.start, chosen.end(), chosen.res, s.rules, func(meta sealedMeta) {
		addSum(meta.summary())
	}, func(live *sealedBlock) {
		addSum(live.summary)
	})
	return merged, found
}
