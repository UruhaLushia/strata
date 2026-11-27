package strata

import "errors"

func aggregateSummaryQueryPlan(res *queryResult, from, to int64, sources querySources, s *Series, opts queryOptions, out *AggregateResult) error {
	estSegs := len(sources.days.idx) + min(len(sources.hours.idx), 256) + len(sources.fine.live)
	if estSegs > 0 {
		out.Segments = getSegmentSummarySlice(estSegs)
	}
	var resultOrder uint64
	return walkQuerySlots(res, from, to, sources, s, opts, func(chosen sealedMeta, meta SegmentMeta) error {
		mergedSum, ok := summaryMetaSlot(chosen, sources, s)
		if !ok {
			return errors.New("strata: selected block disappeared")
		}
		out.Segments = append(out.Segments, SegmentSummary{Meta: meta, Summary: mergedSum})
		resultOrder++
		mergeSummary(&res.Summary, mergedSum, s.cfg.SummaryOps, resultOrder, resultOrder-1)
		return nil
	})
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
