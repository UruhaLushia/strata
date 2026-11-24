package strata

import "sort"

func sealedRangeIndexes(blocks []sealedMeta, indexes []uint32, from, to int64) []uint32 {
	if len(indexes) == 0 {
		return nil
	}
	start := sort.Search(len(indexes), func(i int) bool {
		return blocks[indexes[i]].start >= from
	})
	for start > 0 && blocks[indexes[start-1]].end() > from {
		start--
	}
	end := start + sort.Search(len(indexes)-start, func(i int) bool {
		return blocks[indexes[start+i]].start >= to
	})
	return indexes[start:end]
}

func blockExcludedRules(rules []resetRule, start, end int64, epoch uint64) bool {
	for _, r := range rules {
		if epoch <= r.oldEpoch && r.to > start && r.from < end {
			return true
		}
	}
	return false
}

func (src *blockSource) excluded(b *sealedMeta) bool {
	return src.filterFine && len(src.rules) > 0 && blockExcludedRules(src.rules, b.start, b.end(), b.epoch)
}

func (src *blockSource) isCandidate(pos uint32) bool {
	if src.candBits == nil {
		return true
	}
	w := pos >> 6
	return int(w) < len(src.candBits) && (src.candBits[w]&(uint64(1)<<(pos&63))) != 0
}

func (src *blockSource) eachSlotCandidate(start, end int64, res byte, rules []resetRule, onSealed func(sealedMeta), onLive func(*sealedBlock)) {
	list := src.idx
	if src.candIdx != nil {
		list = src.candIdx
	}
	sLo := sort.Search(len(list), func(i int) bool {
		return src.sealed[list[i]].start >= start
	})
	sHi := sLo
	for sHi < len(list) && src.sealed[list[sHi]].start == start {
		sHi++
	}
	lLo, lHi := 0, 0
	if len(src.live) > 0 {
		lLo = sort.Search(len(src.live), func(i int) bool {
			return src.live[i].start >= start
		})
		lHi = lLo
		for lHi < len(src.live) && src.live[lHi].start == start {
			lHi++
		}
	}
	nextSealed := func(i int) int {
		for i < sHi {
			pos := list[i]
			b := &src.sealed[pos]
			if b.end() == end && b.res == res && (src.candIdx != nil || src.isCandidate(pos)) && !blockExcludedRules(rules, b.start, b.end(), b.epoch) {
				return i
			}
			i++
		}
		return sHi
	}
	nextLive := func(i int) int {
		for i < lHi {
			b := &src.live[i]
			if b.candidate && b.end == end && b.res == res && !blockExcludedRules(rules, b.start, b.end, b.epoch) {
				return i
			}
			i++
		}
		return lHi
	}
	si, li := nextSealed(sLo), nextLive(lLo)
	for si < sHi || li < lHi {
		if li >= lHi {
			onSealed(src.sealed[list[si]])
			si = nextSealed(si + 1)
			continue
		}
		if si >= sHi {
			onLive(&src.live[li])
			li = nextLive(li + 1)
			continue
		}
		sb, lb := src.sealed[list[si]], &src.live[li]
		if lb.epoch < sb.epoch {
			onLive(lb)
			li = nextLive(li + 1)
		} else {
			onSealed(sb)
			si = nextSealed(si + 1)
		}
	}
}

func (src *blockSource) countSlotCandidates(start, end int64, res byte, rules []resetRule) int {
	n := 0
	src.eachSlotCandidate(start, end, res, rules, func(sealedMeta) { n++ }, func(*sealedBlock) { n++ })
	return n
}

func (s *querySources) countSlotCandidates(m sealedMeta, rules []resetRule) int {
	switch m.res {
	case resMinuteHR:
		return s.fine.countSlotCandidates(m.start, m.end(), m.res, rules)
	case resHour:
		return s.hours.countSlotCandidates(m.start, m.end(), m.res, rules)
	case resDay:
		return s.days.countSlotCandidates(m.start, m.end(), m.res, rules)
	default:
		return 0
	}
}

func slotCandidateRowBounds(plan plannedSlot, sources querySources, s *Series) (sumRows, minProduced int, ok bool) {
	m := plan.block
	var src *blockSource
	switch m.res {
	case resMinuteHR:
		src = &sources.fine
	case resHour:
		src = &sources.hours
	case resDay:
		src = &sources.days
	default:
		return 0, 0, false
	}
	src.eachSlotCandidate(m.start, m.end(), m.res, s.rules, func(cand sealedMeta) {
		n := int(cand.rowCount)
		sumRows += n
		minProduced = max(minProduced, n)
	}, func(lb *sealedBlock) {
		n := max(len(lb.rows), int(lb.rowCount))
		sumRows += n
		minProduced = max(minProduced, n)
	})
	return sumRows, minProduced, true
}

// estimateCandidateRowUpperBound returns a conservative upper bound on the total
// rows across all sealed and live candidates that mergeSlot will visit for plans.
// It returns (0, false) if any slot has an invalid resolution or the bound exceeds
// maxBound.
func estimateCandidateRowUpperBound(plans []plannedSlot, sources querySources, s *Series) (int, bool) {
	const maxBound = 1 << 28
	total := 0
	for _, plan := range plans {
		rows, _, ok := slotCandidateRowBounds(plan, sources, s)
		if !ok {
			return 0, false
		}
		total += rows
		if total > maxBound {
			return 0, false
		}
	}
	return total, true
}

func maxSinglePassGroups(policy memPolicy, ops [4]MetricOp) int {
	entryBytes := int64(40) + orderEntryBytes(ops)
	// Conservative 4x factor accounts for 70% load factor plus grow() transient coexistence.
	budget := max(policy.queryBytes/4, 256<<10)
	limit := int(budget / (entryBytes * 4))
	if policy.constrained {
		return min(limit, 4096)
	}
	return min(limit, 8192)
}

func shouldPartitionTopK(d dictionary, series uint16, groupDims []int, arity, topK int) bool {
	if len(groupDims) != 1 || topK <= 0 {
		return false
	}
	dim := groupDims[0]
	return dim >= 0 && dim < arity && d.dimMaxID(series, dim) > highCardThreshold
}

func canUseSinglePassHighCard(plans []plannedSlot, sources querySources, s *Series, policy memPolicy) bool {
	upper, ok := estimateCandidateRowUpperBound(plans, sources, s)
	if !ok {
		return false
	}
	return upper <= maxSinglePassGroups(policy, s.cfg.RowOps)
}

func metaBefore(left, right sealedMeta) bool {
	if left.series != right.series {
		return left.series < right.series
	}
	if left.start != right.start {
		return left.start < right.start
	}
	if left.res != right.res {
		return left.res < right.res
	}
	return left.epoch < right.epoch
}

func sparseFinerGapCursor(res byte, cursor int64, fine, hours *blockCursor) bool {
	if res == resHour {
		if cursor%3600 == 0 {
			return false
		}
		_, candidate := fine.candidateAt(cursor)
		return !candidate
	}
	if res == resDay {
		if cursor%86400 == 0 {
			return false
		}
		fineBlock, fineCandidate := fine.candidateAt(cursor)
		hourBlock, hourCandidate := hours.candidateAt(cursor)
		return (fineBlock || hourBlock) && !fineCandidate && !hourCandidate
	}
	return false
}

func coarseCutByRuleCursor(cursor int64, s *Series, hours, days *blockCursor) bool {
	for _, c := range [2]*blockCursor{hours, days} {
		list, startIdx, preFiltered := c.candList(cursor)
		for i := startIdx; i < len(list); i++ {
			pos := list[i]
			b := &c.src.sealed[pos]
			if b.start > cursor {
				break
			}
			if (preFiltered || c.src.isCandidate(pos)) && b.end() > cursor && metaCutByRule(*b, s) {
				return true
			}
		}
	}
	return false
}

func minCursorNext(left, right int64) int64 {
	if left == 0 {
		return right
	}
	if right == 0 || left < right {
		return left
	}
	return right
}
