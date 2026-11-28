package strata

import "sort"

type blockSource struct {
	sealed     []sealedMeta
	idx        []uint32
	candIdx    []uint32
	live       []sealedBlock
	candBits   []uint64
	candBase   uint32
	candSpan   uint32
	filterFine bool
	rules      []resetRule
}

type blockCursor struct {
	src         blockSource
	si          int
	ci          int
	li          int
	nextCandPos int
}

func advanceSealedPos(sealed []sealedMeta, list []uint32, pos int, cursor int64) int {
	if pos < len(list) && sealed[list[pos]].end() <= cursor {
		if pos+1 < len(list) && sealed[list[pos+1]].end() <= cursor {
			pos += sort.Search(len(list)-pos, func(k int) bool {
				return sealed[list[pos+k]].end() > cursor
			})
		} else {
			pos++
		}
	}
	return pos
}

func (c *blockCursor) advanceIdx(cursor int64) {
	c.si = advanceSealedPos(c.src.sealed, c.src.idx, c.si, cursor)
}

func (c *blockCursor) advanceCand(cursor int64) {
	c.ci = advanceSealedPos(c.src.sealed, c.src.candIdx, c.ci, cursor)
}

func (c *blockCursor) advanceLive(cursor int64) {
	for c.li < len(c.src.live) && c.src.live[c.li].end <= cursor {
		c.li++
	}
}

func (c *blockCursor) candList(cursor int64) ([]uint32, int, bool) {
	if c.src.candIdx != nil {
		c.advanceCand(cursor)
		return c.src.candIdx, c.ci, true
	}
	c.advanceIdx(cursor)
	return c.src.idx, c.si, false
}

func (c *blockCursor) aligned(cursor, to, width int64, s *Series) (sealedMeta, bool) {
	list, startIdx, preFiltered := c.candList(cursor)
	for i := startIdx; i < len(list); i++ {
		pos := list[i]
		b := c.src.sealed[pos]
		if b.start > cursor {
			break
		}
		if (preFiltered || c.src.isCandidate(pos)) && b.start == cursor && b.end() <= to && b.end()-b.start == width && !metaCutByRule(b, s) {
			return b, true
		}
	}
	return sealedMeta{}, false
}

func (c *blockCursor) covering(cursor int64) (sealedMeta, bool) {
	var sb sealedMeta
	hasSealed := false
	list, startIdx, preFiltered := c.candList(cursor)
	for i := startIdx; i < len(list); i++ {
		pos := list[i]
		b := &c.src.sealed[pos]
		if b.start > cursor {
			break
		}
		if b.end() > cursor && (preFiltered || (!c.src.excluded(b) && c.src.isCandidate(pos))) {
			sb = *b
			hasSealed = true
			break
		}
	}
	c.advanceLive(cursor)
	for i := c.li; i < len(c.src.live); i++ {
		b := &c.src.live[i]
		if b.start > cursor {
			break
		}
		if b.candidate && b.end > cursor {
			lb := sealedMeta{
				start: b.start, epoch: b.epoch, order: b.order,
				values: b.summary.Values, rowCount: b.rowCount,
				series: b.series, res: b.res, mask: b.summary.Mask,
			}
			if !hasSealed || metaBefore(lb, sb) {
				return lb, true
			}
			break
		}
	}
	return sb, hasSealed
}

func (c *blockCursor) coveringCoarse(cursor int64, s *Series) (sealedMeta, bool) {
	list, startIdx, preFiltered := c.candList(cursor)
	for i := startIdx; i < len(list); i++ {
		pos := list[i]
		b := c.src.sealed[pos]
		if b.start > cursor {
			break
		}
		if (preFiltered || c.src.isCandidate(pos)) && b.end() > cursor && !metaCutByRule(b, s) {
			return b, true
		}
	}
	return sealedMeta{}, false
}

func (c *blockCursor) partial(cursor int64, fine, hours *blockCursor, s *Series) (sealedMeta, bool) {
	list, startIdx, preFiltered := c.candList(cursor)
	for i := startIdx; i < len(list); i++ {
		pos := list[i]
		b := c.src.sealed[pos]
		if b.start >= cursor {
			break
		}
		if (!preFiltered && !c.src.isCandidate(pos)) || b.end() <= cursor || metaCutByRule(b, s) {
			continue
		}
		if !coversUntilUnion(cursor, b.end(), fine, hours) {
			return b, true
		}
	}
	return sealedMeta{}, false
}

func coversUntilUnion(from, to int64, fine, hours *blockCursor) bool {
	lists := [2]*blockCursor{fine, hours}
	var sIdx, lIdx [2]int
	for k, list := range lists {
		src := &list.src
		sIdx[k] = sort.Search(len(src.idx), func(i int) bool {
			return src.sealed[src.idx[i]].end() > from
		})
		lIdx[k] = sort.Search(len(src.live), func(i int) bool {
			return src.live[i].end > from
		})
	}
	cursor := from
	for cursor < to {
		next := cursor
		for k, list := range lists {
			src := &list.src
			sIdx[k] = advanceSealedPos(src.sealed, src.idx, sIdx[k], cursor)
			for i := sIdx[k]; i < len(src.idx); i++ {
				b := &src.sealed[src.idx[i]]
				if b.start > cursor {
					break
				}
				if !src.excluded(b) && b.end() > next {
					next = b.end()
				}
			}
			for lIdx[k] < len(src.live) && src.live[lIdx[k]].end <= cursor {
				lIdx[k]++
			}
			for i := lIdx[k]; i < len(src.live) && src.live[i].start <= cursor; i++ {
				if src.live[i].end > next {
					next = src.live[i].end
				}
			}
		}
		if next <= cursor {
			return false
		}
		cursor = next
	}
	return true
}

func (c *blockCursor) candidateCoversUntil(from, to int64) bool {
	si, li := c.si, c.li
	src := &c.src
	for cursor := from; cursor < to; cursor += 60 {
		si = advanceSealedPos(src.sealed, src.idx, si, cursor)
		found := false
		for i := si; i < len(src.idx); i++ {
			pos := src.idx[i]
			b := &src.sealed[pos]
			if b.start > cursor {
				break
			}
			if b.end() > cursor && !src.excluded(b) && src.isCandidate(pos) {
				found = true
				break
			}
		}
		if !found {
			for li < len(src.live) && src.live[li].end <= cursor {
				li++
			}
			for i := li; i < len(src.live) && src.live[i].start <= cursor; i++ {
				if src.live[i].candidate && src.live[i].end > cursor {
					found = true
					break
				}
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (c *blockCursor) nextCandidate(cursor int64) int64 {
	var sealedNext int64
	switch {
	case c.src.candIdx != nil:
		c.advanceCand(cursor)
		for i := c.ci; i < len(c.src.candIdx); i++ {
			if s := c.src.sealed[c.src.candIdx[i]].start; s > cursor {
				sealedNext = s
				break
			}
		}
	case c.src.candBits == nil:
		c.advanceIdx(cursor)
		for i := c.si; i < len(c.src.idx); i++ {
			b := &c.src.sealed[c.src.idx[i]]
			if b.start > cursor && !c.src.excluded(b) {
				sealedNext = b.start
				break
			}
		}
	default:
		c.advanceIdx(cursor)
		if c.nextCandPos < c.si {
			c.nextCandPos = c.si
		}
		for ; c.nextCandPos < len(c.src.idx); c.nextCandPos++ {
			pos := c.src.idx[c.nextCandPos]
			b := &c.src.sealed[pos]
			if b.start > cursor && !c.src.excluded(b) && c.src.isCandidate(pos) {
				sealedNext = b.start
				break
			}
		}
	}
	var liveNext int64
	c.advanceLive(cursor)
	for i := c.li; i < len(c.src.live); i++ {
		if c.src.live[i].candidate && c.src.live[i].start > cursor {
			liveNext = c.src.live[i].start
			break
		}
	}
	return minCursorNext(sealedNext, liveNext)
}

func (c *blockCursor) candidateAt(cursor int64) (bool, bool) {
	c.advanceIdx(cursor)
	c.advanceLive(cursor)
	hasBlock, hasCandidate := false, false
	for i := c.si; i < len(c.src.idx); i++ {
		pos := c.src.idx[i]
		b := &c.src.sealed[pos]
		if b.start > cursor {
			break
		}
		if b.end() <= cursor || c.src.excluded(b) {
			continue
		}
		hasBlock = true
		if c.src.isCandidate(pos) {
			hasCandidate = true
		}
	}
	for i := c.li; i < len(c.src.live); i++ {
		b := &c.src.live[i]
		if b.start > cursor {
			break
		}
		if b.end <= cursor {
			continue
		}
		hasBlock = true
		if b.candidate {
			hasCandidate = true
		}
	}
	return hasBlock, hasCandidate
}
