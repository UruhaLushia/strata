package strata

import "math/bits"

type rowSelCursor struct {
	sel     rowSelection
	pos     int
	wordIdx int
	remWord uint64
}

// next borrows indexed rows from the selection and writes bitmap rows only to scratch.
func (c *rowSelCursor) next(n int, scratch *[]int32) (bool, int, []int32) {
	if n <= 0 {
		return true, 0, nil
	}
	pos := c.pos
	c.pos += n
	var rows []int32
	switch c.sel.mode {
	case selRange:
		return true, c.sel.lo + pos, nil
	case selIdx:
		rows = c.sel.idx[pos:c.pos]
	case selBits:
		if cap(*scratch) < n {
			*scratch = make([]int32, n)
		}
		rows = (*scratch)[:n]
		wordIdx, word := c.wordIdx, c.remWord
		for i := range rows {
			for word == 0 {
				word = c.sel.bits[wordIdx]
				wordIdx++
			}
			rows[i] = int32((wordIdx-1)<<6 + bits.TrailingZeros64(word))
			word &= word - 1
		}
		c.wordIdx, c.remWord = wordIdx, word
	}
	if rows[n-1]-rows[0] == int32(n-1) {
		return true, int(rows[0]), nil
	}
	return false, 0, rows
}

func extractScanConstTags(filters []compiledFilter, arity int) ([8]uint32, uint8) {
	var tags [8]uint32
	var mask uint8
	for _, f := range filters {
		if !f.negate && len(f.ids) == 1 && f.dim >= 0 && f.dim < arity {
			tags[f.dim] = f.single
			mask |= uint8(1) << f.dim
		}
	}
	return tags, mask
}

func setScanConstCols(cols *[streamColumnCount]decodedCol, tags [8]uint32, mask uint8) {
	for mask != 0 {
		dim := bits.TrailingZeros8(mask)
		cols[dim] = decodedCol{mode: modeConst, val: uint64(tags[dim])}
		mask &= mask - 1
	}
}
