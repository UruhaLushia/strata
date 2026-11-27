package strata

import (
	"encoding/binary"
	"math/bits"
	"slices"
)

type selMode uint8

const (
	selRange selMode = iota
	selIdx
	selBits
)

type rowSelection struct {
	mode     selMode
	lo, hi   int
	cnt      int
	idx      []int32
	idxPool  *[]int32
	bits     []uint64
	bitsPool *[]uint64
}

func (s rowSelection) count() int {
	switch s.mode {
	case selRange:
		return s.hi - s.lo
	case selIdx:
		return len(s.idx)
	default:
		return s.cnt
	}
}

func (s *rowSelection) release() {
	if s.idxPool != nil {
		putMatchIdx(s.idxPool)
		s.idxPool = nil
		s.idx = nil
	}
	if s.bitsPool != nil {
		putRowBits(s.bitsPool)
		s.bitsPool = nil
		s.bits = nil
	}
	s.mode = selRange
	s.lo, s.hi, s.cnt = 0, 0, 0
}

func (s rowSelection) sliceRows(start, n int, scratch []int32) (bool, int, []int32) {
	if n <= 0 {
		return true, 0, nil
	}
	switch s.mode {
	case selRange:
		return true, s.lo + start, nil
	case selIdx:
		return false, 0, s.idx[start : start+n]
	default:
		out := scratch[:0]
		skip := start
		for wi, w := range s.bits {
			if w == 0 {
				continue
			}
			if skip > 0 {
				ones := bits.OnesCount64(w)
				if skip >= ones {
					skip -= ones
					continue
				}
				for skip > 0 {
					w &= w - 1
					skip--
				}
			}
			base := wi << 6
			for w != 0 {
				tz := bits.TrailingZeros64(w)
				out = append(out, int32(base+tz))
				if len(out) == n {
					return false, 0, out
				}
				w &= w - 1
			}
		}
		return false, 0, out
	}
}

func matchesFilterID(id uint32, f compiledFilter) bool {
	matched := false
	switch {
	case len(f.ids) == 0:
	case len(f.ids) == 1:
		matched = id == f.single
	case f.smallLen > 0:
		if slices.Contains(f.small[:f.smallLen], id) {
			matched = true
		}
	case id/64 < uint32(len(f.bits)):
		matched = f.bits[id/64]&(uint64(1)<<(id&63)) != 0
	}
	if f.negate {
		return !matched
	}
	return matched
}

func binarySearchTagCol(col decodedCol, count int, target uint32) (int, int) {
	lo, hi := 0, count
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if col.tagAt(mid) < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo >= count || col.tagAt(lo) != target {
		return 0, 0
	}
	endLo, endHi := lo+1, count
	for endLo < endHi {
		mid := int(uint(endLo+endHi) >> 1)
		if col.tagAt(mid) <= target {
			endLo = mid + 1
		} else {
			endHi = mid
		}
	}
	return lo, endLo
}

func (s *rowSelection) collectWordIdx(w uint64, wi, count, span, bitLimit int) {
	ones := bits.OnesCount64(w)
	if s.cnt+ones > bitLimit {
		s.bitsPool = getRowBits((count + 63) >> 6)
		s.bits = *s.bitsPool
		for _, r := range s.idx {
			s.bits[r>>6] |= uint64(1) << (r & 63)
		}
		*s.idxPool = s.idx
		putMatchIdx(s.idxPool)
		s.idxPool, s.idx, s.mode = nil, nil, selBits
		s.bits[wi] = w
	} else {
		if s.idxPool == nil {
			s.mode = selIdx
			s.idxPool = getMatchIdx(min(span, 1024))
			s.idx = *s.idxPool
		}
		for ; w != 0; w &= w - 1 {
			s.idx = append(s.idx, int32(wi<<6|bits.TrailingZeros64(w)))
		}
	}
	s.cnt += ones
}

func scanSmallRangeIdx(col decodedCol, lo, hi int, f compiledFilter, singleEq bool) rowSelection {
	span := hi - lo
	var pool *[]int32
	var idx []int32
	if singleEq {
		target := f.single
		switch col.mode {
		case modeU8:
			data := col.data[lo:hi]
			for i, v := range data {
				if uint32(v) != target {
					continue
				}
				if pool == nil {
					pool = getMatchIdx(span)
					idx = *pool
				}
				idx = append(idx, int32(lo+i))
			}
		case modeU16:
			data := col.data[2*lo : 2*hi]
			for r := lo; len(data) >= 2; r, data = r+1, data[2:] {
				if uint32(binary.LittleEndian.Uint16(data)) != target {
					continue
				}
				if pool == nil {
					pool = getMatchIdx(span)
					idx = *pool
				}
				idx = append(idx, int32(r))
			}
		case modeU32:
			data := col.data[4*lo : 4*hi]
			for r := lo; len(data) >= 4; r, data = r+1, data[4:] {
				if binary.LittleEndian.Uint32(data) != target {
					continue
				}
				if pool == nil {
					pool = getMatchIdx(span)
					idx = *pool
				}
				idx = append(idx, int32(r))
			}
		}
	} else {
		for r := lo; r < hi; r++ {
			if !matchesFilterID(col.tagAt(r), f) {
				continue
			}
			if pool == nil {
				pool = getMatchIdx(span)
				idx = *pool
			}
			idx = append(idx, int32(r))
		}
	}
	if pool == nil {
		return rowSelection{}
	}
	*pool = idx
	if len(idx) == span {
		putMatchIdx(pool)
		return rowSelection{mode: selRange, lo: lo, hi: hi, cnt: span}
	}
	return rowSelection{mode: selIdx, cnt: len(idx), idx: idx, idxPool: pool}
}

func scanRangeAdaptive(col decodedCol, count, lo, hi int, f compiledFilter) rowSelection {
	span := hi - lo
	if span <= 0 {
		return rowSelection{}
	}
	singleEq := !f.negate && len(f.ids) == 1
	target := f.single
	if singleEq {
		if col.mode == modeConst {
			if uint32(col.val) == target {
				return rowSelection{mode: selRange, lo: lo, hi: hi, cnt: span}
			}
			return rowSelection{}
		}
		if col.mode == modeU8 && target > 0xff || col.mode == modeU16 && target > 0xffff {
			return rowSelection{}
		}
	}
	if span <= 256 {
		return scanSmallRangeIdx(col, lo, hi, f, singleEq)
	}
	bitLimit := span / 4
	var sel rowSelection
	if singleEq {
		data := col.data
		switch col.mode {
		case modeU8:
			for wi, r := lo>>6, lo; r < hi; wi++ {
				var w uint64
				end := min(hi, (wi+1)<<6)
				for _, v := range data[r:end] {
					if uint32(v) == target {
						w |= uint64(1) << (r & 63)
					}
					r++
				}
				if w != 0 {
					if sel.mode == selBits {
						sel.bits[wi] = w
						sel.cnt += bits.OnesCount64(w)
					} else {
						sel.collectWordIdx(w, wi, count, span, bitLimit)
					}
				}
			}
		case modeU16:
			for wi, r := lo>>6, lo; r < hi; wi++ {
				var w uint64
				end := min(hi, (wi+1)<<6)
				for word := data[2*r : 2*end]; len(word) >= 2; r, word = r+1, word[2:] {
					if uint32(binary.LittleEndian.Uint16(word)) == target {
						w |= uint64(1) << (r & 63)
					}
				}
				if w != 0 {
					if sel.mode == selBits {
						sel.bits[wi] = w
						sel.cnt += bits.OnesCount64(w)
					} else {
						sel.collectWordIdx(w, wi, count, span, bitLimit)
					}
				}
			}
		case modeU32:
			for wi, r := lo>>6, lo; r < hi; wi++ {
				var w uint64
				end := min(hi, (wi+1)<<6)
				for word := data[4*r : 4*end]; len(word) >= 4; r, word = r+1, word[4:] {
					if binary.LittleEndian.Uint32(word) == target {
						w |= uint64(1) << (r & 63)
					}
				}
				if w != 0 {
					if sel.mode == selBits {
						sel.bits[wi] = w
						sel.cnt += bits.OnesCount64(w)
					} else {
						sel.collectWordIdx(w, wi, count, span, bitLimit)
					}
				}
			}
		}
	} else {
		for wi, r := lo>>6, lo; r < hi; wi++ {
			var w uint64
			for end := min(hi, (wi+1)<<6); r < end; r++ {
				if matchesFilterID(col.tagAt(r), f) {
					w |= uint64(1) << (r & 63)
				}
			}
			if w != 0 {
				if sel.mode == selBits {
					sel.bits[wi] = w
					sel.cnt += bits.OnesCount64(w)
				} else {
					sel.collectWordIdx(w, wi, count, span, bitLimit)
				}
			}
		}
	}
	if sel.idxPool != nil {
		*sel.idxPool = sel.idx
	}
	if sel.cnt == span {
		sel.release()
		return rowSelection{mode: selRange, lo: lo, hi: hi, cnt: span}
	}
	return sel
}

func filterCols(count int, filters []compiledFilter, allowDim0BS bool, loadCol func(int) (decodedCol, error), modeOf func(int) (byte, uint64)) (rowSelection, error) {
	if count == 0 {
		return rowSelection{}, nil
	}
	if len(filters) == 0 {
		return rowSelection{mode: selRange, lo: 0, hi: count, cnt: count}, nil
	}
	var doneStack [16]bool
	var done []bool
	if len(filters) <= len(doneStack) {
		done = doneStack[:len(filters)]
	} else {
		done = make([]bool, len(filters))
	}
	dim0SingleIdx := -1
	for i, f := range filters {
		if f.dim < 0 || f.dim >= 8 {
			if !f.negate {
				return rowSelection{}, nil
			}
			done[i] = true
			continue
		}
		if !f.negate && len(f.ids) == 0 {
			return rowSelection{}, nil
		}
		mode, val := modeOf(f.dim)
		if mode == modeConst {
			if !matchesFilterID(uint32(val), f) {
				return rowSelection{}, nil
			}
			done[i] = true
			continue
		}
		if !f.negate && len(f.ids) == 1 {
			if (mode == modeU8 && f.single > 0xff) || (mode == modeU16 && f.single > 0xffff) {
				return rowSelection{}, nil
			}
			if allowDim0BS && f.dim == 0 && dim0SingleIdx < 0 {
				dim0SingleIdx = i
			}
		}
	}
	sel := rowSelection{mode: selRange, lo: 0, hi: count, cnt: count}
	lastDim := -1
	var lastCol decodedCol
	if dim0SingleIdx >= 0 {
		col, err := loadCol(0)
		if err != nil {
			return rowSelection{}, err
		}
		lastDim, lastCol = 0, col
		lo, hi := binarySearchTagCol(col, count, filters[dim0SingleIdx].single)
		if lo >= hi {
			return rowSelection{}, nil
		}
		sel = rowSelection{mode: selRange, lo: lo, hi: hi, cnt: hi - lo}
		done[dim0SingleIdx] = true
	}
	for i, f := range filters {
		if done[i] {
			continue
		}
		col := lastCol
		if f.dim != lastDim {
			var err error
			col, err = loadCol(f.dim)
			if err != nil {
				sel.release()
				return rowSelection{}, err
			}
			lastDim, lastCol = f.dim, col
		}
		switch sel.mode {
		case selRange:
			sel = scanRangeAdaptive(col, count, sel.lo, sel.hi, f)
			if sel.count() == 0 {
				return rowSelection{}, nil
			}
		case selIdx:
			out := sel.idx[:0]
			for _, r := range sel.idx {
				if matchesFilterID(col.tagAt(int(r)), f) {
					out = append(out, r)
				}
			}
			if len(out) == 0 {
				sel.release()
				return rowSelection{}, nil
			}
			sel.idx = out
			sel.cnt = len(out)
			*sel.idxPool = out
		case selBits:
			cnt := 0
			for wi, w := range sel.bits {
				if w == 0 {
					continue
				}
				base := wi << 6
				nw := w
				for m := w; m != 0; m &= m - 1 {
					tz := bits.TrailingZeros64(m)
					if !matchesFilterID(col.tagAt(base+tz), f) {
						nw &^= uint64(1) << tz
					}
				}
				sel.bits[wi] = nw
				cnt += bits.OnesCount64(nw)
			}
			if cnt == 0 {
				sel.release()
				return rowSelection{}, nil
			}
			sel.cnt = cnt
		}
	}
	return sel, nil
}
