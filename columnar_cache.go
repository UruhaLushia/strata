package strata

import "errors"

// blockColCache caches decompressed STR2 columns for a single block within slot.colArena.
// For filtered queries, output arena allocation is deferred until after filter() succeeds,
// avoiding large arena allocations on zero-hit or sparse blocks.
type blockColCache struct {
	view        columnarBlockView
	slot        *querySlot
	arena       []byte
	arenaUsed   int
	arenaCap    int
	arenaBudget int
	cols        [streamColumnCount]decodedCol
	cached      [streamColumnCount]bool
	cachedOffs  [streamColumnCount]int
	fullBlock   bool
}

func filterColMask(filters []compiledFilter) uint16 {
	var m uint16
	for _, f := range filters {
		if f.dim >= 0 && f.dim < 8 {
			m |= uint16(1) << f.dim
		}
	}
	return m
}

func newBlockColCache(view columnarBlockView, outputMask, filterMask uint16, arenaBudget int, slot *querySlot) blockColCache {
	if arenaBudget < 64<<10 {
		arenaBudget = 64 << 10
	}
	c := blockColCache{
		view:        view,
		slot:        slot,
		arenaBudget: arenaBudget,
	}
	for i := range c.cachedOffs {
		c.cachedOffs[i] = -1
	}
	if filterMask == 0 {
		fullBytes := view.count * streamRowBytes(view, outputMask)
		if fullBytes <= arenaBudget {
			if fullBytes > 0 {
				c.arena = slot.ensureArena(fullBytes)
			}
			c.arenaCap = fullBytes
			c.fullBlock = true
		} else {
			c.arena = slot.ensureArena(arenaBudget)
			c.arenaCap = arenaBudget / 2
		}
		return c
	}
	filterBytes := view.count * streamRowBytes(view, filterMask)
	filterCap := min(filterBytes, arenaBudget/2)
	if filterCap > 0 {
		c.arena = slot.ensureArena(filterCap)
	}
	c.arenaCap = filterCap
	return c
}

func (c *blockColCache) growArena(newSize int) {
	if newSize <= len(c.arena) {
		return
	}
	if c.arena == nil {
		c.arena = c.slot.ensureArena(newSize)
		return
	}
	if newSize <= cap(c.arena) {
		c.arena = c.arena[:newSize]
		return
	}
	used := c.arenaUsed
	next := make([]byte, newSize)
	if used > 0 {
		copy(next, c.arena[:used])
	}
	if newSize <= maxActiveArenaCap {
		c.slot.colArena = next
	}
	c.arena = next
	for idx := range streamColumnCount {
		if off := c.cachedOffs[idx]; off >= 0 {
			n := len(c.cols[idx].data)
			c.cols[idx].data = next[off : off+n]
		}
	}
}

func (c *blockColCache) uncachedRowBytes(colMask uint16) int {
	total := 0
	for idx := range streamColumnCount {
		if colMask&(uint16(1)<<idx) == 0 || c.cached[idx] {
			continue
		}
		d := c.view.descs[idx]
		if d.mode != modeConst && d.mode != modeZero {
			total += int(d.width)
		}
	}
	return total
}

func (c *blockColCache) prepareOutput(outputMask uint16, total int, isRange bool) {
	if c.fullBlock {
		return
	}
	fullNeed := c.arenaUsed + c.view.count*c.uncachedRowBytes(outputMask)
	if fullNeed <= c.arenaBudget && (fullNeed <= maxRetainedArenaCap || total*4 >= c.view.count) {
		c.growArena(fullNeed)
		c.arenaCap = fullNeed
		c.fullBlock = true
		return
	}
	rowBytes := c.packRowBytes(outputMask, isRange)
	if rowBytes == 0 || total <= 0 {
		return
	}
	packBytes := total * rowBytes
	minPack := min(total, 256) * rowBytes
	remBudget := max(c.arenaBudget-c.arenaUsed, minPack)
	need := c.arenaUsed + min(packBytes, remBudget)
	if need > len(c.arena) {
		c.growArena(need)
	}
}

func (c *blockColCache) load(idx int) (decodedCol, error) {
	if c.cached[idx] {
		return c.cols[idx], nil
	}
	d := c.view.descs[idx]
	if d.mode == modeConst || d.mode == modeZero {
		col := decodedCol{mode: d.mode, val: d.value, loaded: true}
		c.cols[idx], c.cached[idx] = col, true
		return col, nil
	}
	if d.width == 0 {
		return decodedCol{}, errors.New("strata: invalid column stream width")
	}
	start, end := int(d.offset), int(d.offset+d.length)
	if start < 0 || end < start || end > len(c.view.plain) {
		return decodedCol{}, errors.New("strata: invalid column stream bounds")
	}
	expected := c.view.count * int(d.width)
	if int(d.rawLen) != expected {
		return decodedCol{}, errors.New("strata: invalid column stream payload")
	}
	if c.arenaUsed+expected <= c.arenaCap {
		off := c.arenaUsed
		dst := c.arena[off : off : off+expected]
		plain, err := c.slot.decodeZstdInto(c.view.plain[start:end], dst)
		if err != nil {
			return decodedCol{}, err
		}
		if len(plain) != expected {
			return decodedCol{}, errors.New("strata: invalid column stream length")
		}
		c.arenaUsed += expected
		col := decodedCol{mode: d.mode, width: d.width, data: plain, loaded: true}
		c.cols[idx], c.cached[idx], c.cachedOffs[idx] = col, true, off
		return col, nil
	}
	return decodeStreamColInto(c.view, idx, c.slot)
}

func (c *blockColCache) filter(filters []compiledFilter, allowDim0BS bool) (rowSelection, error) {
	return filterCols(c.view.count, filters, allowDim0BS && !c.view.unsorted, c.load, func(dim int) (byte, uint64) {
		return c.view.descs[dim].mode, c.view.descs[dim].value
	})
}

func (c *blockColCache) loadAll(colMask uint16) error {
	for idx := range streamColumnCount {
		if colMask&(uint16(1)<<idx) == 0 {
			continue
		}
		if _, err := c.load(idx); err != nil {
			return err
		}
	}
	return nil
}

func (c *blockColCache) packRowBytes(colMask uint16, isRange bool) int {
	total := 0
	for idx := range streamColumnCount {
		if colMask&(uint16(1)<<idx) == 0 {
			continue
		}
		d := c.view.descs[idx]
		if d.mode == modeConst || d.mode == modeZero {
			continue
		}
		if isRange && c.cached[idx] {
			continue
		}
		total += int(d.width)
	}
	return total
}

func (c *blockColCache) availPackBytes() int {
	return max(len(c.arena)-c.arenaUsed, 0)
}

func (c *blockColCache) packChunk(colMask uint16, isRange bool, rangeLo int, rows []int32, n int) ([streamColumnCount]decodedCol, error) {
	var cols [streamColumnCount]decodedCol
	needBytes := n * c.packRowBytes(colMask, isRange)
	var dstArena []byte
	if needBytes > 0 {
		if c.arenaUsed+needBytes <= len(c.arena) {
			dstArena = c.arena[c.arenaUsed : c.arenaUsed+needBytes]
		} else {
			dstArena = make([]byte, needBytes)
		}
	}
	off := 0
	for idx := range streamColumnCount {
		if colMask&(uint16(1)<<idx) == 0 {
			continue
		}
		d := c.view.descs[idx]
		if d.mode == modeConst || d.mode == modeZero {
			cols[idx] = decodedCol{mode: d.mode, val: d.value, loaded: true}
			continue
		}
		w := int(d.width)
		if c.cached[idx] {
			src := c.cols[idx].data
			if isRange {
				cols[idx] = decodedCol{mode: d.mode, width: d.width, data: src[rangeLo*w : (rangeLo+n)*w], loaded: true}
				continue
			}
			dst := dstArena[off : off+n*w]
			off += n * w
			gatherColBytes(dst, src, rows, w)
			cols[idx] = decodedCol{mode: d.mode, width: d.width, data: dst, loaded: true}
			continue
		}
		rawCol, err := decodeStreamColInto(c.view, idx, c.slot)
		if err != nil {
			return cols, err
		}
		dst := dstArena[off : off+n*w]
		off += n * w
		if isRange {
			copy(dst, rawCol.data[rangeLo*w:(rangeLo+n)*w])
		} else {
			gatherColBytes(dst, rawCol.data, rows, w)
		}
		cols[idx] = decodedCol{mode: d.mode, width: d.width, data: dst, loaded: true}
	}
	return cols, nil
}
