package strata

import (
	"encoding/binary"
	"errors"
)

type decodedCol struct {
	mode  byte
	width byte
	val   uint64
	data  []byte
}

func (c decodedCol) tagAt(row int) uint32 {
	switch c.mode {
	case modeConst:
		return uint32(c.val)
	case modeU8:
		return uint32(c.data[row])
	case modeU16:
		return uint32(binary.LittleEndian.Uint16(c.data[row*2:]))
	default:
		return binary.LittleEndian.Uint32(c.data[row*4:])
	}
}

func (c decodedCol) valAt(row int) int64 {
	switch c.mode {
	case modeConst:
		return int64(c.val)
	case modeZero:
		return 0
	case modeU8:
		return int64(c.data[row])
	case modeU16:
		return int64(binary.LittleEndian.Uint16(c.data[row*2:]))
	case modeU32:
		return int64(binary.LittleEndian.Uint32(c.data[row*4:]))
	default:
		return int64(binary.LittleEndian.Uint64(c.data[row*8:]))
	}
}

func (c decodedCol) maskAt(row int) byte {
	if c.mode == modeConst {
		return byte(c.val)
	}
	return c.data[row]
}

func decodeSmallBlockCols(raw []byte, id uint8, slot *querySlot) (int, [streamColumnCount]decodedCol, error) {
	plain, err := smallBlockPlain(raw, id, slot)
	if err != nil {
		return 0, [streamColumnCount]decodedCol{}, err
	}
	return parseSmallBlockCols(plain)
}

func filterSmallCols(count int, cols *[streamColumnCount]decodedCol, filters []compiledFilter, allowDim0BS bool) (rowSelection, error) {
	return filterCols(count, filters, allowDim0BS, func(dim int) (decodedCol, error) {
		return cols[dim], nil
	}, func(dim int) (byte, uint64) {
		return cols[dim].mode, cols[dim].val
	})
}

func smallBlockPrefix(plain []byte) (int, int, error) {
	if len(plain) < 2+1+8*4+1+32 {
		return 0, 0, errors.New("strata: invalid small column block")
	}
	off := 2 + 1 + 8*4 + 1 + 32
	count, n := binary.Uvarint(plain[off:])
	if n <= 0 {
		return 0, 0, errors.New("strata: invalid column row count")
	}
	off += n
	if count >= 256 || off >= len(plain) {
		return 0, 0, errors.New("strata: invalid column row count")
	}
	if plain[off] != blockColumnarMarker {
		return 0, 0, errors.New("strata: invalid small column block")
	}
	return off + 1, int(count), nil
}

func parseSmallBlockCols(plain []byte) (int, [streamColumnCount]decodedCol, error) {
	var cols [streamColumnCount]decodedCol
	off, count, err := smallBlockPrefix(plain)
	if err != nil {
		return 0, cols, err
	}
	for dim := range 8 {
		if off >= len(plain) {
			return 0, cols, errors.New("strata: truncated column block")
		}
		mode := plain[off]
		off++
		cols[dim] = decodedCol{mode: mode, width: tagModeWidth(mode)}
		if mode == modeConst {
			if off+4 > len(plain) {
				return 0, cols, errors.New("strata: truncated column block")
			}
			cols[dim].val = uint64(binary.LittleEndian.Uint32(plain[off:]))
			off += 4
		}
	}
	for metric := range 4 {
		if off >= len(plain) {
			return 0, cols, errors.New("strata: truncated column block")
		}
		mode := plain[off]
		off++
		cols[8+metric] = decodedCol{mode: mode, width: valueModeWidth(mode)}
		if mode == modeConst {
			if off+8 > len(plain) {
				return 0, cols, errors.New("strata: truncated column block")
			}
			cols[8+metric].val = binary.LittleEndian.Uint64(plain[off:])
			off += 8
		}
	}
	if off >= len(plain) {
		return 0, cols, errors.New("strata: truncated column block")
	}
	maskMode := plain[off]
	off++
	cols[12] = decodedCol{mode: maskMode, width: 1}
	if maskMode == modeConst {
		if off >= len(plain) {
			return 0, cols, errors.New("strata: truncated column block")
		}
		cols[12].val = uint64(plain[off])
		off++
	} else if maskMode != modeU8 {
		return 0, cols, errors.New("strata: invalid mask column mode")
	}
	for dim := range 8 {
		if cols[dim].mode == modeConst {
			continue
		}
		size := count * int(cols[dim].width)
		if cols[dim].width == 0 || off > len(plain)-size {
			return 0, cols, errors.New("strata: invalid tag column")
		}
		cols[dim].data = plain[off : off+size]
		off += size
	}
	for metric := range 4 {
		mode := cols[8+metric].mode
		if mode == modeConst || mode == modeZero {
			continue
		}
		size := count * int(cols[8+metric].width)
		if cols[8+metric].width == 0 || off > len(plain)-size {
			return 0, cols, errors.New("strata: invalid value column")
		}
		cols[8+metric].data = plain[off : off+size]
		off += size
	}
	if cols[12].mode != modeConst {
		if off > len(plain)-count {
			return 0, cols, errors.New("strata: invalid mask column")
		}
		cols[12].data = plain[off : off+count]
		off += count
	}
	if off != len(plain) {
		return 0, cols, errors.New("strata: trailing column block")
	}
	return count, cols, nil
}

func resolvePageCol(view *columnarBlockView, cols *[streamColumnCount]decodedCol, idx int, slot *querySlot) (decodedCol, error) {
	if view != nil {
		return decodeStreamColInto(*view, idx, slot)
	}
	return cols[idx], nil
}

func appendPageFromStream(view columnarBlockView, meta SegmentMeta, d dictionary, series uint16, arity int, isRange bool, rangeLo int, rows []int32, n int, slot *querySlot, page *PageResult) error {
	return appendPage(&view, nil, meta, d, series, arity, isRange, rangeLo, rows, n, slot, page)
}

func appendPageFromCols(cols *[streamColumnCount]decodedCol, meta SegmentMeta, d dictionary, series uint16, arity int, isRange bool, rangeLo int, rows []int32, n int, page *PageResult) error {
	return appendPage(nil, cols, meta, d, series, arity, isRange, rangeLo, rows, n, nil, page)
}

func appendPage(view *columnarBlockView, cols *[streamColumnCount]decodedCol, meta SegmentMeta, d dictionary, series uint16, arity int, isRange bool, rangeLo int, rows []int32, n int, slot *querySlot, page *PageResult) error {
	baseRow := len(page.Rows)
	tagStart := len(page.tagBuf)
	page.Rows = page.Rows[:baseRow+n]
	page.tagBuf = page.tagBuf[:tagStart+n*arity]
	outRows := page.Rows[baseRow : baseRow+n]
	for i := range n {
		outRows[i] = PageRow{
			From: meta.CoveredFrom, Resolution: meta.Resolution, Exact: meta.Exact,
			Tags: page.tagBuf[tagStart+i*arity : tagStart+(i+1)*arity],
		}
	}
	for dim := range arity {
		col, err := resolvePageCol(view, cols, dim, slot)
		if err != nil {
			return err
		}
		if n > 0 && col.mode == modeConst {
			tag, err := d.resolve(series, uint8(dim), uint32(col.val))
			if err != nil {
				return err
			}
			for i := range outRows {
				outRows[i].Tags[dim] = tag
			}
			continue
		}
		for i := range n {
			r := rangeLo + i
			if !isRange {
				r = int(rows[i])
			}
			tag, err := d.resolve(series, uint8(dim), col.tagAt(r))
			if err != nil {
				return err
			}
			outRows[i].Tags[dim] = tag
		}
	}
	for m := range 4 {
		col, err := resolvePageCol(view, cols, 8+m, slot)
		if err != nil {
			return err
		}
		for i := range n {
			r := rangeLo + i
			if !isRange {
				r = int(rows[i])
			}
			outRows[i].Values[m] = col.valAt(r)
		}
	}
	mcol, err := resolvePageCol(view, cols, 12, slot)
	if err != nil {
		return err
	}
	for i := range n {
		r := rangeLo + i
		if !isRange {
			r = int(rows[i])
		}
		outRows[i].Mask = mcol.maskAt(r)
	}
	return nil
}
