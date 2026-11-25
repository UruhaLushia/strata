package strata

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type columnarBlockView struct {
	plain    []byte
	count    int
	descs    [streamColumnCount]columnStreamDesc
	summary  Summary
	unsorted bool
}

func parseColumnarBlock(raw []byte) (columnarBlockView, error) {
	if !bytes.HasPrefix(raw, blockStreamsMagic[:]) {
		return columnarBlockView{}, errors.New("strata: non-stream block")
	}
	plain := raw[len(blockStreamsMagic):]
	off := 0
	for _, n := range []int{2, 1, 8, 8, 8, 8} {
		if _, err := takeBlockBytes(plain, &off, n); err != nil {
			return columnarBlockView{}, err
		}
	}
	mask, err := takeBlockBytes(plain, &off, 1)
	if err != nil {
		return columnarBlockView{}, err
	}
	view := columnarBlockView{plain: plain}
	view.summary.Mask = mask[0]
	for i := range view.summary.Values {
		field, err := takeBlockBytes(plain, &off, 8)
		if err != nil {
			return columnarBlockView{}, err
		}
		view.summary.Values[i] = int64(binary.LittleEndian.Uint64(field))
	}
	count, err := readBlockUvarint(plain, &off)
	if err != nil || count > 1<<20 {
		return columnarBlockView{}, errors.New("strata: invalid column row count")
	}
	view.count = int(count)
	if off >= len(plain) || plain[off] != blockColumnarStreamsMarker {
		return columnarBlockView{}, errors.New("strata: invalid column stream marker")
	}
	off++
	if off+2 > len(plain) || plain[off] != 1 || plain[off+1] != streamColumnCount {
		return columnarBlockView{}, errors.New("strata: invalid column stream header")
	}
	off += 2
	if len(plain)-off < streamColumnCount*24 {
		return columnarBlockView{}, errors.New("strata: truncated column stream directory")
	}
	for i := range view.descs {
		p := plain[off+i*24 : off+(i+1)*24]
		if i == 0 && p[2]&streamFlagUnsorted != 0 {
			view.unsorted = true
		}
		view.descs[i] = columnStreamDesc{
			mode: p[0], width: p[1], flags: p[2], value: binary.LittleEndian.Uint64(p[4:12]),
			offset: binary.LittleEndian.Uint32(p[12:16]), length: binary.LittleEndian.Uint32(p[16:20]),
			rawLen: binary.LittleEndian.Uint32(p[20:24]),
		}
	}
	return view, nil
}

func decodeStreamColInto(view columnarBlockView, idx int, slot *querySlot) (decodedCol, error) {
	desc := view.descs[idx]
	if desc.mode == modeConst || desc.mode == modeZero {
		return decodedCol{mode: desc.mode, val: desc.value, loaded: true}, nil
	}
	if desc.width == 0 {
		return decodedCol{}, errors.New("strata: invalid column stream width")
	}
	start, end := int(desc.offset), int(desc.offset+desc.length)
	if start < 0 || end < start || end > len(view.plain) {
		return decodedCol{}, errors.New("strata: invalid column stream bounds")
	}
	expected := view.count * int(desc.width)
	if int(desc.rawLen) != expected {
		return decodedCol{}, errors.New("strata: invalid column stream payload")
	}
	if desc.flags&streamFlagRaw != 0 {
		if end-start != expected {
			return decodedCol{}, errors.New("strata: invalid column stream length")
		}
		return decodedCol{mode: desc.mode, width: desc.width, data: view.plain[start:end], loaded: true}, nil
	}
	plain, err := slot.decodeZstd(view.plain[start:end])
	if err != nil {
		return decodedCol{}, err
	}
	if len(plain) != expected {
		return decodedCol{}, errors.New("strata: invalid column stream length")
	}
	return decodedCol{mode: desc.mode, width: desc.width, data: plain, loaded: true}, nil
}

func filterStreamView(view columnarBlockView, filters []compiledFilter, allowDim0BS bool, slot *querySlot) (rowSelection, error) {
	return filterCols(view.count, filters, allowDim0BS && !view.unsorted, func(dim int) (decodedCol, error) {
		return decodeStreamColInto(view, dim, slot)
	}, func(dim int) (byte, uint64) {
		return view.descs[dim].mode, view.descs[dim].value
	})
}

func streamRowBytes(view columnarBlockView, colMask uint16) int {
	total := 0
	for idx := range streamColumnCount {
		if colMask&(uint16(1)<<idx) == 0 {
			continue
		}
		d := view.descs[idx]
		if d.mode != modeConst && d.mode != modeZero {
			total += int(d.width)
		}
	}
	return total
}

func gatherColBytes(dst, src []byte, rows []int32, width int) {
	switch width {
	case 1:
		for i, r := range rows {
			dst[i] = src[r]
		}
	case 2:
		for i, r := range rows {
			ri, di := int(r)*2, i*2
			dst[di] = src[ri]
			dst[di+1] = src[ri+1]
		}
	case 4:
		for i, r := range rows {
			ri, di := int(r)*4, i*4
			copy(dst[di:di+4], src[ri:ri+4])
		}
	case 8:
		for i, r := range rows {
			ri, di := int(r)*8, i*8
			copy(dst[di:di+8], src[ri:ri+8])
		}
	}
}

func resolveBatchCol(cache *blockColCache, cols *[streamColumnCount]decodedCol, idx int) (decodedCol, error) {
	if cache != nil {
		return cache.load(idx)
	}
	return cols[idx], nil
}

func fillBatchFromStreamCache(cache *blockColCache, isRange bool, rangeLo int, rows []int32, n, arity int, dimMask, metricMask uint8, slot *querySlot) error {
	return fillBatch(cache, nil, isRange, rangeLo, rows, n, arity, dimMask, metricMask, slot)
}

func fillBatchFromCols(cols *[streamColumnCount]decodedCol, isRange bool, rangeLo int, rows []int32, n, arity int, dimMask, metricMask uint8, slot *querySlot) {
	_ = fillBatch(nil, cols, isRange, rangeLo, rows, n, arity, dimMask, metricMask, slot)
}

func fillBatch(cache *blockColCache, cols *[streamColumnCount]decodedCol, isRange bool, rangeLo int, rows []int32, n, arity int, dimMask, metricMask uint8, slot *querySlot) error {
	slot.ensureBatch(n, arity)
	for dim := range arity {
		dst := slot.tagIDs[dim][:n]
		if dimMask&(uint8(1)<<dim) == 0 {
			clear(dst)
			continue
		}
		col, err := resolveBatchCol(cache, cols, dim)
		if err != nil {
			return err
		}
		if isRange {
			decodeTagRange(dst, col, rangeLo)
		} else {
			decodeTagIndexed(dst, col, rows)
		}
	}
	for m := range 4 {
		dst := slot.values[m][:n]
		if metricMask&(uint8(1)<<m) == 0 {
			clear(dst)
			continue
		}
		col, err := resolveBatchCol(cache, cols, 8+m)
		if err != nil {
			return err
		}
		if isRange {
			decodeValRange(dst, col, rangeLo)
		} else {
			decodeValIndexed(dst, col, rows)
		}
	}
	mcol, err := resolveBatchCol(cache, cols, 12)
	if err != nil {
		return err
	}
	if isRange {
		decodeMaskRange(slot.masks[:n], mcol, rangeLo)
	} else {
		decodeMaskIndexed(slot.masks[:n], mcol, rows)
	}
	return nil
}
