package strata

import (
	"encoding/binary"

	"github.com/UruhaLushia/strata/codec"
)

func appendColumnarRowsStreams(raw []byte, rows []idRow, enc codec.Encoder, unsorted bool) ([]byte, error) {
	raw = append(raw, blockColumnarStreamsMarker, 1, streamColumnCount)
	descs := make([]columnStreamDesc, streamColumnCount)
	payloads := make([][]byte, streamColumnCount)
	for dim := range 8 {
		mode, value := tagColumnModeRows(rows, dim)
		descs[dim] = columnStreamDesc{mode: mode, width: tagModeWidth(mode), value: uint64(value)}
		if mode != modeConst {
			payloads[dim] = encodeTagColumn(rows, dim, mode)
		}
	}
	for metric := range 4 {
		mode, value := valueColumnModeRows(rows, metric)
		descs[8+metric] = columnStreamDesc{mode: mode, width: valueModeWidth(mode), value: uint64(value)}
		if mode != modeConst && mode != modeZero {
			payloads[8+metric] = encodeValueColumn(rows, metric, mode)
		}
	}
	maskMode, maskValue := maskColumnMode(rows)
	descs[12] = columnStreamDesc{mode: maskMode, width: 1, value: uint64(maskValue)}
	if maskMode != modeConst {
		payloads[12] = make([]byte, len(rows))
		for i, row := range rows {
			payloads[12][i] = normalizeMask(row.initMask)
		}
	}
	headerStart := len(raw)
	raw = append(raw, make([]byte, streamColumnCount*24)...)
	payloadStart := len(raw)
	for i := range descs {
		if len(payloads[i]) == 0 {
			continue
		}
		out, compressed, err := encodeSmaller(enc, nil, payloads[i], 8<<20)
		if err != nil {
			return nil, err
		}
		if !compressed {
			descs[i].flags |= streamFlagRaw
		}
		descs[i].offset = uint32(payloadStart)
		descs[i].length = uint32(len(out))
		descs[i].rawLen = uint32(len(payloads[i]))
		raw = append(raw, out...)
		payloadStart = len(raw)
	}
	for i, d := range descs {
		p := raw[headerStart+i*24 : headerStart+(i+1)*24]
		p[0], p[1] = d.mode, d.width
		flags := d.flags
		if i == 0 && unsorted {
			flags |= streamFlagUnsorted
		}
		p[2] = flags
		binary.LittleEndian.PutUint64(p[4:12], d.value)
		binary.LittleEndian.PutUint32(p[12:16], d.offset)
		binary.LittleEndian.PutUint32(p[16:20], d.length)
		binary.LittleEndian.PutUint32(p[20:24], d.rawLen)
	}
	return raw, nil
}

func tagModeWidth(mode byte) byte {
	switch mode {
	case modeU8:
		return 1
	case modeU16:
		return 2
	case modeU32:
		return 4
	}
	return 0
}

func valueModeWidth(mode byte) byte {
	switch mode {
	case modeU8:
		return 1
	case modeU16:
		return 2
	case modeU32:
		return 4
	case modeI64:
		return 8
	}
	return 0
}

func encodeTagColumn(rows []idRow, dim int, mode byte) []byte {
	out := make([]byte, 0, len(rows)*int(tagModeWidth(mode)))
	for _, row := range rows {
		switch mode {
		case modeU8:
			out = append(out, byte(row.tags[dim]))
		case modeU16:
			out = binary.LittleEndian.AppendUint16(out, uint16(row.tags[dim]))
		case modeU32:
			out = binary.LittleEndian.AppendUint32(out, row.tags[dim])
		}
	}
	return out
}

func encodeValueColumn(rows []idRow, metric int, mode byte) []byte {
	out := make([]byte, 0, len(rows)*int(valueModeWidth(mode)))
	for _, row := range rows {
		switch mode {
		case modeU8:
			out = append(out, byte(row.values[metric]))
		case modeU16:
			out = binary.LittleEndian.AppendUint16(out, uint16(row.values[metric]))
		case modeU32:
			out = binary.LittleEndian.AppendUint32(out, uint32(row.values[metric]))
		case modeI64:
			out = binary.LittleEndian.AppendUint64(out, uint64(row.values[metric]))
		}
	}
	return out
}

func decodeColumnarRowsStreams(raw []byte, off *int, b *sealedBlock, count int, id uint8, slot *querySlot) error {
	view := columnarBlockView{plain: raw, count: count, codecID: id}
	if err := parseStreamDirectory(&view, *off); err != nil {
		return err
	}
	b.rows = make([]idRow, count)
	for i := range view.descs {
		col, err := decodeStreamColInto(view, i, slot)
		if err != nil {
			return err
		}
		for row := range b.rows {
			switch {
			case i < 8:
				b.rows[row].tags[i] = col.tagAt(row)
			case i < 12:
				b.rows[row].values[i-8] = col.valAt(row)
			default:
				b.rows[row].initMask = col.maskAt(row)
			}
		}
	}
	*off = len(raw)
	return nil
}
