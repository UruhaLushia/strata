package strata

import (
	"encoding/binary"
	"errors"

	"github.com/klauspost/compress/zstd"
)

func appendColumnarRowsStreams(raw []byte, rows []idRow, enc *zstd.Encoder, unsorted bool) ([]byte, error) {
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
		out := enc.EncodeAll(payloads[i], nil)
		if len(out) >= len(payloads[i]) {
			out = payloads[i]
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

func decodeColumnarRowsStreams(raw []byte, off *int, b *sealedBlock, count int) error {
	if *off >= len(raw) || raw[*off] != 1 {
		return errors.New("strata: invalid column stream version")
	}
	*off++
	if *off >= len(raw) || raw[*off] != streamColumnCount {
		return errors.New("strata: invalid column stream count")
	}
	*off++
	headerLen := streamColumnCount * 24
	if len(raw)-*off < headerLen {
		return errors.New("strata: truncated column stream directory")
	}
	descs := make([]columnStreamDesc, streamColumnCount)
	for i := range descs {
		p := raw[*off+i*24 : *off+(i+1)*24]
		descs[i] = columnStreamDesc{
			mode: p[0], width: p[1], flags: p[2], value: binary.LittleEndian.Uint64(p[4:12]),
			offset: binary.LittleEndian.Uint32(p[12:16]), length: binary.LittleEndian.Uint32(p[16:20]),
			rawLen: binary.LittleEndian.Uint32(p[20:24]),
		}
	}
	dataStart := *off + headerLen
	b.rows = make([]idRow, count)
	for i, d := range descs {
		if d.mode == modeConst || d.mode == modeZero {
			continue
		}
		if d.width == 0 {
			return errors.New("strata: invalid column stream width")
		}
		start, end := int(d.offset), int(d.offset+d.length)
		if start < dataStart || end < start || end > len(raw) {
			return errors.New("strata: invalid column stream bounds")
		}
		fill := func(plain []byte) error {
			if len(plain) != count*int(d.width) {
				return errors.New("strata: invalid column stream payload")
			}
			for row := range count {
				p := plain[row*int(d.width) : (row+1)*int(d.width)]
				if i < 8 {
					b.rows[row].tags[i] = uint32(readColumnUint64(p))
				} else if i < 12 {
					b.rows[row].values[i-8] = int64(readColumnUint64(p))
				} else {
					b.rows[row].initMask = p[0]
				}
			}
			return nil
		}
		var err error
		if d.flags&streamFlagRaw != 0 {
			if int(d.length) != int(d.rawLen) {
				return errors.New("strata: invalid raw column stream length")
			}
			err = fill(raw[start:end])
		} else {
			err = decodeColumnStream(raw[start:end], int(d.rawLen), fill)
		}
		if err != nil {
			return err
		}
	}
	for i, d := range descs {
		if d.mode != modeConst && d.mode != modeZero {
			continue
		}
		for row := range b.rows {
			if i < 8 {
				b.rows[row].tags[i] = uint32(d.value)
			} else if i < 12 {
				b.rows[row].values[i-8] = int64(d.value)
			} else {
				b.rows[row].initMask = byte(d.value)
			}
		}
	}
	*off = len(raw)
	return nil
}

func decodeColumnStream(raw []byte, expected int, use func([]byte) error) error {
	scratch := getZstdScratch()
	dec := getZstdDecoder()
	plain, err := dec.DecodeAll(raw, (*scratch)[:0])
	putZstdDecoder(dec)
	if err == nil && len(plain) != expected {
		err = errors.New("invalid column stream length")
	}
	if err == nil {
		err = use(plain)
	}
	putZstdScratch(scratch, plain)
	return err
}

func readColumnUint64(raw []byte) uint64 {
	switch len(raw) {
	case 1:
		return uint64(raw[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(raw))
	case 4:
		return uint64(binary.LittleEndian.Uint32(raw))
	default:
		return binary.LittleEndian.Uint64(raw)
	}
}
