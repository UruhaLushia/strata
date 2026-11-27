package strata

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/UruhaLushia/strata/codec"
)

var (
	errBlockOutsideSection = errors.New("strata: block outside section")
	errTruncatedBlock      = errors.New("strata: truncated block")
	errInvalidBlockVint    = errors.New("strata: invalid block varint")
)

const (
	blockTagRawSmall   byte = 1
	blockTagCompSmall  byte = 2
	blockTagStreams    byte = 3
	streamFlagUnsorted byte = 1
)

const maxSmallBlockRaw = 32 << 10
const tsIndexEntryBytes = 80

func isStreamBlock(raw []byte) bool { return len(raw) != 0 && raw[0] == blockTagStreams }

func encodeBlockWithEncoder(b sealedBlock, enc codec.Encoder) ([]byte, error) {
	raw := make([]byte, 0, 64+len(b.rows)*73)
	raw = binary.LittleEndian.AppendUint16(raw, b.series)
	raw = append(raw, b.res)
	raw = binary.LittleEndian.AppendUint64(raw, uint64(b.start))
	raw = binary.LittleEndian.AppendUint64(raw, uint64(b.end))
	raw = binary.LittleEndian.AppendUint64(raw, b.epoch)
	raw = binary.LittleEndian.AppendUint64(raw, b.order)
	raw = append(raw, normalizeMask(b.summary.Mask))
	for _, v := range b.summary.Values {
		raw = binary.LittleEndian.AppendUint64(raw, uint64(v))
	}
	raw = binary.AppendUvarint(raw, uint64(len(b.rows)))
	rows := b.rows
	if !b.preserveOrder && len(rows) > 1 {
		rows = append([]idRow(nil), b.rows...)
		sort.Slice(rows, func(i, j int) bool {
			for dim := range rows[i].tags {
				if rows[i].tags[dim] != rows[j].tags[dim] {
					return rows[i].tags[dim] < rows[j].tags[dim]
				}
			}
			return false
		})
	}
	var err error
	if len(rows) >= 256 {
		raw, err = appendColumnarRowsStreams(raw, rows, enc, b.preserveOrder)
	} else {
		raw, err = appendColumnarRows(raw, rows)
	}
	if err != nil {
		return nil, err
	}
	if len(rows) >= 256 {
		return append([]byte{blockTagStreams}, raw...), nil
	}
	out, compressed, err := encodeSmaller(enc, nil, raw, maxSmallBlockRaw)
	if err != nil {
		return nil, err
	}
	tag := blockTagRawSmall
	if compressed {
		tag = blockTagCompSmall
	}
	return append([]byte{tag}, out...), nil
}

func smallBlockPlain(raw []byte, id uint8, slot *querySlot) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errTruncatedBlock
	}
	switch raw[0] {
	case blockTagRawSmall:
		return raw[1:], nil
	case blockTagCompSmall:
		return slot.decode(id, raw[1:], maxSmallBlockRaw)
	default:
		return nil, errors.New("strata: invalid TS block tag")
	}
}

func decodeBlock(raw []byte, id uint8, codecs *codecTable) (sealedBlock, error) {
	slot := querySlot{codecs: codecs}
	defer slot.close()
	if isStreamBlock(raw) {
		return decodeBlockPlain(raw[1:], blockColumnarStreamsMarker, id, &slot)
	}
	plain, err := smallBlockPlain(raw, id, &slot)
	if err != nil {
		return sealedBlock{}, err
	}
	return decodeBlockPlain(plain, blockColumnarMarker, id, &slot)
}

func decodeBlockPlain(plain []byte, wantMarker byte, id uint8, slot *querySlot) (sealedBlock, error) {
	var b sealedBlock
	off := 0
	field, err := takeBlockBytes(plain, &off, 2)
	if err != nil {
		return b, err
	}
	b.series = binary.LittleEndian.Uint16(field)
	field, err = takeBlockBytes(plain, &off, 1)
	if err != nil {
		return b, err
	}
	b.res = field[0]
	field, err = takeBlockBytes(plain, &off, 8)
	if err != nil {
		return b, err
	}
	b.start = int64(binary.LittleEndian.Uint64(field))
	field, err = takeBlockBytes(plain, &off, 8)
	if err != nil {
		return b, err
	}
	b.end = int64(binary.LittleEndian.Uint64(field))
	field, err = takeBlockBytes(plain, &off, 8)
	if err != nil {
		return b, err
	}
	b.epoch = binary.LittleEndian.Uint64(field)
	field, err = takeBlockBytes(plain, &off, 8)
	if err != nil {
		return b, err
	}
	b.order = binary.LittleEndian.Uint64(field)
	field, err = takeBlockBytes(plain, &off, 1)
	if err != nil {
		return b, err
	}
	b.summary.Mask = field[0]
	for i := range b.summary.Values {
		field, err = takeBlockBytes(plain, &off, 8)
		if err != nil {
			return b, err
		}
		b.summary.Values[i] = int64(binary.LittleEndian.Uint64(field))
	}
	n, err := readBlockUvarint(plain, &off)
	if err != nil {
		return b, err
	}
	if n > 1<<20 || (n < 256) != (wantMarker == blockColumnarMarker) {
		return b, errors.New("strata: block row count too large")
	}
	if off >= len(plain) || plain[off] != wantMarker {
		return b, errors.New("strata: invalid block marker")
	}
	off++
	var decodeErr error
	if wantMarker == blockColumnarStreamsMarker {
		decodeErr = decodeColumnarRowsStreams(plain, &off, &b, int(n), id, slot)
	} else {
		decodeErr = decodeColumnarRows(plain, &off, &b, int(n))
	}
	if decodeErr != nil {
		return b, decodeErr
	}
	if off != len(plain) {
		return b, errors.New("strata: trailing block")
	}
	return b, nil
}

func takeBlockBytes(raw []byte, off *int, n int) ([]byte, error) {
	if n < 0 || *off > len(raw)-n {
		return nil, errors.New("strata: truncated block")
	}
	field := raw[*off : *off+n]
	*off += n
	return field, nil
}

func readSliceUvarint(raw []byte, off *int, truncErr, invalidErr error) (uint64, error) {
	if *off >= len(raw) {
		return 0, truncErr
	}
	value, n := binary.Uvarint(raw[*off:])
	if n <= 0 {
		return 0, invalidErr
	}
	*off += n
	return value, nil
}

func readBlockUvarint(raw []byte, off *int) (uint64, error) {
	return readSliceUvarint(raw, off, errTruncatedBlock, errInvalidBlockVint)
}

func sectionRaw(f *osFile, d sectionDesc, mapped []byte) ([]byte, error) {
	if d.offset < dataStart || d.length > 1<<40 {
		return nil, errors.New("strata: invalid section")
	}
	if mapped != nil && d.offset >= dataStart && d.offset+d.length <= dataStart+uint64(len(mapped)) {
		return mapped[d.offset-dataStart : d.offset-dataStart+d.length], nil
	}
	raw := make([]byte, d.length)
	_, err := f.ReadAt(raw, int64(d.offset))
	return raw, err
}

func decodeBlocks(f *osFile, s superblock, mapped []byte, codecs *codecTable) ([]sealedMeta, error) {
	idxd := s.sections[4]
	if idxd.length == 0 || s.sections[3].length == 0 {
		return nil, nil
	}
	expected := uint64(idxd.count) * tsIndexEntryBytes
	if idxd.count == 0 || uint64(idxd.count) > s.sections[secTSBlocks].length ||
		expected != uint64(idxd.rawLen) || idxd.codec != codec.IDNone && expected > maxSectionRawBytes {
		return nil, errors.New("strata: invalid TS index entry size")
	}
	idxraw, err := sectionRaw(f, idxd, mapped)
	if err != nil {
		return nil, err
	}
	if crc(idxraw) != idxd.crc {
		return nil, errors.New("strata: TS index CRC mismatch")
	}
	if idxd.codec != codec.IDNone {
		idxraw, err = codecs.decode(idxd.codec, nil, idxraw, int(expected))
		if err != nil {
			return nil, err
		}
	}
	if uint64(len(idxraw)) != expected {
		return nil, errors.New("strata: invalid TS index length")
	}
	out := make([]sealedMeta, 0, len(idxraw)/tsIndexEntryBytes)
	for i := 0; i < len(idxraw); i += tsIndexEntryBytes {
		p := idxraw[i : i+tsIndexEntryBytes]
		m := sealedMeta{
			series:   binary.LittleEndian.Uint16(p[0:2]),
			res:      p[2],
			mask:     p[3],
			length:   binary.LittleEndian.Uint32(p[4:8]),
			start:    int64(binary.LittleEndian.Uint64(p[8:16])),
			epoch:    binary.LittleEndian.Uint64(p[16:24]),
			offset:   binary.LittleEndian.Uint64(p[24:32]),
			order:    binary.LittleEndian.Uint64(p[32:40]),
			crc:      binary.LittleEndian.Uint32(p[40:44]),
			rowCount: binary.LittleEndian.Uint32(p[44:48]),
		}
		for j := range m.values {
			m.values[j] = int64(binary.LittleEndian.Uint64(p[48+j*8 : 56+j*8]))
		}
		out = append(out, m)
	}
	return out, nil
}

func sectionBlock(f *osFile, d sectionDesc, off uint64, n uint32, mapped []byte) ([]byte, error) {
	raw, bufPtr, err := sectionBlockPooled(f, d, off, n, mapped)
	if err != nil {
		putReadBuf(bufPtr)
		return nil, err
	}
	defer putReadBuf(bufPtr)
	return append([]byte(nil), raw...), nil
}

func sectionBlockPooled(f *osFile, d sectionDesc, off uint64, n uint32, mapped []byte) ([]byte, *[]byte, error) {
	if off < d.offset || off+uint64(n) > d.offset+d.length {
		return nil, nil, errBlockOutsideSection
	}
	if mapped != nil && off >= dataStart && off+uint64(n) <= dataStart+uint64(len(mapped)) {
		return mapped[off-dataStart : off-dataStart+uint64(n)], nil, nil
	}
	if n > maxReadBufCap {
		raw := make([]byte, n)
		_, err := f.ReadAt(raw, int64(off))
		return raw, nil, err
	}
	bufPtr := getReadBuf(int(n))
	raw := (*bufPtr)[:n]
	_, err := f.ReadAt(raw, int64(off))
	return raw, bufPtr, err
}
