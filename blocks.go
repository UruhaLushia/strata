package strata

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/klauspost/compress/zstd"
)

var (
	blockStreamsMagic      = [4]byte{'S', 'T', 'R', '2'}
	errBlockOutsideSection = errors.New("strata: block outside section")
)

const streamFlagUnsorted byte = 1

func encodeBlockLevel(b sealedBlock, level CompressionLevel) ([]byte, error) {
	enc := newZstdEncoder(level, 128<<10)
	defer enc.Close()
	return encodeBlockWithEncoder(b, enc)
}

func encodeBlockWithEncoder(b sealedBlock, enc *zstd.Encoder) ([]byte, error) {
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
		out := make([]byte, 0, len(raw)+len(blockStreamsMagic))
		out = append(out, blockStreamsMagic[:]...)
		return append(out, raw...), nil
	}
	return enc.EncodeAll(raw, nil), nil
}

func decodeBlock(raw []byte) (sealedBlock, error) {
	if bytes.HasPrefix(raw, blockStreamsMagic[:]) {
		return decodeBlockPlain(raw[4:], blockColumnarStreamsMarker)
	}
	plainScratch := getZstdScratch()
	dec := getZstdDecoder()
	plain, err := dec.DecodeAll(raw, (*plainScratch)[:0])
	putZstdDecoder(dec)
	if err != nil {
		putZstdScratch(plainScratch, plain)
		return sealedBlock{}, err
	}
	b, err := decodeBlockPlain(plain, blockColumnarMarker)
	putZstdScratch(plainScratch, plain)
	return b, err
}

func decodeBlockPlain(plain []byte, wantMarker byte) (sealedBlock, error) {
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
	if n > 1<<20 {
		return b, errors.New("strata: block row count too large")
	}
	if off >= len(plain) || plain[off] != wantMarker {
		return b, errors.New("strata: invalid block marker")
	}
	off++
	var decodeErr error
	if wantMarker == blockColumnarStreamsMarker {
		decodeErr = decodeColumnarRowsStreams(plain, &off, &b, int(n))
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

func readBlockUvarint(raw []byte, off *int) (uint64, error) {
	if *off >= len(raw) {
		return 0, errors.New("strata: truncated block")
	}
	value, n := binary.Uvarint(raw[*off:])
	if n <= 0 {
		return 0, errors.New("strata: invalid block varint")
	}
	*off += n
	return value, nil
}

func newZstdEncoder(level CompressionLevel, windowSize int) *zstd.Encoder {
	if windowSize <= 0 {
		windowSize = 128 << 10
	}
	encoderLevel := zstd.SpeedFastest
	switch level {
	case CompressionDefault:
		encoderLevel = zstd.SpeedDefault
	case CompressionBetter:
		encoderLevel = zstd.SpeedBetterCompression
	case CompressionBest:
		encoderLevel = zstd.SpeedBestCompression
	}
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(encoderLevel), zstd.WithWindowSize(windowSize))
	return enc
}

func zstdDecompress(raw []byte) ([]byte, error) {
	dec := getZstdDecoder()
	defer putZstdDecoder(dec)
	return dec.DecodeAll(raw, nil)
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

func decodeBlocks(f *osFile, s superblock, mapped []byte) ([]sealedMeta, error) {
	idxd := s.sections[4]
	if idxd.length == 0 || s.sections[3].length == 0 {
		return nil, nil
	}
	idxraw, err := sectionRaw(f, idxd, mapped)
	if err != nil {
		return nil, err
	}
	if crc(idxraw) != idxd.crc {
		return nil, errors.New("strata: TS index CRC mismatch")
	}
	if idxd.codec != 1 {
		return nil, errors.New("strata: unsupported TS index codec")
	}
	idxraw, err = zstdDecompress(idxraw)
	if err != nil {
		return nil, errors.New("strata: decode TS index: " + err.Error())
	}
	if idxd.count == 0 || len(idxraw)%int(idxd.count) != 0 {
		return nil, errors.New("strata: invalid TS index")
	}
	if len(idxraw)/int(idxd.count) != 104 {
		return nil, errors.New("strata: invalid TS index entry size")
	}
	out := make([]sealedMeta, 0, len(idxraw)/104)
	for i := 0; i < len(idxraw); i += 104 {
		p := idxraw[i : i+104]
		m := sealedMeta{
			series:   binary.LittleEndian.Uint16(p),
			res:      p[2],
			start:    int64(binary.LittleEndian.Uint64(p[8:16])),
			epoch:    binary.LittleEndian.Uint64(p[24:32]),
			offset:   binary.LittleEndian.Uint64(p[32:40]),
			length:   binary.LittleEndian.Uint32(p[40:44]),
			crc:      binary.LittleEndian.Uint32(p[48:52]),
			order:    binary.LittleEndian.Uint64(p[52:60]),
			rowCount: binary.LittleEndian.Uint32(p[60:64]),
			mask:     p[64],
		}
		for j := range m.values {
			m.values[j] = int64(binary.LittleEndian.Uint64(p[65+j*8 : 73+j*8]))
		}
		out = append(out, m)
	}
	return out, nil
}

func loadSealedBlock(db *DB, b sealedBlock) (sealedBlock, error) {
	raw, bufPtr, err := sectionBlockPooled(db.data, db.head.sections[3], b.offset, b.length, db.mappedBytes())
	defer putReadBuf(bufPtr)
	if err != nil {
		return sealedBlock{}, err
	}
	if crc(raw) != b.crc {
		return sealedBlock{}, errors.New("strata: block CRC mismatch")
	}
	decoded, err := decodeBlock(raw)
	if err != nil {
		return sealedBlock{}, err
	}
	decoded.offset, decoded.length, decoded.rawLen, decoded.crc = b.offset, b.length, b.rawLen, b.crc
	return decoded, nil
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
