package strata

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
)

const (
	walRowEncodedBytes   = 8*4 + 4*8 + 1 // 65 bytes per idRow
	walBucketHeaderFixed = 27 + 8 + 33   // 68 bytes per bucket header
)

type walFrameMeta struct {
	offset        int64
	length        uint32
	typ           byte
	hasTS         bool
	series        uint16
	seq           uint64
	minTS         int64
	maxTS         int64
	kvRev         uint64
	resetOldEpoch uint64
}

func (m walFrameMeta) isResetAll() bool {
	return m.typ == frameReset && m.hasTS && m.minTS == math.MinInt64 && m.maxTS == math.MaxInt64
}

type walBucketRef struct {
	frameIdx uint32
	series   uint16
	_        uint16
	ts       int64
	epoch    uint64
}

func readWALFrame(f *osFile, pos, end int64, head *[frameHeader]byte, buf *[]byte) (byte, uint64, int64, error) {
	if end-pos < frameHeader {
		return 0, 0, pos, io.ErrUnexpectedEOF
	}
	if _, err := f.ReadAt(head[:], pos); err != nil {
		return 0, 0, pos, err
	}
	typ, seq, ln, wantCRC, ok := decodeWALHeader(head, pos, end)
	if !ok {
		return 0, 0, pos, errors.New("strata: invalid WAL header")
	}
	if cap(*buf) < int(ln) {
		*buf = make([]byte, ln)
	} else {
		*buf = (*buf)[:ln]
	}
	if ln > 0 {
		if _, err := f.ReadAt(*buf, pos+frameHeader); err != nil {
			return 0, 0, pos, err
		}
	}
	if crc32.Update(crc32.Update(0, castagnoli, head[12:]), castagnoli, *buf) != wantCRC {
		return 0, 0, pos, errors.New("strata: WAL CRC mismatch")
	}
	return typ, seq, pos + frameHeader + int64(ln), nil
}

func scanWALFrameMetas(f *osFile, start, end int64) ([]walFrameMeta, error) {
	var metas []walFrameMeta
	var head [frameHeader]byte
	var payloadBuf []byte
	pos := start
	for pos < end {
		typ, seq, nextPos, err := readWALFrame(f, pos, end, &head, &payloadBuf)
		if err != nil {
			return nil, err
		}
		m := walFrameMeta{offset: pos, length: uint32(len(payloadBuf)), typ: typ, seq: seq}
		switch m.typ {
		case frameDelta:
			minTS, maxTS, cnt, err := scanDeltaBounds(payloadBuf)
			if err == nil && cnt > 0 {
				m.minTS, m.maxTS, m.hasTS = minTS, maxTS, true
			}
		case frameReset:
			if x, err := decodeReset(payloadBuf); err == nil {
				m.minTS, m.maxTS, m.hasTS = x.from, x.to, true
				m.series = x.series
				m.resetOldEpoch = x.oldEpoch
			}
		case frameKVBatch:
			if rev, _, err := decodeKVBatch(payloadBuf); err == nil {
				m.kvRev = rev
				m.hasTS = true
			}
		}
		metas = append(metas, m)
		pos = nextPos
	}
	return metas, nil
}

func skipDeltaDicts(raw []byte) (int, error) {
	off := 0
	dn, err := readBlockUvarint(raw, &off)
	if err != nil {
		return 0, err
	}
	for range dn {
		if off+7 > len(raw) {
			return 0, errors.New("strata: truncated delta dictionary")
		}
		off += 7
		ln, err := readBlockUvarint(raw, &off)
		if err != nil || ln > uint64(len(raw)-off) {
			return 0, errors.New("strata: invalid delta dictionary")
		}
		off += int(ln)
	}
	return off, nil
}

func scanDeltaBounds(raw []byte) (int64, int64, int, error) {
	off, err := skipDeltaDicts(raw)
	if err != nil {
		return 0, 0, 0, err
	}
	bn, err := readBlockUvarint(raw, &off)
	if err != nil {
		return 0, 0, 0, err
	}
	var minTS, maxTS int64
	for i := range bn {
		if off+walBucketHeaderFixed > len(raw) {
			return 0, 0, 0, errors.New("strata: truncated delta bucket")
		}
		ts := int64(binary.LittleEndian.Uint64(raw[off+2 : off+10]))
		if i == 0 || ts < minTS {
			minTS = ts
		}
		if i == 0 || ts > maxTS {
			maxTS = ts
		}
		off += walBucketHeaderFixed
		rn, err := readBlockUvarint(raw, &off)
		if err != nil || rn > uint64(len(raw)-off)/walRowEncodedBytes {
			return 0, 0, 0, errors.New("strata: invalid delta rows")
		}
		off += int(rn) * walRowEncodedBytes
	}
	if off != len(raw) {
		return 0, 0, 0, errors.New("strata: trailing delta")
	}
	return minTS, maxTS, int(bn), nil
}

func collectWALBucketRefs(f *osFile, eligible []walFrameMeta) ([]walBucketRef, error) {
	var refs []walBucketRef
	var buf []byte
	for fi, m := range eligible {
		if m.typ != frameDelta {
			continue
		}
		var err error
		buf, err = readWALPayload(f, m, buf)
		if err != nil {
			return nil, err
		}
		off, err := skipDeltaDicts(buf)
		if err != nil {
			return nil, err
		}
		bn, err := readBlockUvarint(buf, &off)
		if err != nil {
			return nil, err
		}
		for range bn {
			if off+walBucketHeaderFixed > len(buf) {
				return nil, errors.New("strata: truncated delta bucket")
			}
			sid := binary.LittleEndian.Uint16(buf[off : off+2])
			ts := int64(binary.LittleEndian.Uint64(buf[off+2 : off+10]))
			epoch := binary.LittleEndian.Uint64(buf[off+10 : off+18])
			off += walBucketHeaderFixed
			rn, err := readBlockUvarint(buf, &off)
			if err != nil || rn > uint64(len(buf)-off)/walRowEncodedBytes {
				return nil, errors.New("strata: invalid delta rows")
			}
			off += int(rn) * walRowEncodedBytes
			refs = append(refs, walBucketRef{frameIdx: uint32(fi), series: sid, ts: ts, epoch: epoch})
		}
	}
	return refs, nil
}

func readWALPayload(f *osFile, m walFrameMeta, buf []byte) ([]byte, error) {
	if cap(buf) < int(m.length) {
		buf = make([]byte, m.length)
	} else {
		buf = buf[:m.length]
	}
	if m.length > 0 {
		if _, err := f.ReadAt(buf, m.offset+frameHeader); err != nil {
			return buf, err
		}
	}
	return buf, nil
}

func decodeDeltaForHour(raw []byte, wantSeries uint16, wantHour int64) ([]*bucket, error) {
	off, err := skipDeltaDicts(raw)
	if err != nil {
		return nil, err
	}
	bn, err := readBlockUvarint(raw, &off)
	if err != nil {
		return nil, err
	}
	var out []*bucket
	for range bn {
		if off+walBucketHeaderFixed > len(raw) {
			return nil, errors.New("strata: truncated delta bucket")
		}
		sid := binary.LittleEndian.Uint16(raw[off : off+2])
		ts := int64(binary.LittleEndian.Uint64(raw[off+2 : off+10]))
		if sid != wantSeries || hourTS(ts) != wantHour {
			off += walBucketHeaderFixed
			rn, err := readBlockUvarint(raw, &off)
			if err != nil || rn > uint64(len(raw)-off)/walRowEncodedBytes {
				return nil, errors.New("strata: invalid delta rows")
			}
			off += int(rn) * walRowEncodedBytes
			continue
		}
		x := &bucket{
			series: sid,
			ts:     ts,
			epoch:  binary.LittleEndian.Uint64(raw[off+10 : off+18]),
			order:  binary.LittleEndian.Uint64(raw[off+18 : off+26]),
			arity:  raw[off+26],
		}
		off += 27
		for j := range 4 {
			x.summaryOps[j] = MetricOp(raw[off+j])
			x.rowOps[j] = MetricOp(raw[off+4+j])
		}
		off += 8
		x.summary.Mask = raw[off]
		off++
		for j := range 4 {
			x.summary.Values[j] = int64(binary.LittleEndian.Uint64(raw[off+j*8 : off+(j+1)*8]))
		}
		off += 32
		rn, err := readBlockUvarint(raw, &off)
		if err != nil || rn > uint64(len(raw)-off)/walRowEncodedBytes {
			return nil, errors.New("strata: invalid delta rows")
		}
		x.rows = make([]idRow, rn)
		for j := range x.rows {
			p := raw[off : off+walRowEncodedBytes]
			for k := range 8 {
				x.rows[j].tags[k] = binary.LittleEndian.Uint32(p[k*4 : k*4+4])
			}
			for k := range 4 {
				x.rows[j].values[k] = int64(binary.LittleEndian.Uint64(p[32+k*8 : 32+k*8+8]))
			}
			x.rows[j].initMask = p[64]
			off += walRowEncodedBytes
		}
		out = append(out, x)
	}
	return out, nil
}

func copyWALRangeStreaming(dst, src *osFile, start, end int64, minSeq uint64, dstPos int64, copyBuf []byte) (int64, error) {
	if len(copyBuf) < frameHeader {
		copyBuf = make([]byte, 64<<10)
	}
	var head [frameHeader]byte
	pos := start
	for pos < end {
		if end-pos < frameHeader {
			return dstPos, io.ErrUnexpectedEOF
		}
		if _, err := src.ReadAt(head[:], pos); err != nil {
			return dstPos, err
		}
		_, seq, ln, wantCRC, ok := decodeWALHeader(&head, pos, end)
		if !ok {
			return dstPos, errors.New("strata: invalid WAL header")
		}
		frameSize := int64(frameHeader) + int64(ln)
		if seq <= minSeq {
			pos += frameSize
			continue
		}
		if _, err := dst.WriteAt(head[:], dstPos); err != nil {
			return dstPos, err
		}
		sum := crc32.Update(0, castagnoli, head[12:])
		srcOff := pos + frameHeader
		dstPos += frameHeader
		rem := int64(ln)
		for rem > 0 {
			chunkLen := min(rem, int64(len(copyBuf)))
			chunk := copyBuf[:chunkLen]
			if _, err := src.ReadAt(chunk, srcOff); err != nil {
				return dstPos, err
			}
			if _, err := dst.WriteAt(chunk, dstPos); err != nil {
				return dstPos, err
			}
			sum = crc32.Update(sum, castagnoli, chunk)
			srcOff += chunkLen
			dstPos += chunkLen
			rem -= chunkLen
		}
		if sum != wantCRC {
			return dstPos, errors.New("strata: WAL CRC mismatch")
		}
		pos += frameSize
	}
	return dstPos, nil
}

func markTailDeltasLive(src *osFile, tailMetas []walFrameMeta, resetAllSeq map[uint16]uint64, live dictLiveBits) error {
	var buf []byte
	for _, m := range tailMetas {
		var err error
		buf, err = readWALPayload(src, m, buf)
		if err != nil {
			return err
		}
		dicts, buckets, err := decodeDelta(buf)
		if err != nil {
			return err
		}
		for _, de := range dicts {
			if de.value != "" && m.seq > resetAllSeq[de.series] {
				live.mark(dictDimKey{series: de.series, dim: de.dim}, de.id)
			}
		}
		for _, b := range buckets {
			if m.seq <= resetAllSeq[b.series] {
				continue
			}
			for _, row := range b.rows {
				for dim, tag := range row.tags {
					if tag != 0 {
						live.mark(dictDimKey{series: b.series, dim: uint8(dim)}, tag)
					}
				}
			}
		}
	}
	return nil
}

func copyWALRangeWithResurrection(dst, src *osFile, start, end int64, minSeq uint64, dstPos int64, snap *dictCompactSnap, live dictLiveBits, pruned map[dictDimKey]bool) (int64, error) {
	snapDims := make(map[dictDimKey]*dictDimCompactSnap, len(pruned))
	for i := range snap.dims {
		if pruned[snap.dims[i].key] {
			snapDims[snap.dims[i].key] = &snap.dims[i]
		}
	}
	var head [frameHeader]byte
	var payloadBuf []byte
	resetSeen := make(map[uint16]bool)
	for pos := start; pos < end; {
		typ, seq, nextPos, err := readWALFrame(src, pos, end, &head, &payloadBuf)
		if err != nil {
			return dstPos, err
		}
		pos = nextPos
		if seq <= minSeq {
			continue
		}
		if typ == frameReset {
			if x, err := decodeReset(payloadBuf); err == nil && x.from == math.MinInt64 && x.to == math.MaxInt64 {
				resetSeen[x.series] = true
			}
		} else if typ == frameDelta {
			dicts, buckets, err := decodeDelta(payloadBuf)
			if err != nil {
				return dstPos, err
			}
			for _, de := range dicts {
				if !resetSeen[de.series] {
					live.mark(dictDimKey{series: de.series, dim: de.dim}, de.id)
				}
			}
			var extra []dictEntry
			for _, b := range buckets {
				if resetSeen[b.series] {
					continue
				}
				for _, row := range b.rows {
					for dim, tag := range row.tags {
						dk := dictDimKey{series: b.series, dim: uint8(dim)}
						sd := snapDims[dk]
						if tag == 0 || sd == nil || tag >= sd.nextID || live.has(dk, tag) {
							continue
						}
						live.mark(dk, tag)
						if v := sd.resolve(tag); v != "" {
							extra = append(extra, dictEntry{id: tag, series: b.series, dim: uint8(dim), value: v})
						}
					}
				}
			}
			if len(extra) > 0 {
				if dstPos, err = appendFrame(dst, dstPos, frameDelta, seq, encodeDelta(append(extra, dicts...), buckets)); err != nil {
					return dstPos, err
				}
				continue
			}
		}
		if _, err = dst.WriteAt(head[:], dstPos); err != nil {
			return dstPos, err
		}
		dstPos += frameHeader
		if len(payloadBuf) > 0 {
			if _, err = dst.WriteAt(payloadBuf, dstPos); err != nil {
				return dstPos, err
			}
			dstPos += int64(len(payloadBuf))
		}
	}
	return dstPos, nil
}
