package strata

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
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
			if err != nil {
				return nil, err
			}
			if cnt > 0 {
				m.minTS, m.maxTS, m.hasTS = minTS, maxTS, true
			}
		case frameReset:
			x, err := decodeReset(payloadBuf)
			if err != nil {
				return nil, err
			}
			m.minTS, m.maxTS, m.hasTS = x.from, x.to, true
			m.series = x.series
			m.resetOldEpoch = x.oldEpoch
		case frameKVBatch:
			rev, _, err := decodeKVBatch(payloadBuf)
			if err != nil {
				return nil, err
			}
			m.kvRev = rev
			m.hasTS = true
		}
		metas = append(metas, m)
		pos = nextPos
	}
	return metas, nil
}

func skipDeltaDicts(raw []byte) (int, error) {
	off := 0
	dn, err := readBlockUvarint(raw, &off)
	if err != nil || dn > uint64(len(raw)-off)/4 {
		return 0, errors.New("strata: invalid delta dictionary")
	}
	for range dn {
		id, err := readBlockUvarint(raw, &off)
		if err != nil || id > uint64(^uint32(0)) {
			return 0, errors.New("strata: invalid delta dictionary")
		}
		sid, err := readBlockUvarint(raw, &off)
		if err != nil || sid > uint64(^uint16(0)) || off >= len(raw) {
			return 0, errors.New("strata: invalid delta dictionary")
		}
		off++
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
	if err != nil || bn > uint64(len(raw)-off)/walDeltaBucketMinBytes {
		return 0, 0, 0, errors.New("strata: invalid delta bucket")
	}
	var minTS, maxTS int64
	for i := range bn {
		body, err := nextDeltaBucketBody(raw, &off)
		if err != nil {
			return 0, 0, 0, err
		}
		ts := int64(binary.LittleEndian.Uint64(body[2:10]))
		if i == 0 || ts < minTS {
			minTS = ts
		}
		if i == 0 || ts > maxTS {
			maxTS = ts
		}
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
		if err != nil || bn > uint64(len(buf)-off)/walDeltaBucketMinBytes {
			return nil, errors.New("strata: invalid delta bucket")
		}
		for range bn {
			body, err := nextDeltaBucketBody(buf, &off)
			if err != nil {
				return nil, err
			}
			refs = append(refs, walBucketRef{
				frameIdx: uint32(fi),
				series:   binary.LittleEndian.Uint16(body[0:2]),
				ts:       int64(binary.LittleEndian.Uint64(body[2:10])),
				epoch:    binary.LittleEndian.Uint64(body[10:18]),
			})
		}
		if off != len(buf) {
			return nil, errors.New("strata: trailing delta")
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
	if err != nil || bn > uint64(len(raw)-off)/walDeltaBucketMinBytes {
		return nil, errors.New("strata: invalid delta bucket")
	}
	var out []*bucket
	for range bn {
		body, err := nextDeltaBucketBody(raw, &off)
		if err != nil {
			return nil, err
		}
		sid := binary.LittleEndian.Uint16(body[0:2])
		ts := int64(binary.LittleEndian.Uint64(body[2:10]))
		if sid != wantSeries || hourTS(ts) != wantHour {
			continue
		}
		x, err := decodeDeltaBucket(body)
		if err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	if off != len(raw) {
		return nil, errors.New("strata: trailing delta")
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
						v, err := sd.resolve(tag)
						if err != nil {
							return dstPos, err
						}
						if v != "" {
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
