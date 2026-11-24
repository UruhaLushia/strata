package strata

import (
	"bytes"
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

func scanWALFrameMetas(f *osFile, start, end int64) ([]walFrameMeta, error) {
	var metas []walFrameMeta
	var head [frameHeader]byte
	var payloadBuf []byte
	hash := crc32.New(castagnoli)
	pos := start
	for pos < end {
		if end-pos < frameHeader {
			return nil, io.ErrUnexpectedEOF
		}
		if _, err := f.ReadAt(head[:], pos); err != nil {
			return nil, err
		}
		if !bytes.Equal(head[:4], walMagic[:]) {
			return nil, errors.New("strata: invalid WAL magic")
		}
		ln := binary.LittleEndian.Uint32(head[4:8])
		if ln > 256<<20 || pos+frameHeader+int64(ln) > end {
			return nil, errors.New("strata: invalid WAL length")
		}
		if cap(payloadBuf) < int(ln) {
			payloadBuf = make([]byte, ln)
		} else {
			payloadBuf = payloadBuf[:ln]
		}
		if ln > 0 {
			if _, err := f.ReadAt(payloadBuf, pos+frameHeader); err != nil {
				return nil, err
			}
		}
		hash.Reset()
		_, _ = hash.Write(head[12:])
		_, _ = hash.Write(payloadBuf)
		if hash.Sum32() != binary.LittleEndian.Uint32(head[8:12]) {
			return nil, errors.New("strata: WAL CRC mismatch")
		}
		m := walFrameMeta{
			offset: pos,
			length: ln,
			typ:    head[12],
			seq:    binary.LittleEndian.Uint64(head[13:21]),
		}
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
		pos += frameHeader + int64(ln)
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
	hash := crc32.New(castagnoli)
	pos := start
	for pos < end {
		if end-pos < frameHeader {
			return dstPos, io.ErrUnexpectedEOF
		}
		if _, err := src.ReadAt(head[:], pos); err != nil {
			return dstPos, err
		}
		if !bytes.Equal(head[:4], walMagic[:]) {
			return dstPos, errors.New("strata: invalid WAL magic")
		}
		ln := binary.LittleEndian.Uint32(head[4:8])
		frameSize := int64(frameHeader) + int64(ln)
		if ln > 256<<20 || pos+frameSize > end {
			return dstPos, errors.New("strata: invalid WAL length")
		}
		seq := binary.LittleEndian.Uint64(head[13:21])
		if seq <= minSeq {
			pos += frameSize
			continue
		}
		if _, err := dst.WriteAt(head[:], dstPos); err != nil {
			return dstPos, err
		}
		hash.Reset()
		_, _ = hash.Write(head[12:])
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
			_, _ = hash.Write(chunk)
			srcOff += chunkLen
			dstPos += chunkLen
			rem -= chunkLen
		}
		if hash.Sum32() != binary.LittleEndian.Uint32(head[8:12]) {
			return dstPos, errors.New("strata: WAL CRC mismatch")
		}
		pos += frameSize
	}
	return dstPos, nil
}
