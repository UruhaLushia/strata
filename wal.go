package strata

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

var walMagic = [4]byte{'U', 'D', 'W', '1'}

const (
	frameDelta             byte = 1
	frameKVBatch           byte = 2
	frameReset             byte = 3
	frameHeader                 = 21
	walDeltaBucketPrefix        = 18 // series(2) + ts(8) + epoch(8)
	walDeltaBucketMinBytes      = 23
)

type walFrame struct {
	typ     byte
	seq     uint64
	payload []byte
}

func packMetricOps(ops [4]MetricOp) byte {
	return byte(ops[0]&3) | byte(ops[1]&3)<<2 | byte(ops[2]&3)<<4 | byte(ops[3]&3)<<6
}

func unpackMetricOps(v byte) [4]MetricOp {
	return [4]MetricOp{MetricOp(v & 3), MetricOp((v >> 2) & 3), MetricOp((v >> 4) & 3), MetricOp((v >> 6) & 3)}
}

func readDeltaVarint(raw []byte, off *int) (int64, error) {
	if *off >= len(raw) {
		return 0, errors.New("strata: truncated delta varint")
	}
	v, n := binary.Varint(raw[*off:])
	if n <= 0 {
		return 0, errors.New("strata: invalid delta varint")
	}
	*off += n
	return v, nil
}

func nextDeltaBucketBody(raw []byte, off *int) ([]byte, error) {
	blen, err := readBlockUvarint(raw, off)
	if err != nil || blen < walDeltaBucketPrefix || blen > uint64(len(raw)-*off) {
		return nil, errors.New("strata: truncated delta bucket")
	}
	body := raw[*off : *off+int(blen)]
	*off += int(blen)
	return body, nil
}

func encodeDelta(dicts []dictEntry, buckets []*bucket) []byte {
	var b []byte
	b = binary.AppendUvarint(b, uint64(len(dicts)))
	for _, d := range dicts {
		b = binary.AppendUvarint(b, uint64(d.id))
		b = binary.AppendUvarint(b, uint64(d.series))
		b = append(b, d.dim)
		b = binary.AppendUvarint(b, uint64(len(d.value)))
		b = append(b, d.value...)
	}
	b = binary.AppendUvarint(b, uint64(len(buckets)))
	var body []byte
	for _, x := range buckets {
		body = body[:0]
		body = binary.LittleEndian.AppendUint16(body, x.series)
		body = binary.LittleEndian.AppendUint64(body, uint64(x.ts))
		body = binary.LittleEndian.AppendUint64(body, x.epoch)
		body = binary.AppendUvarint(body, x.order)
		sumMask := normalizeMask(x.summary.Mask)
		body = append(body, x.arity, packMetricOps(x.summaryOps), packMetricOps(x.rowOps), sumMask)
		for j := range 4 {
			if sumMask&(1<<j) != 0 {
				body = binary.AppendVarint(body, x.summary.Values[j])
			}
		}
		body = binary.AppendUvarint(body, uint64(len(x.rows)))
		arity := min(int(x.arity), 8)
		for _, r := range x.rows {
			mask := normalizeMask(r.initMask)
			body = append(body, mask)
			for k := range arity {
				body = binary.AppendUvarint(body, uint64(r.tags[k]))
			}
			for k := range 4 {
				if mask&(1<<k) != 0 {
					body = binary.AppendVarint(body, r.values[k])
				}
			}
		}
		b = binary.AppendUvarint(b, uint64(len(body)))
		b = append(b, body...)
	}
	return b
}

func decodeDeltaBucket(body []byte) (*bucket, error) {
	if len(body) < walDeltaBucketPrefix {
		return nil, errors.New("strata: truncated delta bucket")
	}
	x := &bucket{
		series: binary.LittleEndian.Uint16(body[0:2]),
		ts:     int64(binary.LittleEndian.Uint64(body[2:10])),
		epoch:  binary.LittleEndian.Uint64(body[10:18]),
	}
	off := walDeltaBucketPrefix
	var err error
	if x.order, err = readBlockUvarint(body, &off); err != nil {
		return nil, err
	}
	if off+4 > len(body) {
		return nil, errors.New("strata: truncated delta bucket")
	}
	x.arity = body[off]
	if x.arity > 8 {
		return nil, errors.New("strata: invalid delta arity")
	}
	x.summaryOps = unpackMetricOps(body[off+1])
	x.rowOps = unpackMetricOps(body[off+2])
	x.summary.Mask = body[off+3]
	off += 4
	if x.summary.Mask&^uint8(0x0f) != 0 {
		return nil, errors.New("strata: invalid delta summary mask")
	}
	for j := range 4 {
		if x.summary.Mask&(1<<j) == 0 {
			continue
		}
		if x.summary.Values[j], err = readDeltaVarint(body, &off); err != nil {
			return nil, err
		}
	}
	rn, err := readBlockUvarint(body, &off)
	if err != nil || rn > uint64(len(body)-off) {
		return nil, errors.New("strata: invalid delta rows")
	}
	arity := int(x.arity)
	if rn > 0 {
		x.rows = make([]idRow, rn)
	}
	for j := range x.rows {
		if off >= len(body) {
			return nil, errors.New("strata: truncated delta row")
		}
		mask := body[off]
		off++
		if mask&^uint8(0x0f) != 0 {
			return nil, errors.New("strata: invalid delta row mask")
		}
		x.rows[j].initMask = mask
		for k := range arity {
			tag, err := readBlockUvarint(body, &off)
			if err != nil || tag > uint64(^uint32(0)) {
				return nil, errors.New("strata: invalid delta row tag")
			}
			x.rows[j].tags[k] = uint32(tag)
		}
		for k := range 4 {
			if mask&(1<<k) == 0 {
				continue
			}
			if x.rows[j].values[k], err = readDeltaVarint(body, &off); err != nil {
				return nil, err
			}
		}
	}
	if off != len(body) {
		return nil, errors.New("strata: trailing delta bucket")
	}
	return x, nil
}

func decodeDelta(raw []byte) ([]dictEntry, []*bucket, error) {
	off := 0
	dn, err := readBlockUvarint(raw, &off)
	if err != nil || dn > uint64(len(raw)-off)/4 {
		return nil, nil, errors.New("strata: invalid delta dictionary")
	}
	dicts := make([]dictEntry, dn)
	for i := range dicts {
		id, err := readBlockUvarint(raw, &off)
		if err != nil || id > uint64(^uint32(0)) {
			return nil, nil, errors.New("strata: invalid delta dictionary")
		}
		sid, err := readBlockUvarint(raw, &off)
		if err != nil || sid > uint64(^uint16(0)) || off >= len(raw) {
			return nil, nil, errors.New("strata: invalid delta dictionary")
		}
		dim := raw[off]
		off++
		ln, err := readBlockUvarint(raw, &off)
		if err != nil || ln > uint64(len(raw)-off) {
			return nil, nil, errors.New("strata: invalid delta dictionary")
		}
		dicts[i] = dictEntry{id: uint32(id), series: uint16(sid), dim: dim, value: string(raw[off : off+int(ln)])}
		off += int(ln)
	}
	bn, err := readBlockUvarint(raw, &off)
	if err != nil || bn > uint64(len(raw)-off)/walDeltaBucketMinBytes {
		return nil, nil, errors.New("strata: invalid delta bucket")
	}
	buckets := make([]*bucket, bn)
	for i := range buckets {
		body, err := nextDeltaBucketBody(raw, &off)
		if err != nil {
			return nil, nil, err
		}
		if buckets[i], err = decodeDeltaBucket(body); err != nil {
			return nil, nil, err
		}
	}
	if off != len(raw) {
		return nil, nil, errors.New("strata: trailing delta")
	}
	return dicts, buckets, nil
}

func encodeReset(x resetFrame) []byte {
	b := make([]byte, 0, 42)
	b = binary.LittleEndian.AppendUint16(b, x.series)
	b = binary.LittleEndian.AppendUint64(b, x.seq)
	b = binary.LittleEndian.AppendUint64(b, x.oldEpoch)
	b = binary.LittleEndian.AppendUint64(b, x.newEpoch)
	b = binary.LittleEndian.AppendUint64(b, uint64(x.from))
	b = binary.LittleEndian.AppendUint64(b, uint64(x.to))
	return b
}

func decodeReset(raw []byte) (resetFrame, error) {
	if len(raw) != 42 {
		return resetFrame{}, errors.New("strata: invalid reset frame")
	}
	return resetFrame{
		series:   binary.LittleEndian.Uint16(raw[0:2]),
		seq:      binary.LittleEndian.Uint64(raw[2:10]),
		oldEpoch: binary.LittleEndian.Uint64(raw[10:18]),
		newEpoch: binary.LittleEndian.Uint64(raw[18:26]),
		from:     int64(binary.LittleEndian.Uint64(raw[26:34])),
		to:       int64(binary.LittleEndian.Uint64(raw[34:42])),
	}, nil
}
func frameBytes(typ byte, seq uint64, payload []byte) []byte {
	raw := make([]byte, frameHeader+len(payload))
	copy(raw[:4], walMagic[:])
	binary.LittleEndian.PutUint32(raw[4:8], uint32(len(payload)))
	raw[12] = typ
	binary.LittleEndian.PutUint64(raw[13:21], seq)
	copy(raw[frameHeader:], payload)
	binary.LittleEndian.PutUint32(raw[8:12], crc(raw[12:]))
	return raw
}
func appendFrame(f *osFile, pos int64, typ byte, seq uint64, payload []byte) (int64, error) {
	raw := frameBytes(typ, seq, payload)
	n, err := f.WriteAt(raw, pos)
	if err != nil {
		return pos, err
	}
	if n != len(raw) {
		return pos, io.ErrShortWrite
	}
	return pos + int64(n), nil
}
func decodeWALHeader(head *[frameHeader]byte, pos, limit int64) (kind uint8, seq uint64, length uint32, wantCRC uint32, ok bool) {
	if !bytes.Equal(head[:4], walMagic[:]) {
		return 0, 0, 0, 0, false
	}
	ln := binary.LittleEndian.Uint32(head[4:8])
	if ln > 256<<20 || pos+frameHeader+int64(ln) > limit {
		return 0, 0, 0, 0, false
	}
	return head[12], binary.LittleEndian.Uint64(head[13:21]), ln, binary.LittleEndian.Uint32(head[8:12]), true
}

func scanWAL(f *osFile, start int64, truncate bool, apply func(walFrame) error) (int64, uint64, error) {
	info, err := f.Stat()
	if err != nil {
		return start, 0, err
	}
	size := info.Size()
	pos := start
	var max uint64
	var head [frameHeader]byte
	for pos+frameHeader <= size {
		if _, err = f.ReadAt(head[:], pos); err != nil {
			break
		}
		typ, seq, ln, wantCRC, ok := decodeWALHeader(&head, pos, size)
		if !ok {
			break
		}
		payload := make([]byte, ln)
		if ln > 0 {
			if _, err = f.ReadAt(payload, pos+frameHeader); err != nil {
				break
			}
		}
		if crc32.Update(crc32.Update(0, castagnoli, head[12:]), castagnoli, payload) != wantCRC {
			break
		}
		fr := walFrame{typ: typ, seq: seq, payload: payload}
		if fr.seq > max {
			max = fr.seq
		}
		if err = apply(fr); err != nil {
			return pos, max, err
		}
		pos += frameHeader + int64(ln)
	}
	if truncate && pos < size {
		_ = f.f.Truncate(pos)
	}
	return pos, max, nil
}
