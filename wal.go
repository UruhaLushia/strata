package strata

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

var walMagic = [4]byte{'U', 'D', 'W', '1'}

const (
	frameDelta   byte = 1
	frameKVBatch byte = 2
	frameReset   byte = 3
	frameHeader       = 21
)

type walFrame struct {
	typ     byte
	seq     uint64
	payload []byte
	raw     []byte
}

func encodeDelta(dicts []dictEntry, buckets []*bucket) []byte {
	var b []byte
	b = binary.AppendUvarint(b, uint64(len(dicts)))
	for _, d := range dicts {
		b = binary.LittleEndian.AppendUint32(b, d.id)
		b = binary.LittleEndian.AppendUint16(b, d.series)
		b = append(b, d.dim)
		b = binary.AppendUvarint(b, uint64(len(d.value)))
		b = append(b, d.value...)
	}
	b = binary.AppendUvarint(b, uint64(len(buckets)))
	for _, x := range buckets {
		b = binary.LittleEndian.AppendUint16(b, x.series)
		b = binary.LittleEndian.AppendUint64(b, uint64(x.ts))
		b = binary.LittleEndian.AppendUint64(b, x.epoch)
		b = binary.LittleEndian.AppendUint64(b, x.order)
		b = append(b, x.arity)
		for _, op := range x.summaryOps {
			b = append(b, byte(op))
		}
		for _, op := range x.rowOps {
			b = append(b, byte(op))
		}
		b = append(b, x.summary.Mask)
		for _, v := range x.summary.Values {
			b = binary.LittleEndian.AppendUint64(b, uint64(v))
		}
		b = binary.AppendUvarint(b, uint64(len(x.rows)))
		for _, r := range x.rows {
			for _, id := range r.tags {
				b = binary.LittleEndian.AppendUint32(b, id)
			}
			for _, v := range r.values {
				b = binary.LittleEndian.AppendUint64(b, uint64(v))
			}
			b = append(b, r.initMask)
		}
	}
	return b
}
func decodeDelta(raw []byte) ([]dictEntry, []*bucket, error) {
	r := bytes.NewReader(raw)
	n, err := readUvarint(r)
	if err != nil || n > uint64(r.Len())/8 {
		return nil, nil, errors.New("strata: invalid delta dictionary")
	}
	dicts := make([]dictEntry, n)
	for i := range dicts {
		if err = binary.Read(r, binary.LittleEndian, &dicts[i].id); err != nil {
			return nil, nil, err
		}
		if err = binary.Read(r, binary.LittleEndian, &dicts[i].series); err != nil {
			return nil, nil, err
		}
		dicts[i].dim, err = r.ReadByte()
		if err != nil {
			return nil, nil, err
		}
		ln, err := readUvarint(r)
		if err != nil || ln > uint64(r.Len()) {
			return nil, nil, errors.New("strata: invalid delta dictionary")
		}
		v := make([]byte, ln)
		if _, err = io.ReadFull(r, v); err != nil {
			return nil, nil, err
		}
		dicts[i].value = string(v)
	}
	n, err = readUvarint(r)
	if err != nil || n > uint64(r.Len())/(walBucketHeaderFixed+1) {
		return nil, nil, errors.New("strata: invalid delta bucket")
	}
	buckets := make([]*bucket, n)
	for i := range buckets {
		x := &bucket{}
		if err = binary.Read(r, binary.LittleEndian, &x.series); err != nil {
			return nil, nil, err
		}
		if err = binary.Read(r, binary.LittleEndian, &x.ts); err != nil {
			return nil, nil, err
		}
		if err = binary.Read(r, binary.LittleEndian, &x.epoch); err != nil {
			return nil, nil, err
		}
		if err = binary.Read(r, binary.LittleEndian, &x.order); err != nil {
			return nil, nil, err
		}
		x.arity, err = r.ReadByte()
		if err != nil {
			return nil, nil, err
		}
		for j := range x.summaryOps {
			op, readErr := r.ReadByte()
			if readErr != nil {
				return nil, nil, readErr
			}
			x.summaryOps[j] = MetricOp(op)
		}
		for j := range x.rowOps {
			op, readErr := r.ReadByte()
			if readErr != nil {
				return nil, nil, readErr
			}
			x.rowOps[j] = MetricOp(op)
		}
		x.summary.Mask, err = r.ReadByte()
		if err != nil {
			return nil, nil, err
		}
		for j := range x.summary.Values {
			if err = binary.Read(r, binary.LittleEndian, &x.summary.Values[j]); err != nil {
				return nil, nil, err
			}
		}
		rn, err := readUvarint(r)
		if err != nil || rn > uint64(r.Len())/walRowEncodedBytes {
			return nil, nil, errors.New("strata: invalid delta rows")
		}
		x.rows = make([]idRow, rn)
		for j := range x.rows {
			for k := range x.rows[j].tags {
				if err = binary.Read(r, binary.LittleEndian, &x.rows[j].tags[k]); err != nil {
					return nil, nil, err
				}
			}
			for k := range x.rows[j].values {
				if err = binary.Read(r, binary.LittleEndian, &x.rows[j].values[k]); err != nil {
					return nil, nil, err
				}
			}
			x.rows[j].initMask, err = r.ReadByte()
			if err != nil {
				return nil, nil, err
			}
		}
		buckets[i] = x
	}
	if r.Len() != 0 {
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
	var x resetFrame
	r := bytes.NewReader(raw)
	err := binary.Read(r, binary.LittleEndian, &x.series)
	if err == nil {
		err = binary.Read(r, binary.LittleEndian, &x.seq)
	}
	if err == nil {
		err = binary.Read(r, binary.LittleEndian, &x.oldEpoch)
	}
	if err == nil {
		err = binary.Read(r, binary.LittleEndian, &x.newEpoch)
	}
	if err == nil {
		err = binary.Read(r, binary.LittleEndian, &x.from)
	}
	if err == nil {
		err = binary.Read(r, binary.LittleEndian, &x.to)
	}
	if err == nil && r.Len() != 0 {
		err = errors.New("strata: trailing reset")
	}
	return x, err
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
func scanWAL(f *osFile, start int64, truncate bool, apply func(walFrame) error) (int64, uint64, error) {
	info, err := f.Stat()
	if err != nil {
		return start, 0, err
	}
	size := info.Size()
	pos := start
	var max uint64
	for pos+frameHeader <= size {
		head := make([]byte, frameHeader)
		if _, err = f.ReadAt(head, pos); err != nil {
			break
		}
		if !bytes.Equal(head[:4], walMagic[:]) {
			break
		}
		ln := binary.LittleEndian.Uint32(head[4:8])
		if ln > 256<<20 || pos+frameHeader+int64(ln) > size {
			break
		}
		payload := make([]byte, ln)
		if _, err = f.ReadAt(payload, pos+frameHeader); err != nil {
			break
		}
		check := append(append([]byte(nil), head[12:]...), payload...)
		if crc(check) != binary.LittleEndian.Uint32(head[8:12]) {
			break
		}
		fr := walFrame{typ: head[12], seq: binary.LittleEndian.Uint64(head[13:21]), payload: payload, raw: append(head, payload...)}
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
