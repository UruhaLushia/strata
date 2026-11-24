package strata

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

func encodeKVBatch(rev uint64, ops []kvOp) []byte {
	out := binary.LittleEndian.AppendUint64(nil, rev)
	out = binary.AppendUvarint(out, uint64(len(ops)))
	for _, op := range ops {
		out = append(out, op.kind)
		out = binary.LittleEndian.AppendUint16(out, op.region)
		out = binary.LittleEndian.AppendUint64(out, uint64(op.expiresAtUnixNano))
		out = binary.AppendUvarint(out, uint64(len(op.key)))
		out = binary.AppendUvarint(out, uint64(len(op.value)))
		out = append(out, op.key...)
		out = append(out, op.value...)
	}
	return out
}

func decodeKVBatch(raw []byte) (uint64, []kvOp, error) {
	if len(raw) < 8 {
		return 0, nil, errors.New("strata: invalid KV batch")
	}
	rev := binary.LittleEndian.Uint64(raw)
	r := bytes.NewReader(raw[8:])
	n, err := binary.ReadUvarint(r)
	if err != nil || n > 1<<20 || n > uint64(r.Len())/13 {
		return 0, nil, errors.New("strata: invalid KV operation count")
	}
	ops := make([]kvOp, n)
	for i := range ops {
		kind, err := r.ReadByte()
		if err != nil || kind < kvPut || kind > kvClear {
			return 0, nil, errors.New("strata: invalid KV operation")
		}
		var id uint16
		if err = binary.Read(r, binary.LittleEndian, &id); err != nil {
			return 0, nil, err
		}
		var expiresAt int64
		if err = binary.Read(r, binary.LittleEndian, &expiresAt); err != nil {
			return 0, nil, err
		}
		kn, err := binary.ReadUvarint(r)
		if err != nil || kn > uint64(r.Len()) {
			return 0, nil, errors.New("strata: invalid KV key")
		}
		vn, err := binary.ReadUvarint(r)
		if err != nil || kn > uint64(r.Len()) || vn > uint64(r.Len())-kn {
			return 0, nil, errors.New("strata: invalid KV value")
		}
		ops[i] = kvOp{kind: kind, region: id, key: make([]byte, kn), value: make([]byte, vn), expiresAtUnixNano: expiresAt}
		if len(ops[i].key) > 0 {
			if _, err = io.ReadFull(r, ops[i].key); err != nil {
				return 0, nil, err
			}
		}
		if len(ops[i].value) > 0 {
			if _, err = io.ReadFull(r, ops[i].value); err != nil {
				return 0, nil, err
			}
		}
	}
	if r.Len() != 0 {
		return 0, nil, errors.New("strata: trailing KV batch")
	}
	return rev, ops, nil
}
