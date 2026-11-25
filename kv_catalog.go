package strata

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
	"time"
)

type kvRegion struct {
	id       uint16
	name     string
	clearRev uint64
}
type kvKey struct {
	region uint16
	key    string
}
type kvValue struct {
	value             []byte
	modRev            uint64
	expiresAtUnixNano int64
	deleted           bool
}
type kvOp struct {
	kind              byte
	region            uint16
	key               []byte
	value             []byte
	name              string
	expiresAtUnixNano int64
	ttl               time.Duration
	hasTTL            bool
}
type kvSSTMeta struct {
	first, last []byte
	maxRev      uint64
	offset      uint64
	length      uint32
	crc         uint32
}
type kvSSTEntry struct {
	region            uint16
	key               []byte
	value             []byte
	modRev            uint64
	expiresAtUnixNano int64
	blobOffset        uint64
	blobLen           uint32
	blobCRC           uint32
	external          bool
	deleted           bool
}

const (
	kvPut byte = iota + 1
	kvDelete
	kvClear
)

func encodeKVCatalog(db *DB, maxRev uint64) []byte {
	ids := make([]int, 0, len(db.kvRegions))
	for id := range db.kvRegions {
		if id != 0 {
			ids = append(ids, int(id))
		}
	}
	sort.Ints(ids)
	out := binary.AppendUvarint(nil, uint64(len(ids)))
	for _, x := range ids {
		r := db.kvRegions[uint16(x)]
		out = binary.LittleEndian.AppendUint16(out, r.id)
		clearRev := min(r.clearRev, maxRev)
		out = binary.LittleEndian.AppendUint64(out, clearRev)
		out = binary.AppendUvarint(out, uint64(len(r.name)))
		out = append(out, r.name...)
	}
	return out
}

func decodeKVCatalog(raw []byte, db *DB) error {
	r := bytes.NewReader(raw)
	n, err := binary.ReadUvarint(r)
	if err != nil || n > 65535 {
		return errors.New("strata: invalid KV catalog")
	}
	for range n {
		var id uint16
		if err = binary.Read(r, binary.LittleEndian, &id); err != nil {
			return err
		}
		if id == 0 {
			return ErrInvalidRegion
		}
		var clearRev uint64
		if err = binary.Read(r, binary.LittleEndian, &clearRev); err != nil {
			return err
		}
		nameLen, err := binary.ReadUvarint(r)
		if err != nil || nameLen > uint64(r.Len()) {
			return errors.New("strata: invalid KV region name")
		}
		name := make([]byte, nameLen)
		if len(name) > 0 {
			if _, err = r.Read(name); err != nil {
				return err
			}
		}
		region := &kvRegion{id: id, name: string(name), clearRev: clearRev}
		db.kvRegions[id] = region
		if region.name != "" {
			db.kvCatalog[region.name] = id
		}
		if id >= 0x100 && id <= 0x7fff && id >= db.kvNext {
			db.kvNext = id + 1
		}
	}
	if r.Len() != 0 {
		return errors.New("strata: trailing KV catalog")
	}
	return nil
}
