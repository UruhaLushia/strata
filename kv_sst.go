package strata

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"github.com/klauspost/compress/zstd"
)

type kvSSTBlock struct {
	raw         []byte
	first, last []byte
	maxRev      uint64
	entries     []kvSSTEntry
}

func physicalKVKey(region uint16, key []byte) []byte {
	out := make([]byte, 2+len(key))
	binary.BigEndian.PutUint16(out, region)
	copy(out[2:], key)
	return out
}

func encodeKVBlockWithEncoder(entries []kvSSTEntry, enc *zstd.Encoder) []byte {
	var out []byte
	var prev []byte
	for _, e := range entries {
		key := physicalKVKey(e.region, e.key)
		shared := 0
		for shared < len(prev) && shared < len(key) && prev[shared] == key[shared] {
			shared++
		}
		out = binary.AppendUvarint(out, uint64(shared))
		out = binary.AppendUvarint(out, uint64(len(key)-shared))
		valueLen := len(e.value)
		if e.external {
			valueLen = 0
		}
		out = binary.AppendUvarint(out, uint64(valueLen))
		out = binary.LittleEndian.AppendUint64(out, e.modRev)
		out = binary.LittleEndian.AppendUint64(out, uint64(e.expiresAtUnixNano))
		var flags byte
		if e.external {
			flags = 1
		}
		out = append(out, flags)
		out = append(out, key[shared:]...)
		if e.external {
			out = binary.LittleEndian.AppendUint64(out, e.blobOffset)
			out = binary.LittleEndian.AppendUint32(out, e.blobLen)
			out = binary.LittleEndian.AppendUint32(out, e.blobCRC)
		} else {
			out = append(out, e.value...)
		}
		prev = key
	}
	return enc.EncodeAll(out, nil)
}

func decodeKVBlock(raw []byte) ([]kvSSTEntry, error) {
	plain, err := zstdDecompress(raw)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(plain)
	var prev []byte
	out := make([]kvSSTEntry, 0)
	for r.Len() > 0 {
		shared, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		suffixLen, err := binary.ReadUvarint(r)
		if err != nil || shared > uint64(len(prev)) || suffixLen > uint64(r.Len()) {
			return nil, errors.New("strata: invalid KV SST key")
		}
		valueLen, err := binary.ReadUvarint(r)
		if err != nil || suffixLen > uint64(r.Len()) || uint64(r.Len()) < suffixLen+17 || valueLen > uint64(r.Len())-suffixLen-17 {
			return nil, errors.New("strata: invalid KV SST value")
		}
		var rev uint64
		if err = binary.Read(r, binary.LittleEndian, &rev); err != nil {
			return nil, err
		}
		var expiresAt int64
		if err = binary.Read(r, binary.LittleEndian, &expiresAt); err != nil {
			return nil, err
		}
		flags, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		key := append(append([]byte(nil), prev[:shared]...), make([]byte, suffixLen)...)
		if _, err = r.Read(key[shared:]); err != nil {
			return nil, err
		}
		var value []byte
		var blobOffset uint64
		var blobLen, blobCRC uint32
		external := flags&1 != 0
		if external {
			if valueLen != 0 || r.Len() < 16 {
				return nil, errors.New("strata: invalid external KV value")
			}
			if err = binary.Read(r, binary.LittleEndian, &blobOffset); err != nil {
				return nil, err
			}
			if err = binary.Read(r, binary.LittleEndian, &blobLen); err != nil {
				return nil, err
			}
			if err = binary.Read(r, binary.LittleEndian, &blobCRC); err != nil {
				return nil, err
			}
		} else {
			value = make([]byte, valueLen)
			if len(value) > 0 {
				if _, err = r.Read(value); err != nil {
					return nil, err
				}
			}
		}
		if len(key) < 2 {
			return nil, errors.New("strata: invalid KV SST region")
		}
		out = append(out, kvSSTEntry{
			region: binary.BigEndian.Uint16(key), key: append([]byte(nil), key[2:]...),
			value: value, modRev: rev, expiresAtUnixNano: expiresAt,
			blobOffset: blobOffset, blobLen: blobLen, blobCRC: blobCRC, external: external,
		})
		prev = key
	}
	return out, nil
}

func (db *DB) loadKVIndex() ([]byte, error) {
	db.kvSST = nil
	d := db.head.sections[2]
	if d.length == 0 {
		return nil, nil
	}
	raw, err := sectionRaw(db.data, d, db.mappedBytes())
	if err != nil {
		return nil, err
	}
	if crc(raw) != d.crc {
		return nil, errors.New("strata: KV index CRC mismatch")
	}
	r := bytes.NewReader(raw)
	catalogLen, err := binary.ReadUvarint(r)
	if err != nil || catalogLen > uint64(r.Len()) {
		return nil, errors.New("strata: invalid KV catalog index")
	}
	catalog := make([]byte, catalogLen)
	if _, err = r.Read(catalog); err != nil {
		return nil, err
	}
	for r.Len() > 0 {
		firstLen, err := binary.ReadUvarint(r)
		if err != nil || firstLen > uint64(r.Len()) {
			return nil, errors.New("strata: invalid KV index")
		}
		first := make([]byte, firstLen)
		if _, err = r.Read(first); err != nil {
			return nil, err
		}
		lastLen, err := binary.ReadUvarint(r)
		if err != nil || lastLen > uint64(r.Len()) {
			return nil, errors.New("strata: invalid KV index")
		}
		last := make([]byte, lastLen)
		if _, err = r.Read(last); err != nil {
			return nil, err
		}
		var m kvSSTMeta
		m.first, m.last = first, last
		if err = binary.Read(r, binary.LittleEndian, &m.maxRev); err != nil {
			return nil, err
		}
		if err = binary.Read(r, binary.LittleEndian, &m.offset); err != nil {
			return nil, err
		}
		if err = binary.Read(r, binary.LittleEndian, &m.length); err != nil {
			return nil, err
		}
		if err = binary.Read(r, binary.LittleEndian, &m.rawLen); err != nil {
			return nil, err
		}
		if err = binary.Read(r, binary.LittleEndian, &m.crc); err != nil {
			return nil, err
		}
		db.kvSST = append(db.kvSST, m)
	}
	return catalog, nil
}

func readKVBlockFrom(data *osFile, mapped []byte, sec sectionDesc, m kvSSTMeta) ([]kvSSTEntry, error) {
	raw, err := sectionBlock(data, sec, m.offset, m.length, mapped)
	if err != nil {
		return nil, err
	}
	if crc(raw) != m.crc {
		return nil, errors.New("strata: KV block CRC mismatch")
	}
	return decodeKVBlock(raw)
}

func readKVBlobFrom(data *osFile, mapped []byte, sec sectionDesc, e kvSSTEntry) ([]byte, error) {
	if sec.length == 0 || !e.external || e.blobLen == 0 {
		return nil, errors.New("strata: invalid KV blob pointer")
	}
	raw, err := sectionBlock(data, sec, e.blobOffset, e.blobLen, mapped)
	if err != nil {
		return nil, err
	}
	if crc(raw) != e.blobCRC {
		return nil, errors.New("strata: KV blob CRC mismatch")
	}
	return zstdDecompress(raw)
}

func readKVBlockFromView(view *fileView, sec sectionDesc, m kvSSTMeta) ([]kvSSTEntry, error) {
	if view == nil || view.data == nil {
		return nil, errors.New("strata: database is closed")
	}
	return readKVBlockFrom(view.data, view.mappedBytes(), sec, m)
}

func readKVBlobFromView(view *fileView, sec sectionDesc, e kvSSTEntry) ([]byte, error) {
	if view == nil || view.data == nil {
		return nil, errors.New("strata: database is closed")
	}
	return readKVBlobFrom(view.data, view.mappedBytes(), sec, e)
}

func (db *DB) readKVBlock(m kvSSTMeta) ([]kvSSTEntry, error) {
	return readKVBlockFrom(db.data, db.mappedBytes(), db.head.sections[0], m)
}

func (db *DB) readKVBlob(e kvSSTEntry) ([]byte, error) {
	return readKVBlobFrom(db.data, db.mappedBytes(), db.head.sections[1], e)
}

func kvExpired(expiresAt, now int64) bool {
	return expiresAt != 0 && expiresAt <= now
}

func (db *DB) getKVLocked(region uint16, key string) ([]byte, bool, error) {
	now := db.opts.Now().UTC().UnixNano()
	clearRev := uint64(0)
	if r := db.kvRegions[region]; r != nil {
		clearRev = r.clearRev
	}
	if e, ok := db.kvOverlay[kvKey{region: region, key: key}]; ok {
		if e.modRev > clearRev {
			if e.deleted || kvExpired(e.expiresAtUnixNano, now) {
				return nil, false, nil
			}
			return append([]byte(nil), e.value...), true, nil
		}
	}
	physical := physicalKVKey(region, []byte(key))
	i := sort.Search(len(db.kvSST), func(i int) bool {
		return bytes.Compare(db.kvSST[i].last, physical) >= 0
	})
	if i < len(db.kvSST) {
		meta := db.kvSST[i]
		if bytes.Compare(physical, meta.first) >= 0 {
			entries, err := db.readKVBlock(meta)
			if err != nil {
				return nil, false, err
			}
			for _, e := range entries {
				if e.region == region && string(e.key) == key && e.modRev > clearRev && !e.deleted && !kvExpired(e.expiresAtUnixNano, now) {
					if e.external {
						value, readErr := db.readKVBlob(e)
						if readErr != nil {
							return nil, false, readErr
						}
						return value, true, nil
					}
					return append([]byte(nil), e.value...), true, nil
				}
			}
		}
	}
	return nil, false, nil
}
