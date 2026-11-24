package strata

import (
	"bytes"
	"sort"
)

func (db *DB) kvEntriesLocked(maxRev uint64) (map[string]kvSSTEntry, error) {
	out := make(map[string]kvSSTEntry)
	now := db.opts.Now().UTC().UnixNano()
	for _, meta := range db.kvSST {
		entries, err := db.readKVBlock(meta)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.modRev <= maxRev {
				if kvExpired(e.expiresAtUnixNano, now) {
					continue
				}
				if e.external {
					e.value, err = db.readKVBlob(e)
					if err != nil {
						return nil, err
					}
					e.external = false
				}
				out[string(physicalKVKey(e.region, e.key))] = e
			}
		}
	}
	for k, e := range db.kvOverlay {
		if e.modRev <= maxRev {
			physical := string(physicalKVKey(k.region, []byte(k.key)))
			if kvExpired(e.expiresAtUnixNano, now) {
				out[physical] = kvSSTEntry{region: k.region, key: []byte(k.key), modRev: e.modRev, deleted: true}
				continue
			}
			out[physical] = kvSSTEntry{
				region: k.region, key: []byte(k.key), value: append([]byte(nil), e.value...),
				modRev: e.modRev, expiresAtUnixNano: e.expiresAtUnixNano, deleted: e.deleted,
			}
		}
	}
	for k, e := range out {
		clearRev := uint64(0)
		if r := db.kvRegions[e.region]; r != nil && r.clearRev <= maxRev {
			clearRev = r.clearRev
		}
		if e.modRev <= clearRev || e.deleted {
			delete(out, k)
		}
	}
	return out, nil
}

func (db *DB) buildKVBlocks(maxRev uint64) ([]kvSSTBlock, error) {
	entries, err := db.kvEntriesLocked(maxRev)
	if err != nil {
		return nil, err
	}
	list := make([]kvSSTEntry, 0, len(entries))
	for _, e := range entries {
		list = append(list, e)
	}
	sort.Slice(list, func(i, j int) bool {
		return bytes.Compare(physicalKVKey(list[i].region, list[i].key), physicalKVKey(list[j].region, list[j].key)) < 0
	})
	var out []kvSSTBlock
	for len(list) > 0 {
		n, rawBytes := 0, 0
		for n < len(list) && (n == 0 || rawBytes < 16<<10) {
			rawBytes += 16 + len(list[n].key) + len(list[n].value)
			n++
		}
		first := physicalKVKey(list[0].region, list[0].key)
		last := physicalKVKey(list[n-1].region, list[n-1].key)
		var max uint64
		for _, e := range list[:n] {
			if e.modRev > max {
				max = e.modRev
			}
		}
		out = append(out, kvSSTBlock{
			first: first, last: last, maxRev: max,
			entries: append([]kvSSTEntry(nil), list[:n]...),
		})
		list = list[n:]
	}
	return out, nil
}
