package strata

import (
	"encoding/binary"
	"errors"
	"slices"
	"sort"
	"strings"
)

func readDictUvarint(raw []byte, off *int) (uint64, error) {
	if *off >= len(raw) {
		return 0, errors.New("strata: truncated dictionary")
	}
	value, n := binary.Uvarint(raw[*off:])
	if n <= 0 {
		return 0, errors.New("strata: invalid dictionary varint")
	}
	*off += n
	return value, nil
}

func readDictUint16(raw []byte, off *int) (uint16, error) {
	if *off > len(raw)-2 {
		return 0, errors.New("strata: truncated dictionary")
	}
	value := binary.LittleEndian.Uint16(raw[*off:])
	*off += 2
	return value, nil
}

func readDictUint32(raw []byte, off *int) (uint32, error) {
	if *off > len(raw)-4 {
		return 0, errors.New("strata: truncated dictionary")
	}
	value := binary.LittleEndian.Uint32(raw[*off:])
	*off += 4
	return value, nil
}

func readDictUint64(raw []byte, off *int) (uint64, error) {
	if *off > len(raw)-8 {
		return 0, errors.New("strata: truncated dictionary")
	}
	value := binary.LittleEndian.Uint64(raw[*off:])
	*off += 8
	return value, nil
}

func readDictInt64(raw []byte, off *int) (int64, error) {
	value, err := readDictUint64(raw, off)
	return int64(value), err
}

func (d *dictionary) matchPrefixIDs(series uint16, dimIdx uint8, prefix string, dst []uint32) []uint32 {
	dim := d.dims[dictDimKey{series: series, dim: dimIdx}]
	if dim == nil || dim.count == 0 {
		return dst
	}
	if dim.ext != nil {
		dst = dim.ext.matchPrefixSST(prefix, dim.extra, dst)
		for v, id := range dim.tailByKey {
			if strings.HasPrefix(v, prefix) {
				dst = append(dst, id)
			}
		}
		for id, v := range dim.extra {
			if strings.HasPrefix(v, prefix) {
				dst = append(dst, id)
			}
		}
		return dst
	}
	dim.revMu.RLock()
	if dim.sorted != nil {
		idx := sort.Search(len(dim.sorted), func(i int) bool {
			return dim.resolve(dim.sorted[i]) >= prefix
		})
		for i := idx; i < len(dim.sorted); i++ {
			id := dim.sorted[i]
			if !strings.HasPrefix(dim.resolve(id), prefix) {
				break
			}
			dst = append(dst, id)
		}
		for v, id := range dim.pending {
			if strings.HasPrefix(v, prefix) {
				dst = append(dst, id)
			}
		}
		dim.revMu.RUnlock()
		return dst
	}
	dim.revMu.RUnlock()
	for id := uint32(1); id < dim.nextID; id++ {
		if v := dim.resolve(id); v != "" && strings.HasPrefix(v, prefix) {
			dst = append(dst, id)
		}
	}
	return dst
}

func (d *dictionary) matchScanIDs(series uint16, dimIdx uint8, op TagOp, target string, dst []uint32) []uint32 {
	dim := d.dims[dictDimKey{series: series, dim: dimIdx}]
	if dim == nil || dim.count == 0 {
		return dst
	}
	if dim.ext != nil {
		dst = dim.ext.matchScanFwd(op, target, dim.extra, dst)
		for id := dim.tailBase; id < dim.nextID; id++ {
			if v := dim.resolve(id); v != "" && tagMatches(op, v, target) {
				dst = append(dst, id)
			}
		}
		for id, v := range dim.extra {
			if id < dim.tailBase && v != "" && tagMatches(op, v, target) {
				dst = append(dst, id)
			}
		}
		return dst
	}
	for id := uint32(1); id < dim.nextID; id++ {
		if v := dim.resolve(id); v != "" && tagMatches(op, v, target) {
			dst = append(dst, id)
		}
	}
	return dst
}

func dedupeUint32(ids []uint32) []uint32 {
	if len(ids) <= 1 {
		return ids
	}
	slices.Sort(ids)
	out := ids[:0]
	for _, id := range ids {
		if len(out) == 0 || out[len(out)-1] != id {
			out = append(out, id)
		}
	}
	return out
}

func (dim *dictDim) buildSorted() {
	dim.byKey = nil
	ids := make([]uint32, 0, dim.count)
	for id := uint32(1); id < dim.nextID; id++ {
		if dim.resolve(id) != "" {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		return dim.resolve(ids[i]) < dim.resolve(ids[j])
	})
	dim.sorted = ids
	if dim.pending == nil {
		dim.pending = make(map[string]uint32)
	} else {
		clear(dim.pending)
	}
}

func (dim *dictDim) flushPendingIntoSorted() {
	if len(dim.pending) == 0 {
		return
	}
	added := make([]uint32, 0, len(dim.pending))
	for _, id := range dim.pending {
		added = append(added, id)
	}
	sort.Slice(added, func(i, j int) bool {
		return dim.resolve(added[i]) < dim.resolve(added[j])
	})
	merged := make([]uint32, 0, len(dim.sorted)+len(added))
	i, j := 0, 0
	for i < len(dim.sorted) && j < len(added) {
		if dim.resolve(added[j]) < dim.resolve(dim.sorted[i]) {
			merged = append(merged, added[j])
			j++
		} else {
			merged = append(merged, dim.sorted[i])
			i++
		}
	}
	merged = append(merged, dim.sorted[i:]...)
	merged = append(merged, added[j:]...)
	dim.sorted = merged
	clear(dim.pending)
}
