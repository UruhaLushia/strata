package strata

import (
	"cmp"
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

func (s *dictDimSnap) lookup(value string) uint32 {
	if (s.count == 0 && len(s.extra) == 0) || value == "" {
		return 0
	}
	for id, v := range s.extra {
		if v == value {
			return id
		}
	}
	if s.ext != nil {
		for id := s.tailBase; id < s.nextID; id++ {
			if hasExtraOverride(s.extra, id) {
				continue
			}
			if v := s.resolveArena(id); len(v) == len(value) && v == value {
				return id
			}
		}
		if id := s.ext.lookupSST(value); id != 0 && !hasExtraOverride(s.extra, id) {
			return id
		}
		return 0
	}
	if s.sorted != nil {
		idx := sort.Search(len(s.sorted), func(i int) bool {
			return s.resolveArena(s.sorted[i]) >= value
		})
		for i := idx; i < len(s.sorted); i++ {
			id := s.sorted[i]
			if s.resolveArena(id) != value {
				break
			}
			if hasExtraOverride(s.extra, id) {
				continue
			}
			return id
		}
		startID := max(s.sortedNextID, uint32(1))
		for id := startID; id < s.nextID; id++ {
			if hasExtraOverride(s.extra, id) {
				continue
			}
			if v := s.resolveArena(id); len(v) == len(value) && v == value {
				return id
			}
		}
		return 0
	}
	for id := uint32(1); id < s.nextID; id++ {
		if hasExtraOverride(s.extra, id) {
			continue
		}
		if v := s.resolveArena(id); len(v) == len(value) && v == value {
			return id
		}
	}
	return 0
}

func (s *dictDimSnap) matchPrefixIDs(prefix string, dst []uint32) []uint32 {
	if s.count == 0 && len(s.extra) == 0 {
		return dst
	}
	if s.ext != nil {
		dst = s.ext.matchPrefixSST(prefix, s.extra, dst)
		for id := s.tailBase; id < s.nextID; id++ {
			if hasExtraOverride(s.extra, id) {
				continue
			}
			if v := s.resolveArena(id); v != "" && strings.HasPrefix(v, prefix) {
				dst = append(dst, id)
			}
		}
		for id, v := range s.extra {
			if v != "" && strings.HasPrefix(v, prefix) {
				dst = append(dst, id)
			}
		}
		return dst
	}
	if s.sorted != nil {
		idx := sort.Search(len(s.sorted), func(i int) bool {
			return s.resolveArena(s.sorted[i]) >= prefix
		})
		for i := idx; i < len(s.sorted); i++ {
			id := s.sorted[i]
			if !strings.HasPrefix(s.resolveArena(id), prefix) {
				break
			}
			if hasExtraOverride(s.extra, id) {
				continue
			}
			dst = append(dst, id)
		}
		startID := max(s.sortedNextID, uint32(1))
		for id := startID; id < s.nextID; id++ {
			if hasExtraOverride(s.extra, id) {
				continue
			}
			if v := s.resolveArena(id); v != "" && strings.HasPrefix(v, prefix) {
				dst = append(dst, id)
			}
		}
		for id, v := range s.extra {
			if v != "" && strings.HasPrefix(v, prefix) {
				dst = append(dst, id)
			}
		}
		return dst
	}
	for id := uint32(1); id < s.nextID; id++ {
		if v := s.resolve(id); v != "" && strings.HasPrefix(v, prefix) {
			dst = append(dst, id)
		}
	}
	return dst
}

func (s *dictDimSnap) matchScanIDs(op TagOp, target string, dst []uint32) []uint32 {
	if s.count == 0 && len(s.extra) == 0 {
		return dst
	}
	if s.ext != nil {
		dst = s.ext.matchScanFwd(op, target, s.extra, dst)
		for id := s.tailBase; id < s.nextID; id++ {
			if hasExtraOverride(s.extra, id) {
				continue
			}
			if v := s.resolveArena(id); v != "" && tagMatches(op, v, target) {
				dst = append(dst, id)
			}
		}
		for id, v := range s.extra {
			if v != "" && tagMatches(op, v, target) {
				dst = append(dst, id)
			}
		}
		return dst
	}
	for id := uint32(1); id < s.nextID; id++ {
		if v := s.resolve(id); v != "" && tagMatches(op, v, target) {
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
		if dim.resolveArena(id) != "" {
			ids = append(ids, id)
		}
	}
	slices.SortFunc(ids, func(a, b uint32) int {
		return cmp.Compare(dim.resolveArena(a), dim.resolveArena(b))
	})
	dim.sorted = ids
	dim.sortedNextID = dim.nextID
}

func (dim *dictDim) mergeTailIntoSorted() {
	if dim.sorted == nil || dim.nextID <= dim.sortedNextID {
		return
	}
	startID := max(dim.sortedNextID, uint32(1))
	added := make([]uint32, 0, int(dim.nextID-startID))
	for id := startID; id < dim.nextID; id++ {
		if dim.resolveArena(id) != "" {
			added = append(added, id)
		}
	}
	if len(added) == 0 {
		dim.sortedNextID = dim.nextID
		return
	}
	slices.SortFunc(added, func(a, b uint32) int {
		return cmp.Compare(dim.resolveArena(a), dim.resolveArena(b))
	})
	merged := make([]uint32, 0, len(dim.sorted)+len(added))
	i := 0
	for _, id := range added {
		val := dim.resolveArena(id)
		pos := sort.Search(len(dim.sorted)-i, func(k int) bool {
			return dim.resolveArena(dim.sorted[i+k]) >= val
		})
		merged = append(merged, dim.sorted[i:i+pos]...)
		merged = append(merged, id)
		i += pos
	}
	merged = append(merged, dim.sorted[i:]...)
	dim.sorted = merged
	dim.sortedNextID = dim.nextID
}
