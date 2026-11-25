package strata

import (
	"slices"
	"sort"
)

type compiledFilter struct {
	dim      int
	negate   bool
	ids      []uint32
	single   uint32
	small    [8]uint32
	smallLen uint8
	bits     []uint64
}

func compileFiltersSnap(dict dictionary, series uint16, arity int, filters []Filter) []compiledFilter {
	out := make([]compiledFilter, len(filters))
	for i, filter := range filters {
		var ids []uint32
		if filter.Dim >= 0 && filter.Dim < arity {
			ids = matchingTagIDsSnap(dict, series, filter)
		}
		compiled := compiledFilter{dim: filter.Dim, negate: filter.Negate, ids: ids}
		switch {
		case len(ids) == 1:
			compiled.single = ids[0]
		case len(ids) <= len(compiled.small):
			compiled.smallLen = uint8(len(ids))
			copy(compiled.small[:], ids)
		default:
			var maxID uint32
			for _, id := range ids {
				if id > maxID {
					maxID = id
				}
			}
			compiled.bits = make([]uint64, maxID/64+1)
			for _, id := range ids {
				compiled.bits[id/64] |= uint64(1) << (id & 63)
			}
		}
		out[i] = compiled
	}
	return out
}

func orderCompiledFilters(filters []Filter, compiled []compiledFilter) ([]Filter, []compiledFilter) {
	type pair struct {
		filter   Filter
		compiled compiledFilter
	}
	pairs := make([]pair, len(filters))
	for i := range filters {
		pairs[i] = pair{filter: filters[i], compiled: compiled[i]}
	}
	sort.SliceStable(pairs, func(i, j int) bool {
		if pairs[i].filter.Negate != pairs[j].filter.Negate {
			return !pairs[i].filter.Negate
		}
		return len(pairs[i].compiled.ids) < len(pairs[j].compiled.ids)
	})
	orderedFilters := make([]Filter, len(pairs))
	orderedCompiled := make([]compiledFilter, len(pairs))
	for i, pair := range pairs {
		orderedFilters[i], orderedCompiled[i] = pair.filter, pair.compiled
	}
	return orderedFilters, orderedCompiled
}

func matchesCompiledRow(row idRow, filters []compiledFilter) bool {
	for _, filter := range filters {
		matched := false
		if filter.dim >= 0 && filter.dim < len(row.tags) {
			id := row.tags[filter.dim]
			switch {
			case len(filter.ids) == 0:
			case len(filter.ids) == 1:
				matched = id == filter.single
			case filter.smallLen > 0:
				if slices.Contains(filter.small[:filter.smallLen], id) {
					matched = true
				}
			case id/64 < uint32(len(filter.bits)):
				matched = filter.bits[id/64]&(uint64(1)<<(id&63)) != 0
			}
		}
		if filter.negate {
			if matched {
				return false
			}
		} else if !matched {
			return false
		}
	}
	return true
}
