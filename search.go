package strata

import (
	"math/bits"
	"net/netip"
	"strings"
)

type TagOp uint8

const (
	TagEq TagOp = iota
	TagPrefix
	TagSuffix
	TagContains
	TagCIDR
)

type Filter struct {
	Dim    int
	Op     TagOp
	Values []string
	Negate bool
}

type blockBitmap struct {
	bits  []uint64
	count int
}

func newBlockBitmap(nBlocks int) *blockBitmap {
	if nBlocks <= 0 {
		return &blockBitmap{}
	}
	return &blockBitmap{bits: make([]uint64, (nBlocks+63)>>6)}
}

func (b *blockBitmap) set(pos uint32) {
	w := pos >> 6
	if int(w) >= len(b.bits) {
		return
	}
	bit := uint64(1) << (pos & 63)
	if b.bits[w]&bit == 0 {
		b.bits[w] |= bit
		b.count++
	}
}

func (b *blockBitmap) intersect(other *blockBitmap) {
	n := min(len(b.bits), len(other.bits))
	cnt := 0
	for i := range n {
		w := b.bits[i] & other.bits[i]
		b.bits[i] = w
		cnt += bits.OnesCount64(w)
	}
	for i := n; i < len(b.bits); i++ {
		b.bits[i] = 0
	}
	b.count = cnt
}

func searchBounds(db *DB, series uint16) (int64, int64) {
	var minTS, maxTS int64
	for key, indexes := range db.sealedByRes {
		if key.series != series || len(indexes) == 0 {
			continue
		}
		first := db.sealed[indexes[0]]
		last := db.sealed[indexes[len(indexes)-1]]
		if minTS == 0 || first.start < minTS {
			minTS = first.start
		}
		if end := last.end(); end > maxTS {
			maxTS = end
		}
	}
	for k, b := range db.live {
		if k.series != series {
			continue
		}
		if minTS == 0 || b.ts < minTS {
			minTS = b.ts
		}
		if b.ts+60 > maxTS {
			maxTS = b.ts + 60
		}
	}
	return minTS, maxTS
}

func (db *DB) seriesSealedCount(series uint16) int {
	n := 0
	for key, indexes := range db.sealedByRes {
		if key.series == series {
			n += len(indexes)
		}
	}
	return n
}

func (db *DB) searchCandidatesCompiled(series uint16, filters []Filter, compiled []compiledFilter, slot *querySlot) (*blockBitmap, uint64, bool, bool, error) {
	var sealed *blockBitmap
	var liveMask uint64
	useLiveMask := !db.livePostingsOverflow && !db.policy.constrained && len(db.live) > 0
	initializedSealed := false
	initializedLive := false
	seriesBlocks := db.seriesSealedCount(series)

	for i, f := range filters {
		if f.Negate {
			continue
		}
		ids := compiled[i].ids
		if len(ids) == 0 {
			return nil, 0, false, true, nil
		}
		totalTags := db.dict.dimCount(series, uint8(f.Dim))
		if totalTags > 0 && len(ids)*5 >= totalTags*4 {
			continue
		}
		unionBits, denseSealed, err := db.unionInvertBitmap(series, uint8(f.Dim), ids, seriesBlocks, len(db.sealed), slot)
		if err != nil {
			return nil, 0, false, false, err
		}
		if !denseSealed {
			if !initializedSealed {
				sealed = unionBits
				initializedSealed = true
			} else {
				sealed.intersect(unionBits)
			}
		}
		if useLiveMask {
			dk := dictDimKey{series: series, dim: uint8(f.Dim)}
			if db.liveSkipDims[dk] {
				useLiveMask = false
			} else {
				var unionLive uint64
				for _, id := range ids {
					unionLive |= db.livePostings[encodeInvertKey(invertKey{series: series, dim: uint8(f.Dim), tag: id})]
				}
				if !initializedLive {
					liveMask = unionLive
					initializedLive = true
				} else {
					liveMask &= unionLive
				}
			}
		}
		if initializedSealed && sealed.count == 0 && (len(db.live) == 0 || (useLiveMask && initializedLive && liveMask == 0)) {
			return nil, 0, false, true, nil
		}
	}
	if !initializedLive {
		useLiveMask = false
	}
	if !initializedSealed {
		return nil, liveMask, useLiveMask, false, nil
	}
	return sealed, liveMask, useLiveMask, false, nil
}

func (db *DB) unionInvertBitmap(series uint16, dim uint8, tags []uint32, seriesBlocks, totalSealed int, slot *querySlot) (*blockBitmap, bool, error) {
	bm := newBlockBitmap(totalSealed)
	if len(db.invert) == 0 || totalSealed == 0 {
		return bm, false, nil
	}
	threshold := 0
	if seriesBlocks >= 64 {
		threshold = (seriesBlocks * 35) / 100
	}
	prefixKey := uint64(series)<<40 | uint64(dim)<<32
	for start := 0; start < len(tags); {
		index := invertChunkIndex(db.invert, prefixKey|uint64(tags[start]))
		if index < 0 {
			start++
			continue
		}
		end := start + 1
		for end < len(tags) && invertChunkIndex(db.invert, prefixKey|uint64(tags[end])) == index {
			end++
		}
		plain, err := db.readInvertChunkSlot(index, slot)
		if err != nil {
			return nil, false, err
		}
		dense := false
		err = decodeInvertChunkSorted(plain, prefixKey, tags[start:end], func(_ uint64, blkID uint32) {
			bm.set(blkID)
		}, func(_ uint64) {
			dense = true
		})
		if err != nil {
			return nil, false, err
		}
		if dense || (threshold > 0 && bm.count >= threshold) {
			return nil, true, nil
		}
		start = end
	}
	return bm, false, nil
}

func (db *DB) matchingTagIDs(series uint16, f Filter) []uint32 {
	if f.Dim < 0 || f.Dim >= 8 {
		return nil
	}
	dimIdx := uint8(f.Dim)
	ids := make([]uint32, 0)
	for _, value := range f.Values {
		switch f.Op {
		case TagEq:
			if id := db.dict.lookup(series, dimIdx, value); id != 0 {
				ids = append(ids, id)
			}
		case TagPrefix:
			ids = db.dict.matchPrefixIDs(series, dimIdx, value, ids)
		default:
			ids = db.dict.matchScanIDs(series, dimIdx, f.Op, value, ids)
		}
	}
	return dedupeUint32(ids)
}

func tagMatches(op TagOp, tag, value string) bool {
	switch op {
	case TagEq:
		return tag == value
	case TagPrefix:
		return strings.HasPrefix(tag, value)
	case TagSuffix:
		return strings.HasSuffix(tag, value)
	case TagContains:
		return strings.Contains(tag, value)
	case TagCIDR:
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			addr, addrErr := netip.ParseAddr(value)
			if addrErr != nil {
				return false
			}
			prefix = netip.PrefixFrom(addr, addr.BitLen())
		}
		if addr, err := netip.ParseAddr(tag); err == nil {
			return prefix.Contains(addr)
		}
		if other, err := netip.ParsePrefix(tag); err == nil {
			return prefix.Contains(other.Addr()) || other.Contains(prefix.Addr())
		}
	}
	return false
}
