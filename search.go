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

func searchCandidatesSnap(snap *querySnapshot, filters []Filter, compiled []compiledFilter, hasLiveCandidate bool, slot *querySlot) (*blockBitmap, bool, error) {
	series := snap.series.id
	seriesBlocks := len(snap.fineAll) + len(snap.hourAll) + len(snap.dayAll)
	totalSealed := len(snap.sealed)
	invertSec := snap.head.sections[secTSInvert]

	var sealed *blockBitmap
	initializedSealed := false

	for i, f := range filters {
		if f.Negate {
			continue
		}
		ids := compiled[i].ids
		if len(ids) == 0 {
			return nil, true, nil
		}
		totalTags := snap.dict.dimCount(series, uint8(f.Dim))
		if totalTags > 0 && len(ids)*5 >= totalTags*4 {
			continue
		}
		unionBits, denseSealed, err := unionInvertBitmapSnap(snap.view, invertSec, snap.invert, series, uint8(f.Dim), ids, seriesBlocks, totalSealed, slot)
		if err != nil {
			return nil, false, err
		}
		if !denseSealed {
			if !initializedSealed {
				sealed = unionBits
				initializedSealed = true
			} else {
				sealed.intersect(unionBits)
			}
		}
		if initializedSealed && sealed.count == 0 {
			if !hasLiveCandidate {
				return nil, true, nil
			}
			break
		}
	}
	if !initializedSealed {
		if seriesBlocks == 0 && !hasLiveCandidate {
			return nil, true, nil
		}
		return nil, false, nil
	}
	return sealed, false, nil
}

func unionInvertBitmapSnap(view *fileView, invertSec sectionDesc, invert []invertChunkMeta, series uint16, dim uint8, tags []uint32, seriesBlocks, totalSealed int, slot *querySlot) (*blockBitmap, bool, error) {
	bm := newBlockBitmap(totalSealed)
	if len(invert) == 0 || totalSealed == 0 {
		return bm, false, nil
	}
	threshold := 0
	if seriesBlocks >= 64 {
		threshold = (seriesBlocks * 35) / 100
	}
	prefixKey := uint64(series)<<40 | uint64(dim)<<32
	for start := 0; start < len(tags); {
		index := invertChunkIndex(invert, prefixKey|uint64(tags[start]))
		if index < 0 {
			start++
			continue
		}
		end := start + 1
		for end < len(tags) && invertChunkIndex(invert, prefixKey|uint64(tags[end])) == index {
			end++
		}
		plain, err := readInvertChunkFromView(view, invertSec, invert[index], slot)
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

func matchingTagIDsSnap(d dictionary, series uint16, f Filter) []uint32 {
	if !d.snapshot || series != d.snapshotSeries || f.Dim < 0 || f.Dim >= 8 {
		return nil
	}
	dimSnap := &d.snapshotDims[f.Dim]
	ids := make([]uint32, 0)
	for _, value := range f.Values {
		switch f.Op {
		case TagEq:
			if id := dimSnap.lookup(value); id != 0 {
				ids = append(ids, id)
			}
		case TagPrefix:
			ids = dimSnap.matchPrefixIDs(value, ids)
		default:
			ids = dimSnap.matchScanIDs(f.Op, value, ids)
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
