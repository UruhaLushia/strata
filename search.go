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
	bits       []uint64
	count      int
	base, span uint32
	inline     [4]uint64
}

func newBlockBitmap(base, span uint32) *blockBitmap {
	bm := &blockBitmap{base: base, span: span}
	words := (uint64(span) + 63) >> 6
	if words <= uint64(len(bm.inline)) {
		bm.bits = bm.inline[:words]
	} else {
		bm.bits = make([]uint64, words)
	}
	return bm
}

func (b *blockBitmap) set(pos uint32) {
	if pos < b.base || pos-b.base >= b.span {
		return
	}
	pos -= b.base
	w := pos >> 6
	bit := uint64(1) << (pos & 63)
	if b.bits[w]&bit == 0 {
		b.bits[w] |= bit
		b.count++
	}
}

func (b *blockBitmap) has(pos uint32) bool {
	return bitmapHas(b.bits, b.base, b.span, pos)
}

func bitmapHas(bits []uint64, base, span, pos uint32) bool {
	if pos < base || pos-base >= span {
		return false
	}
	pos -= base
	return bits[pos>>6]&(uint64(1)<<(pos&63)) != 0
}

// Multi-filter intersections always use the full block range.
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
	var rules []resetRule
	if s := db.series[series]; s != nil {
		rules = s.rules
	}
	sealedExcluded := func(res byte, b sealedMeta) bool {
		if len(rules) == 0 {
			return false
		}
		if res == resMinuteHR {
			return blockExcludedRules(rules, b.start, b.end(), b.epoch)
		}
		return blockExcludedByResetAll(rules, b.epoch)
	}
	var minTS, maxTS int64
	for key, indexes := range db.sealedByRes {
		if key.series != series || len(indexes) == 0 {
			continue
		}
		lo := 0
		for lo < len(indexes) && sealedExcluded(key.res, db.sealed[indexes[lo]]) {
			lo++
		}
		if lo == len(indexes) {
			continue
		}
		hi := len(indexes) - 1
		for hi > lo && sealedExcluded(key.res, db.sealed[indexes[hi]]) {
			hi--
		}
		first := db.sealed[indexes[lo]]
		last := db.sealed[indexes[hi]]
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
		if len(rules) > 0 && blockExcludedRules(rules, b.ts, b.ts+60, b.epoch) {
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

func searchCandidatesSnap(snap *querySnapshot, window [3][]uint32, filters []Filter, compiled []compiledFilter, hasLiveCandidate bool, slot *querySlot) (*blockBitmap, bool, error) {
	series := snap.series.id
	seriesBlocks := len(snap.fineAll) + len(snap.hourAll) + len(snap.dayAll)
	totalSealed := len(snap.sealed)

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
		if ids[0] == 0 {
			continue
		}
		totalTags := snap.dict.dimCount(series, uint8(f.Dim))
		if totalTags > 0 && len(ids)*5 >= totalTags*4 {
			continue
		}
		base, span := uint32(0), uint32(totalSealed)
		if len(filters) == 1 && len(ids) == 1 {
			first, last := uint32(totalSealed), uint32(0)
			for _, indexes := range window {
				if len(indexes) != 0 {
					first = min(first, indexes[0])
					last = max(last, indexes[len(indexes)-1])
				}
			}
			if first <= last {
				base, span = first, last-first+1
			}
		}
		bm := newBlockBitmap(base, span)
		globalCount, denseSealed, err := unionInvertBitmapSnap(snap.view, snap.invertSec, snap.invert, series, uint8(f.Dim), ids, seriesBlocks, totalSealed, slot, bm)
		if err != nil {
			return nil, false, err
		}
		if !denseSealed {
			if !initializedSealed {
				sealed = bm
				initializedSealed = true
			} else {
				sealed.intersect(bm)
			}
		}
		if initializedSealed && sealed.count == 0 {
			// A key present only outside the window still leaves the window's
			// coverage to the planner, as with a full-range bitmap.
			if !hasLiveCandidate && (span == uint32(totalSealed) || globalCount == 0) {
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

func unionInvertBitmapSnap(view *fileView, invertSec sectionDesc, invert []invertChunkMeta, series uint16, dim uint8, tags []uint32, seriesBlocks, totalSealed int, slot *querySlot, bm *blockBitmap) (int, bool, error) {
	if len(invert) == 0 || totalSealed == 0 {
		return 0, false, nil
	}
	windowed := bm.span < uint32(totalSealed)
	globalCount := 0
	onPosting := func(_ uint64, blkID uint32) { bm.set(blkID) }
	if windowed {
		var last uint32
		onPosting = func(_ uint64, blkID uint32) {
			if blkID < uint32(totalSealed) && (globalCount == 0 || blkID != last) {
				globalCount++
				last = blkID
				bm.set(blkID)
			}
		}
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
		plain, bufPtr, err := readInvertChunkFromView(view, invertSec, invert[index], totalSealed, slot)
		if err != nil {
			return 0, false, err
		}
		dense := false
		err = decodeInvertChunkSorted(plain, prefixKey, tags[start:end], onPosting, func(_ uint64) {
			dense = true
		})
		putReadBuf(bufPtr)
		if err != nil {
			return 0, false, err
		}
		if !windowed {
			globalCount = bm.count
		}
		if dense || (threshold > 0 && globalCount >= threshold) {
			return globalCount, true, nil
		}
		start = end
	}
	return globalCount, false, nil
}

func matchingTagIDsSnap(d dictionary, series uint16, f Filter, slot *querySlot) ([]uint32, error) {
	if !d.snapshot || series != d.snapshotSeries || f.Dim < 0 || f.Dim >= 8 {
		return nil, nil
	}
	dimSnap := &d.snapshotDims[f.Dim]
	ids := make([]uint32, 0)
	for _, value := range f.Values {
		if tagMatches(f.Op, "", value) {
			ids = append(ids, 0)
		}
		switch f.Op {
		case TagEq:
			id, err := dimSnap.lookup(value, slot)
			if err != nil {
				return nil, err
			}
			if id != 0 {
				ids = append(ids, id)
			}
		case TagPrefix:
			var err error
			ids, err = dimSnap.matchPrefixIDs(value, ids, slot)
			if err != nil {
				return nil, err
			}
		default:
			var err error
			ids, err = dimSnap.matchScanIDs(f.Op, value, ids, slot)
			if err != nil {
				return nil, err
			}
		}
	}
	return dedupeUint32(ids), nil
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
