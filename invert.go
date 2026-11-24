package strata

import (
	"encoding/binary"
	"errors"
	"sort"
)

const (
	invertModeDeltaVarint byte = 0
	invertModeBitset      byte = 1
	invertModeDenseSkip   byte = 2
	invertModeSingleton   byte = 3
	invertModeRLE         byte = 4
)

type invertKey struct {
	series uint16
	dim    uint8
	tag    uint32
}

type invertChunkMeta struct {
	first, last uint64
	offset      uint64
	length      uint32
	crc         uint32
}

func encodeInvertKey(k invertKey) uint64 {
	return uint64(k.series)<<40 | uint64(k.dim)<<32 | uint64(k.tag)
}

func decodeInvertKey(k uint64) invertKey {
	return invertKey{series: uint16(k >> 40), dim: uint8(k >> 32), tag: uint32(k)}
}

type invertRecordScratch struct {
	delta  []byte
	rle    []byte
	bitset []byte
	out    []byte
}

// encodeInvertRecordAdaptive encodes one posting list using the smallest representation
// among modeDenseSkip, modeSingleton, modeDeltaVarint, modeBitset, and modeRLE.
// ids must be sorted and deduplicated (non-empty when !dense).
func encodeInvertRecordAdaptive(key uint64, ids []uint32, dense bool, sc *invertRecordScratch) []byte {
	sc.out = sc.out[:0]
	sc.out = binary.LittleEndian.AppendUint64(sc.out, key)
	if dense {
		return append(sc.out, invertModeDenseSkip)
	}
	if len(ids) == 1 {
		sc.out = append(sc.out, invertModeSingleton)
		return binary.AppendUvarint(sc.out, uint64(ids[0]))
	}

	// 1. Delta varint payload: count + deltas.
	sc.delta = binary.AppendUvarint(sc.delta[:0], uint64(len(ids)))
	var prev uint32
	for _, id := range ids {
		sc.delta = binary.AppendUvarint(sc.delta, uint64(id-prev))
		prev = id
	}
	bestMode := invertModeDeltaVarint
	bestPayload := sc.delta

	// 2. RLE payload: runCount + (startDelta, runLen) pairs.
	runCount := 1
	for i := 1; i < len(ids); i++ {
		if ids[i] != ids[i-1]+1 {
			runCount++
		}
	}
	if runCount*2 < len(ids) {
		sc.rle = binary.AppendUvarint(sc.rle[:0], uint64(runCount))
		var prevEnd uint64
		for i := 0; i < len(ids); {
			start := uint64(ids[i])
			j := i + 1
			for j < len(ids) && ids[j] == ids[j-1]+1 {
				j++
			}
			runLen := uint64(j - i)
			sc.rle = binary.AppendUvarint(sc.rle, start-prevEnd)
			sc.rle = binary.AppendUvarint(sc.rle, runLen)
			prevEnd = start + runLen
			i = j
		}
		if len(sc.rle) < len(bestPayload) {
			bestMode = invertModeRLE
			bestPayload = sc.rle
		}
	}

	// 3. Bitset payload: count + base + span + bitset bytes (only if potentially smaller).
	base := ids[0]
	span := uint64(ids[len(ids)-1]) - uint64(base) + 1
	bitsetBytes := (span + 7) / 8
	if bitsetBytes+3 < uint64(len(bestPayload)) {
		sc.bitset = binary.AppendUvarint(sc.bitset[:0], uint64(len(ids)))
		sc.bitset = binary.AppendUvarint(sc.bitset, uint64(base))
		sc.bitset = binary.AppendUvarint(sc.bitset, span)
		startOff := len(sc.bitset)
		need := startOff + int(bitsetBytes)
		if cap(sc.bitset) < need {
			grown := make([]byte, need)
			copy(grown, sc.bitset)
			sc.bitset = grown
		} else {
			sc.bitset = sc.bitset[:need]
			clear(sc.bitset[startOff:need])
		}
		bits := sc.bitset[startOff:need]
		for _, id := range ids {
			offset := uint64(id - base)
			bits[offset/8] |= 1 << (offset % 8)
		}
		if len(sc.bitset) < len(bestPayload) {
			bestMode = invertModeBitset
			bestPayload = sc.bitset
		}
	}

	sc.out = append(sc.out, bestMode)
	sc.out = append(sc.out, bestPayload...)
	return sc.out
}

func (db *DB) loadInvertIndex() error {
	db.invert = nil
	d := db.head.sections[secTSInvert]
	if d.length == 0 {
		return nil
	}
	if d.length > uint64(^uint(0)>>1) {
		return errors.New("strata: TS invert section too large")
	}
	prefixLen := min(int(d.length), 10)
	prefix := make([]byte, prefixLen)
	if _, err := db.data.ReadAt(prefix, int64(d.offset)); err != nil {
		return err
	}
	off := 0
	n, err := readBlockUvarint(prefix, &off)
	if err != nil || n > 1<<20 {
		return errors.New("strata: invalid TS invert directory")
	}
	headerLen := off + int(n)*32
	if uint64(headerLen) > d.length {
		return errors.New("strata: truncated TS invert directory")
	}
	raw := make([]byte, headerLen)
	if _, err := db.data.ReadAt(raw, int64(d.offset)); err != nil {
		return err
	}
	off = 0
	if _, err := readBlockUvarint(raw, &off); err != nil {
		return err
	}
	db.invert = make([]invertChunkMeta, n)
	for i := range db.invert {
		if off > len(raw)-32 {
			return errors.New("strata: truncated TS invert directory")
		}
		db.invert[i].first = binary.LittleEndian.Uint64(raw[off:])
		off += 8
		db.invert[i].last = binary.LittleEndian.Uint64(raw[off:])
		off += 8
		db.invert[i].offset = binary.LittleEndian.Uint64(raw[off:])
		off += 8
		db.invert[i].length = binary.LittleEndian.Uint32(raw[off:])
		off += 4
		db.invert[i].crc = binary.LittleEndian.Uint32(raw[off:])
		off += 4
		if db.invert[i].first > db.invert[i].last {
			return errors.New("strata: invalid TS invert directory")
		}
	}
	return nil
}

func invertChunkIndex(chunks []invertChunkMeta, encoded uint64) int {
	index := sort.Search(len(chunks), func(i int) bool {
		return chunks[i].last >= encoded
	})
	if index == len(chunks) || encoded < chunks[index].first {
		return -1
	}
	return index
}
