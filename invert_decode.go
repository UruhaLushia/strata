package strata

import (
	"encoding/binary"
	"errors"
	"io"
)

func readInvertChunkFromSlot(f *osFile, desc sectionDesc, mapped []byte, chunk invertChunkMeta, slot *querySlot) ([]byte, error) {
	raw, bufPtr, err := sectionBlockPooled(f, desc, chunk.offset, chunk.length, mapped)
	if bufPtr != nil {
		defer putReadBuf(bufPtr)
	}
	if err != nil {
		return nil, err
	}
	if crc(raw) != chunk.crc {
		return nil, errors.New("strata: TS invert chunk CRC mismatch")
	}
	if slot != nil {
		return slot.decodeZstd(raw)
	}
	return zstdDecompress(raw)
}

func readInvertChunkFrom(f *osFile, desc sectionDesc, mapped []byte, chunk invertChunkMeta) ([]byte, error) {
	return readInvertChunkFromSlot(f, desc, mapped, chunk, nil)
}

func readInvertChunkFromView(view *fileView, desc sectionDesc, chunk invertChunkMeta, slot *querySlot) ([]byte, error) {
	if view == nil || view.data == nil {
		return nil, errors.New("strata: missing file view")
	}
	return readInvertChunkFromSlot(view.data, desc, view.mappedBytes(), chunk, slot)
}

func decodeInvertRecordBody(plain []byte, off *int, k uint64, mode byte, match bool, onPosting func(key uint64, blockID uint32), onDense func(key uint64)) error {
	switch mode {
	case invertModeDenseSkip:
		if match && onDense != nil {
			onDense(k)
		}
	case invertModeSingleton:
		blk, err := readBlockUvarint(plain, off)
		if err != nil || blk > uint64(^uint32(0)) {
			return errors.New("strata: invalid TS invert singleton")
		}
		if match && onPosting != nil {
			onPosting(k, uint32(blk))
		}
	case invertModeDeltaVarint:
		n, err := readBlockUvarint(plain, off)
		if err != nil {
			return err
		}
		var prev uint32
		for range n {
			delta, err := readBlockUvarint(plain, off)
			if err != nil {
				return err
			}
			if delta > uint64(^uint32(0)-prev) {
				return errors.New("strata: invalid TS invert delta")
			}
			prev += uint32(delta)
			if match && onPosting != nil {
				onPosting(k, prev)
			}
		}
	case invertModeBitset:
		n, err := readBlockUvarint(plain, off)
		if err != nil {
			return err
		}
		base, err := readBlockUvarint(plain, off)
		if err != nil {
			return err
		}
		span, err := readBlockUvarint(plain, off)
		if err != nil {
			return err
		}
		if span == 0 || base > uint64(^uint32(0)) || span > uint64(^uint32(0))-base+1 {
			return errors.New("strata: invalid TS invert bitset")
		}
		byteLen := (span + 7) / 8
		if byteLen > uint64(len(plain)-*off) {
			return io.ErrUnexpectedEOF
		}
		bits := plain[*off : *off+int(byteLen)]
		*off += int(byteLen)
		if !match || onPosting == nil {
			return nil
		}
		var emitted uint64
		for offset := range span {
			if bits[offset/8]&(1<<(offset%8)) != 0 {
				onPosting(k, uint32(base+offset))
				emitted++
			}
		}
		if emitted != n {
			return errors.New("strata: invalid TS invert bitset count")
		}
	case invertModeRLE:
		runCount, err := readBlockUvarint(plain, off)
		if err != nil {
			return err
		}
		var prevEnd uint64
		for range runCount {
			startDelta, err := readBlockUvarint(plain, off)
			if err != nil {
				return err
			}
			runLen, err := readBlockUvarint(plain, off)
			if err != nil {
				return err
			}
			if runLen == 0 || startDelta > uint64(^uint32(0))-prevEnd {
				return errors.New("strata: invalid TS invert RLE")
			}
			start := prevEnd + startDelta
			if runLen-1 > uint64(^uint32(0))-start {
				return errors.New("strata: invalid TS invert RLE")
			}
			end := start + runLen
			if match && onPosting != nil {
				for id := start; id < end; id++ {
					onPosting(k, uint32(id))
				}
			}
			prevEnd = end
		}
	default:
		return errors.New("strata: invalid TS invert mode")
	}
	return nil
}

// decodeInvertChunkScan streams decoded postings directly into callbacks without allocating
// intermediate posting maps or slices. Supports all 5 adaptive invert modes (0..4).
func decodeInvertChunkScan(plain []byte, wanted map[uint64]struct{}, onPosting func(key uint64, blockID uint32), onDense func(key uint64)) error {
	off := 0
	for off < len(plain) {
		if off > len(plain)-9 {
			return errors.New("strata: truncated TS invert record")
		}
		k := binary.LittleEndian.Uint64(plain[off:])
		mode := plain[off+8]
		off += 9
		match := wanted == nil
		if !match {
			_, match = wanted[k]
		}
		if err := decodeInvertRecordBody(plain, &off, k, mode, match, onPosting, onDense); err != nil {
			return err
		}
	}
	return nil
}

// decodeInvertChunkSorted matches strictly sorted ascending tag IDs under prefixKey
// using a zero-allocation two-pointer merge and stops early as soon as k > maxKey.
func decodeInvertChunkSorted(plain []byte, prefixKey uint64, wantedTags []uint32, onPosting func(key uint64, blockID uint32), onDense func(key uint64)) error {
	if len(wantedTags) == 0 {
		return nil
	}
	maxKey := prefixKey | uint64(wantedTags[len(wantedTags)-1])
	wi := 0
	off := 0
	for off < len(plain) {
		if off > len(plain)-9 {
			return errors.New("strata: truncated TS invert record")
		}
		k := binary.LittleEndian.Uint64(plain[off:])
		if k > maxKey {
			return nil
		}
		mode := plain[off+8]
		off += 9
		for wi < len(wantedTags) && (prefixKey|uint64(wantedTags[wi])) < k {
			wi++
		}
		if wi >= len(wantedTags) {
			return nil
		}
		match := (prefixKey | uint64(wantedTags[wi])) == k
		if match {
			wi++
		}
		if err := decodeInvertRecordBody(plain, &off, k, mode, match, onPosting, onDense); err != nil {
			return err
		}
	}
	return nil
}
