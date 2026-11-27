package strata

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/UruhaLushia/strata/codec"
)

const (
	kvFlagTTLChecked uint8 = 1 << 1
	kvFlagHasLiveTTL uint8 = 1 << 2
)

func canPassthroughKV(oldHead superblock, sealedKVRev uint64, target uint8) bool {
	if sealedKVRev != oldHead.sealedKVRev {
		return false
	}
	sBlk, sBlob, sIdx := oldHead.sections[0], oldHead.sections[1], oldHead.sections[2]
	if sBlk.length == 0 && sBlob.length == 0 && sIdx.length == 0 {
		return true
	}
	if sIdx.length == 0 || sIdx.kind != secKVSSTIndex || sIdx.codec != 0 && sIdx.codec != target {
		return false
	}
	if sIdx.flags&kvFlagTTLChecked == 0 || sIdx.flags&kvFlagHasLiveTTL != 0 {
		return false
	}
	if oldHead.walOffset < dataStart {
		return false
	}
	cur := uint64(dataStart)
	if sBlob.length > 0 {
		if sBlob.kind != secKVLargeBlobs || sBlob.codec != target || sBlob.offset != cur || sBlob.length > oldHead.walOffset-cur {
			return false
		}
		cur += sBlob.length
	}
	if sBlk.length > 0 {
		if sBlk.kind != secKVSSTBlocks || sBlk.codec != target || sBlk.offset != cur || sBlk.length > oldHead.walOffset-cur {
			return false
		}
		cur += sBlk.length
	}
	return sIdx.offset == cur && sIdx.length <= oldHead.walOffset-cur
}

func copyPassthroughKVSections(dst, src *osFile, oldHead superblock) (int64, [3]sectionDesc, sectionDesc, error) {
	sBlk, sBlob, sIdx := oldHead.sections[0], oldHead.sections[1], oldHead.sections[2]
	if sBlk.length == 0 && sBlob.length == 0 && sIdx.length == 0 {
		return int64(dataStart), [3]sectionDesc{}, sectionDesc{}, nil
	}
	if src == nil || dst == nil {
		return 0, [3]sectionDesc{}, sectionDesc{}, errors.New("strata: invalid passthrough KV file")
	}
	pos := int64(dataStart)
	buf := make([]byte, 64<<10)
	secHash := crc32.New(castagnoli)
	for _, sec := range [3]sectionDesc{sBlob, sBlk, sIdx} {
		if sec.length == 0 {
			continue
		}
		if sec.offset != uint64(pos) {
			return 0, [3]sectionDesc{}, sectionDesc{}, errors.New("strata: invalid passthrough KV section offset")
		}
		secHash.Reset()
		remaining := sec.length
		for remaining > 0 {
			n := min(uint64(len(buf)), remaining)
			chunk := buf[:n]
			if _, err := src.ReadAt(chunk, pos); err != nil {
				return 0, [3]sectionDesc{}, sectionDesc{}, err
			}
			if _, err := dst.WriteAt(chunk, pos); err != nil {
				return 0, [3]sectionDesc{}, sectionDesc{}, err
			}
			_, _ = secHash.Write(chunk)
			pos += int64(n)
			remaining -= n
		}
		if secHash.Sum32() != sec.crc {
			return 0, [3]sectionDesc{}, sectionDesc{}, errors.New("strata: passthrough KV section CRC mismatch")
		}
	}
	return pos, [3]sectionDesc{sBlk, {}, sIdx}, sBlob, nil
}

func writeKVSections(f *osFile, pos int64, blocks []kvSSTBlock, catalog []byte, enc codec.Encoder, target uint8) (int64, [3]sectionDesc, sectionDesc, error) {
	var desc [3]sectionDesc
	var blobDesc sectionDesc
	if len(blocks) == 0 && len(catalog) == 0 {
		return pos, desc, blobDesc, nil
	}
	var blobs [][]byte
	var blobLengths []uint32
	hasLiveTTL := false
	for i := range blocks {
		for j := range blocks[i].entries {
			e := &blocks[i].entries[j]
			if e.expiresAtUnixNano != 0 {
				hasLiveTTL = true
			}
			if len(e.value) <= kvInlineLimit {
				continue
			}
			if len(e.value) > kvMaxBlobRaw {
				return 0, desc, blobDesc, codec.ErrTooLarge
			}
			raw, compressed, err := encodeSmaller(enc, nil, e.value, kvMaxBlobRaw)
			if err != nil {
				return 0, desc, blobDesc, err
			}
			e.rawBlob = !compressed
			e.external = true
			e.blobLen = uint32(len(raw))
			e.blobCRC = crc(raw)
			blobLengths = append(blobLengths, uint32(len(e.value)))
			e.value = nil
			blobs = append(blobs, raw)
		}
	}
	if len(blobs) > 0 {
		blobStart := pos
		blobHash := crc32.New(castagnoli)
		var blobRawLen uint32
		blobIndex := 0
		for i := range blocks {
			for j := range blocks[i].entries {
				e := &blocks[i].entries[j]
				if !e.external {
					continue
				}
				e.blobOffset = uint64(pos)
				if _, err := f.WriteAt(blobs[blobIndex], pos); err != nil {
					return 0, desc, blobDesc, err
				}
				_, _ = blobHash.Write(blobs[blobIndex])
				blobRawLen += blobLengths[blobIndex]
				pos += int64(len(blobs[blobIndex]))
				blobIndex++
			}
		}
		blobDesc = sectionDesc{
			kind: secKVLargeBlobs, codec: target, count: uint32(len(blobs)),
			offset: uint64(blobStart), length: uint64(pos - blobStart),
			rawLen: blobRawLen, crc: blobHash.Sum32(),
		}
	}
	start := pos
	index := binary.AppendUvarint(nil, uint64(len(catalog)))
	index = append(index, catalog...)
	blockHash := crc32.New(castagnoli)
	var rawAllLen uint32
	for i := range blocks {
		b := &blocks[i]
		plain := encodeKVBlock(b.entries)
		if len(plain) > kvBlockRawLimit(b.last) {
			return 0, desc, blobDesc, codec.ErrTooLarge
		}
		var err error
		b.raw, err = encodePayload(enc, nil, plain)
		if err != nil {
			return 0, desc, blobDesc, err
		}
		if _, err := f.WriteAt(b.raw, pos); err != nil {
			return 0, desc, blobDesc, err
		}
		_, _ = blockHash.Write(b.raw)
		rawAllLen += uint32(len(b.raw))
		out := binary.AppendUvarint(index, uint64(len(b.first)))
		index = append(out, b.first...)
		index = binary.AppendUvarint(index, uint64(len(b.last)))
		index = append(index, b.last...)
		index = binary.LittleEndian.AppendUint64(index, b.maxRev)
		index = binary.LittleEndian.AppendUint64(index, uint64(pos))
		index = binary.LittleEndian.AppendUint32(index, uint32(len(b.raw)))
		index = binary.LittleEndian.AppendUint32(index, crc(b.raw))
		pos += int64(len(b.raw))
	}
	desc[0] = sectionDesc{kind: secKVSSTBlocks, codec: target, count: uint32(len(blocks)), offset: uint64(start), length: uint64(pos - start), rawLen: rawAllLen, crc: blockHash.Sum32()}
	idxStart := pos
	idxFlags := kvFlagTTLChecked
	if hasLiveTTL {
		idxFlags |= kvFlagHasLiveTTL
	}
	out, compressed, err := encodeSmaller(enc, nil, index, maxSectionRawBytes)
	if err != nil {
		return 0, desc, blobDesc, err
	}
	var idxCodec uint8
	if compressed {
		idxCodec = target
	}
	if _, err := f.WriteAt(out, pos); err != nil {
		return 0, desc, blobDesc, err
	}
	desc[2] = sectionDesc{
		kind: secKVSSTIndex, codec: idxCodec, flags: idxFlags, count: uint32(len(blocks)),
		offset: uint64(idxStart), length: uint64(len(out)), rawLen: uint32(len(index)), crc: crc(out),
	}
	return pos + int64(len(out)), desc, blobDesc, nil
}
