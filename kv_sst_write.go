package strata

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/klauspost/compress/zstd"
)

func writeKVSections(f *osFile, pos int64, blocks []kvSSTBlock, catalog []byte, enc *zstd.Encoder) (int64, [3]sectionDesc, sectionDesc, error) {
	var desc [3]sectionDesc
	var blobDesc sectionDesc
	if len(blocks) == 0 && len(catalog) == 0 {
		return pos, desc, blobDesc, nil
	}
	var blobs [][]byte
	var blobLengths []uint32
	for i := range blocks {
		for j := range blocks[i].entries {
			e := &blocks[i].entries[j]
			if len(e.value) <= 32<<10 {
				continue
			}
			raw := enc.EncodeAll(e.value, nil)
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
			kind: secKVLargeBlobs, codec: 1, count: uint32(len(blobs)),
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
		b.raw = encodeKVBlockWithEncoder(b.entries, enc)
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
		index = binary.LittleEndian.AppendUint32(index, uint32(len(b.raw)))
		index = binary.LittleEndian.AppendUint32(index, crc(b.raw))
		pos += int64(len(b.raw))
	}
	desc[0] = sectionDesc{kind: secKVSSTBlocks, codec: 1, count: uint32(len(blocks)), offset: uint64(start), length: uint64(pos - start), rawLen: rawAllLen, crc: blockHash.Sum32()}
	idxStart := pos
	if _, err := f.WriteAt(index, pos); err != nil {
		return 0, desc, blobDesc, err
	}
	desc[2] = sectionDesc{kind: secKVSSTIndex, count: uint32(len(blocks)), offset: uint64(idxStart), length: uint64(len(index)), rawLen: uint32(len(index)), crc: crc(index)}
	return pos + int64(len(index)), desc, blobDesc, nil
}
