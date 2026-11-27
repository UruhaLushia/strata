package strata

import (
	"errors"
	"hash"
	"hash/crc32"
)

func copyPassthroughBlock(dst, source *osFile, section sectionDesc, block sealedMeta, pos int64, sectionHash hash.Hash, buf []byte) error {
	if source == nil || block.offset < section.offset ||
		block.offset+uint64(block.length) > section.offset+section.length {
		return errors.New("strata: invalid passthrough block")
	}
	blockHash := crc32.New(castagnoli)
	if len(buf) == 0 {
		buf = make([]byte, 64<<10)
	}
	remaining := uint64(block.length)
	readPos, writePos := block.offset, pos
	for remaining > 0 {
		n := min(uint64(len(buf)), remaining)
		chunk := buf[:n]
		if _, err := source.ReadAt(chunk, int64(readPos)); err != nil {
			return err
		}
		if _, err := dst.WriteAt(chunk, writePos); err != nil {
			return err
		}
		_, _ = blockHash.Write(chunk)
		_, _ = sectionHash.Write(chunk)
		readPos += n
		writePos += int64(n)
		remaining -= n
	}
	if blockHash.Sum32() != block.crc {
		return errors.New("strata: passthrough block CRC mismatch")
	}
	return nil
}
