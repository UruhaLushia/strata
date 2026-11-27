package eventlog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

const (
	fileMagic        = "STEVLG01"
	frameMagic       = "EVF1"
	tailMagic        = "1FVE"
	fileHeaderSize   = 16
	frameHeaderSize  = 16
	trailerSize      = 8
	frameOverhead    = frameHeaderSize + trailerSize
	recordHeaderSize = 14
	minFrameSize     = frameOverhead + recordHeaderSize
	maxRecordBytes   = 64 << 10
	maxFrameBytes    = 1 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func appendRecord(b []byte, r Record) []byte {
	b = binary.LittleEndian.AppendUint64(b, uint64(r.TimestampNS))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(r.Payload)))
	b = binary.LittleEndian.AppendUint16(b, r.StreamID)
	return append(b, r.Payload...)
}

func finishFrame(b []byte, count int) []byte {
	body := b[frameHeaderSize:]
	copy(b, frameMagic)
	binary.LittleEndian.PutUint32(b[4:], uint32(len(body)))
	binary.LittleEndian.PutUint32(b[8:], uint32(count))
	binary.LittleEndian.PutUint32(b[12:], crc32.Checksum(body, castagnoli))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(b)+trailerSize))
	return append(b, tailMagic...)
}

// decodeFrame validates the complete frame before any record can be delivered.
// Recovery passes nil offsets since it only needs to validate record boundaries.
func decodeFrame(b []byte, offsets *[]uint32) error {
	if len(b) < minFrameSize || string(b[:4]) != frameMagic ||
		binary.LittleEndian.Uint32(b[4:]) != uint32(len(b)-frameOverhead) {
		return ErrCorrupt
	}
	trailer := b[len(b)-trailerSize:]
	body := b[frameHeaderSize : len(b)-trailerSize]
	count := binary.LittleEndian.Uint32(b[8:])
	if string(trailer[4:]) != tailMagic || binary.LittleEndian.Uint32(trailer) != uint32(len(b)) ||
		count == 0 || count > uint32(len(body)/recordHeaderSize) ||
		binary.LittleEndian.Uint32(b[12:]) != crc32.Checksum(body, castagnoli) {
		return ErrCorrupt
	}
	if offsets != nil {
		*offsets = (*offsets)[:0]
		if cap(*offsets) < int(count) {
			*offsets = make([]uint32, 0, int(count))
		}
	}
	pos := frameHeaderSize
	end := len(b) - trailerSize
	for range count {
		if end-pos < recordHeaderSize {
			return ErrCorrupt
		}
		size := binary.LittleEndian.Uint32(b[pos+8:])
		if size > maxRecordBytes || size > uint32(end-pos-recordHeaderSize) {
			return ErrCorrupt
		}
		if offsets != nil {
			*offsets = append(*offsets, uint32(pos))
		}
		pos += recordHeaderSize + int(size)
	}
	if pos != end {
		return ErrCorrupt
	}
	return nil
}

func readAt(f *os.File, b []byte, off int64) error {
	_, err := f.ReadAt(b, off)
	if errors.Is(err, io.EOF) {
		return ErrCorrupt
	}
	return err
}

func writeAt(f *os.File, b []byte, off int64) error {
	n, err := f.WriteAt(b, off)
	if err == nil && n != len(b) {
		return io.ErrShortWrite
	}
	return err
}

func resizeFrame(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n, min(maxFrameBytes, max(n, 2*cap(b))))
	}
	return b[:n]
}

func readForward(f *os.File, b *[]byte, pos, end int64) (int64, error) {
	var header [frameHeaderSize]byte
	if end-pos < minFrameSize {
		return 0, ErrCorrupt
	}
	if err := readAt(f, header[:], pos); err != nil {
		return 0, err
	}
	if string(header[:4]) != frameMagic {
		return 0, ErrCorrupt
	}
	size := int64(binary.LittleEndian.Uint32(header[4:])) + frameOverhead
	if size < minFrameSize || size > maxFrameBytes || size > end-pos {
		return 0, ErrCorrupt
	}
	*b = resizeFrame(*b, int(size))
	copy(*b, header[:])
	if err := readAt(f, (*b)[frameHeaderSize:], pos+frameHeaderSize); err != nil {
		return 0, err
	}
	return pos + size, decodeFrame(*b, nil)
}

func recoverTail(f *os.File, end int64) (int64, error) {
	var b []byte
	for pos := int64(fileHeaderSize); pos < end; {
		next, err := readForward(f, &b, pos, end)
		if err == nil {
			pos = next
			continue
		}
		if err != ErrCorrupt {
			return 0, err
		}
		safe, err := canTruncateTail(f, pos, end)
		if err != nil {
			return 0, err
		}
		if !safe {
			return 0, ErrCorrupt
		}
		if err := f.Truncate(pos); err != nil {
			return 0, err
		}
		return pos, f.Sync()
	}
	return end, nil
}

// canTruncateTail checks whether the remaining bytes fit one torn frame.
func canTruncateTail(f *os.File, pos, end int64) (bool, error) {
	rem := end - pos
	if rem < minFrameSize {
		return true, nil
	}
	if rem > maxFrameBytes {
		return false, nil
	}
	var header [frameHeaderSize]byte
	if err := readAt(f, header[:], pos); err != nil {
		return false, err
	}
	bodyLen := binary.LittleEndian.Uint32(header[4:])
	count := binary.LittleEndian.Uint32(header[8:])
	size := int64(bodyLen) + frameOverhead
	return string(header[:4]) == frameMagic && size >= minFrameSize && size <= maxFrameBytes &&
		size >= rem && count >= 1 && count <= bodyLen/recordHeaderSize, nil
}
