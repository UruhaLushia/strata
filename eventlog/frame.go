package eventlog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"

	"github.com/UruhaLushia/strata/codec"
)

const (
	fileMagic          = "STEVLG01"
	frameMagic         = "EVF1"
	tailMagic          = "1FVE"
	fileHeaderSize     = 16
	frameHeaderSize    = 21
	trailerSize        = 8
	frameOverhead      = frameHeaderSize + trailerSize
	recordHeaderSize   = 14
	minRawFrameSize    = frameOverhead + recordHeaderSize
	minStoredFrameSize = frameOverhead + 1
	maxRecordBytes     = 64 << 10
	maxFrameBytes      = 1 << 20
	maxRawBodyBytes    = maxFrameBytes - frameOverhead
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func appendRecord(b []byte, r Record) []byte {
	b = binary.LittleEndian.AppendUint64(b, uint64(r.TimestampNS))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(r.Payload)))
	b = binary.LittleEndian.AppendUint16(b, r.StreamID)
	return append(b, r.Payload...)
}

func finishFrame(b []byte, rawLen, count int, id uint8) []byte {
	copy(b, frameMagic)
	binary.LittleEndian.PutUint32(b[4:], uint32(len(b)-frameHeaderSize))
	binary.LittleEndian.PutUint32(b[8:], uint32(rawLen))
	binary.LittleEndian.PutUint32(b[12:], uint32(count))
	b[20] = id
	binary.LittleEndian.PutUint32(b[16:], frameCRC(b))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(b)+trailerSize))
	return append(b, tailMagic...)
}

func frameCRC(b []byte) uint32 {
	return crc32.Update(crc32.Checksum(b[:16], castagnoli), castagnoli, b[20:])
}

type storedFrame struct {
	body          []byte
	rawLen, count int
	id            uint8
}

func parseFrame(b []byte) (storedFrame, error) {
	if len(b) < minStoredFrameSize || string(b[:4]) != frameMagic ||
		binary.LittleEndian.Uint32(b[4:]) != uint32(len(b)-frameOverhead) {
		return storedFrame{}, ErrCorrupt
	}
	end := len(b) - trailerSize
	trailer := b[end:]
	if string(trailer[4:]) != tailMagic || binary.LittleEndian.Uint32(trailer) != uint32(len(b)) ||
		binary.LittleEndian.Uint32(b[16:]) != frameCRC(b[:end]) {
		return storedFrame{}, ErrCorrupt
	}
	rawLen, count := binary.LittleEndian.Uint32(b[8:]), binary.LittleEndian.Uint32(b[12:])
	storedLen, id := uint32(end-frameHeaderSize), b[20]
	if rawLen > maxRawBodyBytes || count == 0 || count > rawLen/recordHeaderSize ||
		id == codec.IDNone && storedLen != rawLen || id != codec.IDNone && storedLen >= rawLen {
		return storedFrame{}, ErrCorrupt
	}
	return storedFrame{b[frameHeaderSize:end], int(rawLen), int(count), id}, nil
}

func (f storedFrame) decode(slot *scanSlot, codecs map[uint8]codec.Codec) ([]byte, error) {
	if slot.dec != nil && slot.decID != f.id {
		if err := slot.closeDecoder(); err != nil {
			return nil, err
		}
	}
	body := f.body
	if f.id != codec.IDNone {
		if slot.dec == nil {
			factory := codecs[f.id]
			if factory == nil {
				return nil, codec.ErrUnsupportedCodec
			}
			dec, err := factory.NewDecoder()
			if err != nil {
				return nil, err
			}
			slot.dec, slot.decID = dec, f.id
		}
		raw, err := slot.dec.Decode(slot.raw[:0], f.body, f.rawLen)
		if err != nil {
			return nil, err
		}
		if len(raw) != f.rawLen {
			return nil, ErrCorrupt
		}
		slot.raw, body = raw, raw
	}
	return body, checkRecords(body, f.count, &slot.offsets)
}

func checkRecords(body []byte, count int, offsets *[]uint32) error {
	*offsets = (*offsets)[:0]
	if cap(*offsets) < count {
		*offsets = make([]uint32, 0, count)
	}
	pos, end := 0, len(body)
	for range count {
		if end-pos < recordHeaderSize {
			return ErrCorrupt
		}
		size := binary.LittleEndian.Uint32(body[pos+8:])
		if size > maxRecordBytes || size > uint32(end-pos-recordHeaderSize) {
			return ErrCorrupt
		}
		*offsets = append(*offsets, uint32(pos))
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

func readForward(f *os.File, b *[]byte, pos, end int64) (storedFrame, error) {
	var header [frameHeaderSize]byte
	if end-pos < minStoredFrameSize {
		return storedFrame{}, ErrCorrupt
	}
	if err := readAt(f, header[:], pos); err != nil {
		return storedFrame{}, err
	}
	if string(header[:4]) != frameMagic {
		return storedFrame{}, ErrCorrupt
	}
	size := int64(binary.LittleEndian.Uint32(header[4:])) + frameOverhead
	if size < minStoredFrameSize || size > maxFrameBytes || size > end-pos {
		return storedFrame{}, ErrCorrupt
	}
	*b = resizeFrame(*b, int(size))
	copy(*b, header[:])
	if err := readAt(f, (*b)[frameHeaderSize:], pos+frameHeaderSize); err != nil {
		return storedFrame{}, err
	}
	return parseFrame(*b)
}

func recoverTail(f *os.File, end int64, codecs map[uint8]codec.Codec) (int64, error) {
	var b []byte
	for pos := int64(fileHeaderSize); pos < end; {
		frame, err := readForward(f, &b, pos, end)
		if err == nil {
			if frame.id != codec.IDNone && codecs[frame.id] == nil {
				return 0, codec.ErrUnsupportedCodec
			}
			pos += int64(frameOverhead + len(frame.body))
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
	if rem < minStoredFrameSize {
		return true, nil
	}
	if rem > maxFrameBytes {
		return false, nil
	}
	var header [frameHeaderSize]byte
	if err := readAt(f, header[:], pos); err != nil {
		return false, err
	}
	size := int64(binary.LittleEndian.Uint32(header[4:])) + frameOverhead
	rawLen, count := binary.LittleEndian.Uint32(header[8:]), binary.LittleEndian.Uint32(header[12:])
	return string(header[:4]) == frameMagic && size >= rem && size <= maxFrameBytes &&
		rawLen <= maxRawBodyBytes && count >= 1 && count <= rawLen/recordHeaderSize, nil
}
