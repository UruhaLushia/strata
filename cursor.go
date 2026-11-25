package strata

import (
	"encoding/base64"
	"encoding/binary"
	"math"
)

const (
	cursorVersion byte = 1
	cursorRawSize      = 1 + 8 + 8 + 8 + 8 + 4 + 1 + 4
)

// Cursor is an opaque zero-pointer pagination token that locks the resolved window
// and validates query consistency across page continuations.
type Cursor struct {
	winFromSec int64
	winToSec   int64
	queryHash  uint64
	slotStart  int64
	rowSkip    uint32
	slotRes    uint8
	valid      bool
}

func (c Cursor) IsZero() bool { return !c.valid }

func (c Cursor) String() string {
	if !c.valid {
		return ""
	}
	var buf [cursorRawSize]byte
	buf[0] = cursorVersion
	binary.LittleEndian.PutUint64(buf[1:9], uint64(c.winFromSec))
	binary.LittleEndian.PutUint64(buf[9:17], uint64(c.winToSec))
	binary.LittleEndian.PutUint64(buf[17:25], c.queryHash)
	binary.LittleEndian.PutUint64(buf[25:33], uint64(c.slotStart))
	binary.LittleEndian.PutUint32(buf[33:37], c.rowSkip)
	buf[37] = c.slotRes
	binary.LittleEndian.PutUint32(buf[38:42], crc(buf[:38]))
	return base64.RawURLEncoding.EncodeToString(buf[:])
}

func ParseCursor(raw string) (Cursor, error) {
	if raw == "" {
		return Cursor{}, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(data) != cursorRawSize || data[0] != cursorVersion {
		return Cursor{}, ErrInvalidCursor
	}
	if binary.LittleEndian.Uint32(data[38:42]) != crc(data[:38]) {
		return Cursor{}, ErrInvalidCursor
	}
	c := Cursor{
		winFromSec: int64(binary.LittleEndian.Uint64(data[1:9])),
		winToSec:   int64(binary.LittleEndian.Uint64(data[9:17])),
		queryHash:  binary.LittleEndian.Uint64(data[17:25]),
		slotStart:  int64(binary.LittleEndian.Uint64(data[25:33])),
		rowSkip:    binary.LittleEndian.Uint32(data[33:37]),
		slotRes:    data[37],
		valid:      true,
	}
	if c.winToSec < c.winFromSec || c.rowSkip > math.MaxInt32 {
		return Cursor{}, ErrInvalidCursor
	}
	switch c.slotRes {
	case resMinuteHR, resHour, resDay:
		return c, nil
	default:
		return Cursor{}, ErrInvalidCursor
	}
}

func cursorQueryHash(seriesID uint16, filters []Filter, preferFine, requireExact bool) uint64 {
	const (
		fnvOffset64 = 14695981039346656037
		fnvPrime64  = 1099511628211
	)
	h := uint64(fnvOffset64)
	mixBytes := func(p []byte) {
		for _, b := range p {
			h ^= uint64(b)
			h *= fnvPrime64
		}
	}
	mixString := func(s string) {
		for i := 0; i < len(s); i++ {
			h ^= uint64(s[i])
			h *= fnvPrime64
		}
	}
	var tmp [8]byte
	binary.LittleEndian.PutUint16(tmp[:2], seriesID)
	if preferFine {
		tmp[2] = 1
	}
	if requireExact {
		tmp[3] = 1
	}
	binary.LittleEndian.PutUint32(tmp[4:8], uint32(len(filters)))
	mixBytes(tmp[:8])
	for _, f := range filters {
		binary.LittleEndian.PutUint32(tmp[:4], uint32(f.Dim))
		tmp[4] = byte(f.Op)
		if f.Negate {
			tmp[5] = 1
		} else {
			tmp[5] = 0
		}
		binary.LittleEndian.PutUint16(tmp[6:8], uint16(len(f.Values)))
		mixBytes(tmp[:8])
		for _, v := range f.Values {
			binary.LittleEndian.PutUint32(tmp[:4], uint32(len(v)))
			mixBytes(tmp[:4])
			mixString(v)
		}
	}
	return h
}
