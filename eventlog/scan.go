package eventlog

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"slices"
)

type ScanOptions struct {
	MaxRecords int   // Nonpositive values disable the record limit.
	MaxBytes   int64 // Nonpositive values disable the frame byte limit.
}

type ScanResult struct {
	Records      int
	BytesRead    int64 // Whole frame bytes read, including headers and trailers.
	Stopped      bool
	LimitReached bool
	Truncated    bool
}

const scanWindowBytes = 64 << 10

type scanSlot struct {
	buf     []byte
	offsets []uint32
}

func (s *scanSlot) read(f *os.File, start, end int64) error {
	n := int(end - start)
	if cap(s.buf) < n {
		s.buf = make([]byte, n, max(n, min(2*cap(s.buf), scanWindowBytes)))
	}
	s.buf = s.buf[:n]
	return readAt(f, s.buf, start)
}

// Scan visits flushed records in reverse append order. The callback must not
// call Close or wait for a nested Scan that could exhaust MaxConcurrentScans.
func (l *Log) Scan(ctx context.Context, opts ScanOptions, fn func(Record) error) (ScanResult, error) {
	if fn == nil {
		return ScanResult{}, ErrInvalidOption
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ScanResult{}, err
	}
	var slot *scanSlot
	select {
	case <-ctx.Done():
		return ScanResult{}, ctx.Err()
	case <-l.closing:
		return ScanResult{}, ErrClosed
	case slot = <-l.scanSem:
	}
	l.mu.Lock()
	if l.closed {
		// Close owns the remaining slots once scans can no longer register.
		l.mu.Unlock()
		return ScanResult{}, ErrClosed
	}
	l.scans.Add(1)
	shards := slices.Clone(l.shards)
	l.mu.Unlock()
	if slot == nil {
		slot = new(scanSlot)
	}
	defer func() {
		if cap(slot.buf) > scanWindowBytes {
			slot.buf = nil
		}
		if cap(slot.offsets) > 1024 {
			slot.offsets = nil
		}
		l.scanSem <- slot
		l.scans.Done()
	}()

	var result ScanResult
	for _, s := range slices.Backward(shards) {
		if s.size == fileHeaderSize {
			continue
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if opts.MaxRecords > 0 && result.Records == opts.MaxRecords ||
			opts.MaxBytes > 0 && opts.MaxBytes-result.BytesRead < minFrameSize {
			result.LimitReached = true
			return result, nil
		}
		f, err := openFile(l.shardPath(s.id), os.O_RDONLY)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || !l.published(s.id) {
				result.Truncated = true
				return result, nil
			}
			return result, err
		}
		err = scanShard(ctx, f, s, opts, fn, &result, slot)
		if err != nil || result.LimitReached || result.Stopped {
			return result, err
		}
	}
	return result, ctx.Err()
}

func (l *Log) published(id uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.ContainsFunc(l.shards, func(s shard) bool { return s.id == id })
}

func scanShard(ctx context.Context, f *os.File, s shard, opts ScanOptions, fn func(Record) error, r *ScanResult, slot *scanSlot) (err error) {
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()
	winStart := s.size
	if s.size <= scanWindowBytes {
		winStart = 0
		if err := slot.read(f, 0, s.size); err != nil {
			return err
		}
		if !validHeader(slot.buf, s.id) {
			return ErrCorrupt
		}
	} else if err := checkHeader(f, s.id); err != nil {
		return err
	}
	for end := s.size; end > fileHeaderSize; {
		if err := ctx.Err(); err != nil {
			return err
		}
		if opts.MaxRecords > 0 && r.Records == opts.MaxRecords ||
			opts.MaxBytes > 0 && opts.MaxBytes-r.BytesRead < minFrameSize {
			r.LimitReached = true
			return nil
		}
		if end-fileHeaderSize < minFrameSize {
			return ErrCorrupt
		}
		if end-trailerSize < winStart {
			winStart = max(fileHeaderSize, end-scanWindowBytes)
			if err := slot.read(f, winStart, end); err != nil {
				return err
			}
		}
		trailer := slot.buf[end-winStart-trailerSize : end-winStart]
		size := int64(binary.LittleEndian.Uint32(trailer))
		if string(trailer[4:]) != tailMagic || size < minFrameSize || size > maxFrameBytes || size > end-fileHeaderSize {
			return ErrCorrupt
		}
		if opts.MaxBytes > 0 && size > opts.MaxBytes-r.BytesRead {
			r.LimitReached = true
			return nil
		}
		start := end - size
		if start < winStart {
			winStart = max(fileHeaderSize, end-max(size, scanWindowBytes))
			if err := slot.read(f, winStart, end); err != nil {
				return err
			}
		}
		frame := slot.buf[start-winStart : end-winStart]
		end = start
		r.BytesRead += size
		if err := decodeFrame(frame, &slot.offsets); err != nil {
			return err
		}
		for _, off := range slices.Backward(slot.offsets) {
			if err := ctx.Err(); err != nil {
				return err
			}
			if opts.MaxRecords > 0 && r.Records == opts.MaxRecords {
				r.LimitReached = true
				return nil
			}
			pos := int(off)
			header := frame[pos : pos+recordHeaderSize]
			start := pos + recordHeaderSize
			end := start + int(binary.LittleEndian.Uint32(header[8:]))
			r.Records++
			err := fn(Record{
				TimestampNS: int64(binary.LittleEndian.Uint64(header)),
				StreamID:    binary.LittleEndian.Uint16(header[12:]),
				Payload:     frame[start:end:end],
			})
			if errors.Is(err, ErrStopScan) {
				r.Stopped = true
				return nil
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}
