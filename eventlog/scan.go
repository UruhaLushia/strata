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
	select {
	case <-ctx.Done():
		return ScanResult{}, ctx.Err()
	case <-l.closing:
		return ScanResult{}, ErrClosed
	case l.scanSem <- struct{}{}:
	}
	defer func() { <-l.scanSem }()
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return ScanResult{}, ErrClosed
	}
	l.scans.Add(1)
	shards := slices.Clone(l.shards)
	l.mu.Unlock()
	defer l.scans.Done()

	var result ScanResult
	var raw []byte
	var offsets []uint32
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
		err = scanShard(ctx, f, s, opts, fn, &result, &raw, &offsets)
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

func scanShard(ctx context.Context, f *os.File, s shard, opts ScanOptions, fn func(Record) error, r *ScanResult, raw *[]byte, offsets *[]uint32) (err error) {
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()
	if err := checkHeader(f, s.id); err != nil {
		return err
	}
	var trailer [trailerSize]byte
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
		if err := readAt(f, trailer[:], end-trailerSize); err != nil {
			return err
		}
		size := int64(binary.LittleEndian.Uint32(trailer[:]))
		if string(trailer[4:]) != tailMagic || size < minFrameSize || size > maxFrameBytes || size > end-fileHeaderSize {
			return ErrCorrupt
		}
		if opts.MaxBytes > 0 && size > opts.MaxBytes-r.BytesRead {
			r.LimitReached = true
			return nil
		}
		end -= size
		*raw = resizeFrame(*raw, int(size))
		bodyEnd := int(size) - trailerSize
		if err := readAt(f, (*raw)[:bodyEnd], end); err != nil {
			return err
		}
		copy((*raw)[bodyEnd:], trailer[:])
		r.BytesRead += size
		if err := decodeFrame(*raw, offsets); err != nil {
			return err
		}
		for _, off := range slices.Backward(*offsets) {
			if err := ctx.Err(); err != nil {
				return err
			}
			if opts.MaxRecords > 0 && r.Records == opts.MaxRecords {
				r.LimitReached = true
				return nil
			}
			pos := int(off)
			header := (*raw)[pos : pos+recordHeaderSize]
			start := pos + recordHeaderSize
			end := start + int(binary.LittleEndian.Uint32(header[8:]))
			r.Records++
			err := fn(Record{
				TimestampNS: int64(binary.LittleEndian.Uint64(header)),
				StreamID:    binary.LittleEndian.Uint16(header[12:]),
				Payload:     (*raw)[start:end:end],
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
