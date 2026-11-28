// Package zstd provides a Zstandard block codec.
package zstd

import (
	"bytes"
	"errors"
	"io"
	"math/bits"

	"github.com/UruhaLushia/strata/codec"
	"github.com/klauspost/compress/zstd"
)

// Level selects a Zstandard compression preset.
type Level uint8

const (
	Fastest Level = iota
	Default
	Better
	Best
)

// New returns a Zstandard codec for level; invalid levels fail in NewEncoder with codec.ErrUnsupportedCodec.
func New(level Level) codec.Codec { return algorithm(level) }

type algorithm Level

func (algorithm) ID() uint8 { return codec.IDZstd }

func (a algorithm) NewEncoder(windowBytes int) (codec.Encoder, error) {
	levels := [...]zstd.EncoderLevel{zstd.SpeedFastest, zstd.SpeedDefault, zstd.SpeedBetterCompression, zstd.SpeedBestCompression}
	if int(a) >= len(levels) {
		return nil, codec.ErrUnsupportedCodec
	}
	if windowBytes <= 0 {
		windowBytes = 128 << 10
	}
	windowBytes = min(max(windowBytes, 1<<10), 8<<20)
	windowBytes = 1 << (bits.Len(uint(windowBytes)) - 1)
	w, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(levels[a]), zstd.WithWindowSize(windowBytes))
	if err != nil {
		return nil, err
	}
	return encoder{w}, nil
}

func (algorithm) NewDecoder() (codec.Decoder, error) {
	r, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxWindow(8<<20), zstd.WithDecodeAllCapLimit(true))
	if err != nil {
		return nil, err
	}
	return decoder{r}, nil
}

type encoder struct{ w *zstd.Encoder }

func (e encoder) Encode(dst, src []byte) ([]byte, error) {
	return e.w.EncodeAll(src, dst[:0]), nil
}

func (e encoder) Close() error { return e.w.Close() }

type decoder struct{ r *zstd.Decoder }

func (d decoder) Decode(dst, src []byte, maxRaw int) ([]byte, error) {
	if maxRaw < 0 {
		return nil, codec.ErrTooLarge
	}
	if cap(dst) < maxRaw && maxRaw <= 64<<10 {
		dst = make([]byte, 0, maxRaw)
	}
	if maxRaw > 0 && cap(dst) >= maxRaw {
		out, err := d.r.DecodeAll(src, dst[:0:maxRaw])
		if len(out) > maxRaw || errors.Is(err, zstd.ErrDecoderSizeExceeded) {
			return nil, codec.ErrTooLarge
		}
		if err == nil {
			return out, nil
		}
		// The capped decoder can return a block error when a frame has no content size.
		// The bounded stream distinguishes that case from corrupt input.
	}
	if err := d.r.Reset(bytes.NewReader(src)); err != nil {
		return nil, err
	}
	defer func() { _ = d.r.Reset(nil) }()
	out := bytes.NewBuffer(dst[:0])
	limit := int64(min(uint64(maxRaw)+1, 1<<63-1))
	_, err := out.ReadFrom(io.LimitReader(d.r, limit))
	if out.Len() > maxRaw || errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		return nil, codec.ErrTooLarge
	}
	return out.Bytes(), err
}

func (d decoder) Close() error {
	d.r.Close()
	return nil
}
