// Package s2 provides an S2 block codec.
package s2

import (
	"github.com/UruhaLushia/strata/codec"
	"github.com/klauspost/compress/s2"
)

// Level selects an S2 compression preset.
type Level uint8

const (
	Default Level = iota
	Better
	Best
)

// New returns an S2 codec for level; invalid levels fail in NewEncoder with codec.ErrUnsupportedCodec.
func New(level Level) codec.Codec { return algorithm(level) }

type algorithm Level

func (algorithm) ID() uint8 { return codec.IDS2 }

func (a algorithm) NewEncoder(int) (codec.Encoder, error) {
	levels := [...]encoder{s2.Encode, s2.EncodeBetter, s2.EncodeBest}
	if int(a) >= len(levels) {
		return nil, codec.ErrUnsupportedCodec
	}
	return levels[a], nil
}

func (algorithm) NewDecoder() (codec.Decoder, error) { return decoder{}, nil }

type encoder func(dst, src []byte) []byte

func (e encoder) Encode(dst, src []byte) ([]byte, error) {
	n := s2.MaxEncodedLen(len(src))
	if n < 0 {
		return nil, s2.ErrTooLarge
	}
	if cap(dst) >= n {
		return e(dst[:n], src), nil
	}
	return e(nil, src), nil
}

func (encoder) Close() error { return nil }

type decoder struct{}

func (decoder) Decode(dst, src []byte, maxRaw int) ([]byte, error) {
	if maxRaw < 0 {
		return nil, codec.ErrTooLarge
	}
	n, err := s2.DecodedLen(src)
	if err != nil {
		return nil, err
	}
	if n > maxRaw {
		return nil, codec.ErrTooLarge
	}
	if cap(dst) >= n {
		dst = dst[:n]
	}
	out, err := s2.Decode(dst, src)
	if len(out) > maxRaw {
		return nil, codec.ErrTooLarge
	}
	return out, err
}

func (decoder) Close() error { return nil }
