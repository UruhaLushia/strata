// Package flate provides a raw DEFLATE block codec.
package flate

import (
	"bytes"
	"compress/flate"
	"io"

	"github.com/UruhaLushia/strata/codec"
)

// New returns a raw DEFLATE codec using a standard compress/flate level.
func New(level int) codec.Codec { return algorithm(level) }

type algorithm int

func (algorithm) ID() uint8 { return codec.IDFlate }

func (a algorithm) NewEncoder(int) (codec.Encoder, error) {
	w, err := flate.NewWriter(io.Discard, int(a))
	if err != nil {
		return nil, err
	}
	return &encoder{w}, nil
}

func (algorithm) NewDecoder() (codec.Decoder, error) {
	d := &decoder{}
	d.r = flate.NewReader(&d.src)
	return d, nil
}

type sliceWriter []byte

func (w *sliceWriter) Write(p []byte) (int, error) {
	*w = append(*w, p...)
	return len(p), nil
}

type encoder struct{ w *flate.Writer }

func (e *encoder) Encode(dst, src []byte) ([]byte, error) {
	out := sliceWriter(dst[:0])
	e.w.Reset(&out)
	defer e.w.Reset(io.Discard)
	if _, err := e.w.Write(src); err != nil {
		return nil, err
	}
	err := e.w.Close()
	return out, err
}

func (e *encoder) Close() error {
	e.w = nil
	return nil
}

type decoder struct {
	src bytes.Reader
	r   io.ReadCloser
}

func (d *decoder) Decode(dst, src []byte, maxRaw int) ([]byte, error) {
	if maxRaw < 0 {
		return nil, codec.ErrTooLarge
	}
	d.src.Reset(src)
	defer func() {
		_ = d.r.Close()
		d.src.Reset(nil)
	}()
	if err := d.r.(flate.Resetter).Reset(&d.src, nil); err != nil {
		return nil, err
	}
	out := bytes.NewBuffer(dst[:0])
	limit := int64(min(uint64(maxRaw)+1, 1<<63-1))
	_, err := out.ReadFrom(io.LimitReader(d.r, limit))
	if out.Len() > maxRaw {
		return nil, codec.ErrTooLarge
	}
	return out.Bytes(), err
}

func (d *decoder) Close() error {
	d.r = nil
	return nil
}
