package strata

import (
	"errors"

	"github.com/UruhaLushia/strata/codec"
)

const maxSectionRawBytes = 64 << 20

type codecTable struct {
	factories [256]codec.Codec
	target    uint8
}

func newCodecTable(opts Options) (*codecTable, error) {
	t := &codecTable{}
	add := func(c codec.Codec) error {
		if c == nil {
			return errors.New("strata: nil codec")
		}
		id := c.ID()
		if id == codec.IDNone || id > codec.IDFlate && id < 128 {
			return codec.ErrUnsupportedCodec
		}
		if t.factories[id] != nil {
			return errors.New("strata: duplicate codec ID")
		}
		t.factories[id] = c
		return nil
	}
	if opts.Codec != nil {
		if err := add(opts.Codec); err != nil {
			return nil, err
		}
		t.target = opts.Codec.ID()
	}
	for _, c := range opts.Codecs {
		if err := add(c); err != nil {
			return nil, err
		}
	}
	return t, nil
}

func (t *codecTable) checkSections(head superblock) error {
	for _, d := range head.sections[:secTSInvert+1] {
		if d.kind <= secTSInvert && d.length != 0 && d.codec != codec.IDNone && t.factories[d.codec] == nil {
			return codec.ErrUnsupportedCodec
		}
	}
	return nil
}

func (t *codecTable) newEncoder(window int) (codec.Encoder, error) {
	if t.target == codec.IDNone {
		return nil, nil
	}
	return t.factories[t.target].NewEncoder(window)
}

func (t *codecTable) newDecoder(id uint8) (codec.Decoder, error) {
	if id == codec.IDNone || t == nil || t.factories[id] == nil {
		return nil, codec.ErrUnsupportedCodec
	}
	return t.factories[id].NewDecoder()
}

func (t *codecTable) decode(id uint8, dst, src []byte, maxRaw int) ([]byte, error) {
	dec, err := t.newDecoder(id)
	if err != nil {
		return nil, err
	}
	defer dec.Close()
	return dec.Decode(dst, src, maxRaw)
}

func encodePayload(enc codec.Encoder, dst, src []byte) ([]byte, error) {
	if enc == nil {
		return src, nil
	}
	return enc.Encode(dst, src)
}

func encodeSmaller(enc codec.Encoder, dst, src []byte, maxRaw int) ([]byte, bool, error) {
	if len(src) > maxRaw {
		return src, false, nil
	}
	out, err := encodePayload(enc, dst, src)
	if err != nil {
		return nil, false, err
	}
	if len(out) < len(src) {
		return out, true, nil
	}
	return src, false, nil
}
