// Package codec defines independent block compression interfaces.
package codec

import "errors"

// Built-in codec IDs stored in frame headers.
const (
	IDNone  uint8 = 0
	IDZstd  uint8 = 1
	IDS2    uint8 = 2
	IDFlate uint8 = 3
)

var (
	// ErrUnsupportedCodec reports an unknown codec ID or invalid codec level.
	ErrUnsupportedCodec = errors.New("codec: unsupported codec")
	// ErrTooLarge reports decoded output exceeding the caller's maxRaw limit.
	ErrTooLarge = errors.New("codec: decoded output exceeds limit")
)

// Codec is a concurrency-safe factory for one compression algorithm.
// IDs 1..3 are built in, 4..127 are reserved, and 128..255 are available for custom codecs.
type Codec interface {
	ID() uint8
	// NewEncoder returns a new block encoder; windowBytes <= 0 uses the default, and windowless codecs ignore it.
	NewEncoder(windowBytes int) (Encoder, error)
	NewDecoder() (Decoder, error)
}

// Encoder compresses independent blocks. Individual instances are not concurrency-safe.
// Encode may reuse dst[:0] when dst does not alias src. It retains neither slice.
type Encoder interface {
	Encode(dst, src []byte) ([]byte, error)
	Close() error
}

// Decoder decompresses independent blocks. Individual instances are not concurrency-safe.
// Decode bounds the full output by maxRaw. Zero allows only empty output, and
// a negative limit returns an error. Output belongs to the caller and remains
// valid across later Decode or Close calls unless reused as dst.
type Decoder interface {
	Decode(dst, src []byte, maxRaw int) ([]byte, error)
	Close() error
}
