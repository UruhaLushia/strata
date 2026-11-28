package codec_test

import (
	"bytes"
	stdflate "compress/flate"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"math/rand/v2"
	"path/filepath"
	"strings"
	"testing"

	"github.com/UruhaLushia/strata/codec"
	"github.com/UruhaLushia/strata/codec/flate"
	"github.com/UruhaLushia/strata/codec/s2"
	"github.com/UruhaLushia/strata/codec/zstd"
)

func TestRoundTrip(t *testing.T) {
	random := make([]byte, 12<<10)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range random {
		random[i] = byte(rng.Uint64())
	}
	inputs := [][]byte{nil, bytes.Repeat([]byte("abc"), 60), bytes.Repeat([]byte("abcd"), 6<<10), random}
	for _, tc := range []struct {
		name string
		id   uint8
		impl codec.Codec
	}{
		{"zstd-fastest", codec.IDZstd, zstd.New(zstd.Fastest)},
		{"zstd-default", codec.IDZstd, zstd.New(zstd.Default)},
		{"zstd-better", codec.IDZstd, zstd.New(zstd.Better)},
		{"zstd-best", codec.IDZstd, zstd.New(zstd.Best)},
		{"s2-default", codec.IDS2, s2.New(s2.Default)},
		{"s2-better", codec.IDS2, s2.New(s2.Better)},
		{"s2-best", codec.IDS2, s2.New(s2.Best)},
		{"flate-default", codec.IDFlate, flate.New(stdflate.DefaultCompression)},
		{"flate-fastest", codec.IDFlate, flate.New(stdflate.BestSpeed)},
		{"flate-none", codec.IDFlate, flate.New(stdflate.NoCompression)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.impl.ID(); got != tc.id {
				t.Fatalf("ID = %d, want %d", got, tc.id)
			}
			enc, err := tc.impl.NewEncoder(64 << 10)
			check(t, err)
			dec, err := tc.impl.NewDecoder()
			check(t, err)
			var encoded, snapshots, decoded [][]byte
			for _, raw := range inputs {
				packed, err := enc.Encode(make([]byte, 1, len(raw)*2+64), raw)
				check(t, err)
				encoded, snapshots = append(encoded, packed), append(snapshots, bytes.Clone(packed))
				got, err := dec.Decode(make([]byte, 0, len(raw)), packed, len(raw))
				check(t, err)
				decoded = append(decoded, got)
				if _, err := dec.Decode(nil, packed, -1); err == nil {
					t.Fatal("negative limit accepted")
				}
				if len(raw) != 0 {
					for _, limit := range []int{len(raw) - 1, 0} {
						if _, err := dec.Decode(nil, packed, limit); !errors.Is(err, codec.ErrTooLarge) {
							t.Fatalf("decode %d bytes with limit %d: %v", len(raw), limit, err)
						}
					}
				}
			}
			check(t, enc.Close())
			check(t, dec.Close())
			for i, raw := range inputs {
				if !bytes.Equal(decoded[i], raw) || !bytes.Equal(encoded[i], snapshots[i]) {
					t.Fatalf("incorrect or overwritten output for input %d (%d bytes)", i, len(raw))
				}
			}
		})
	}
}

func TestZstdConcatenatedLimit(t *testing.T) {
	c := zstd.New(zstd.Default)
	enc, err := c.NewEncoder(0)
	check(t, err)
	dec, err := c.NewDecoder()
	check(t, err)
	defer dec.Close()
	raw := bytes.Repeat([]byte("pair"), 12<<10)
	frame, err := enc.Encode(nil, raw)
	check(t, err)
	check(t, enc.Close())
	joined := append(bytes.Clone(frame), frame...)
	const limit = 80 << 10
	for _, dst := range [][]byte{nil, make([]byte, 0, limit)} {
		if _, err := dec.Decode(dst, joined, limit); !errors.Is(err, codec.ErrTooLarge) {
			t.Fatalf("concatenated decode with dst capacity %d: %v", cap(dst), err)
		}
	}
	got, err := dec.Decode(nil, joined, 2*len(raw))
	check(t, err)
	if !bytes.Equal(got, bytes.Repeat(raw, 2)) {
		t.Fatal("incorrect concatenated output at exact limit")
	}
}

func TestPackageBoundaries(t *testing.T) {
	for _, dir := range []string{".", "zstd", "s2", "flate"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		check(t, err)
		for _, name := range files {
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
			check(t, err)
			for _, imp := range f.Imports {
				p := strings.Trim(imp.Path.Value, "\"")
				if strings.Contains("/"+p, "/internal/") || dir == "." && p != "errors" ||
					dir == "flate" && p != "bytes" && p != "compress/flate" && p != "io" && p != "github.com/UruhaLushia/strata/codec" {
					t.Errorf("%s imports %s", name, p)
				}
			}
			ast.Inspect(f, func(node ast.Node) bool {
				var api ast.Node
				switch n := node.(type) {
				case *ast.FuncDecl:
					if n.Name.IsExported() {
						api = n.Type
					}
				case *ast.TypeSpec:
					if n.Name.IsExported() {
						api = n.Type
					}
				}
				if api != nil {
					ast.Inspect(api, func(n ast.Node) bool {
						if id, ok := n.(*ast.Ident); ok && id.Name == "any" {
							t.Errorf("%s: any in exported API", name)
						}
						if _, ok := n.(*ast.InterfaceType); ok && n != api {
							t.Errorf("%s: anonymous interface in exported API", name)
						}
						return true
					})
				}
				return true
			})
		}
	}
}

func TestInvalidLevels(t *testing.T) {
	for _, tc := range []struct {
		name string
		c    codec.Codec
		want error
	}{
		{"zstd", zstd.New(99), codec.ErrUnsupportedCodec},
		{"s2", s2.New(99), codec.ErrUnsupportedCodec},
		{"flate", flate.New(99), nil},
	} {
		enc, err := tc.c.NewEncoder(0)
		if err == nil {
			_ = enc.Close()
			t.Fatalf("%s: expected error for invalid level", tc.name)
		}
		if tc.want != nil && !errors.Is(err, tc.want) {
			t.Fatalf("%s: error = %v, want %v", tc.name, err, tc.want)
		}
	}
}

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
