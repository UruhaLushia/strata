package strata

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"
)

func invertTestRecord(dst []byte, key uint64, mode byte, payload ...uint64) []byte {
	dst = append(binary.LittleEndian.AppendUint64(dst, key), mode)
	for _, v := range payload {
		dst = binary.AppendUvarint(dst, v)
	}
	return dst
}

func TestInvertVarintValidation(t *testing.T) {
	const maxID = 1<<32 - 1
	overflow := append(bytes.Repeat([]byte{0x80}, 9), 2)
	tooLong := append(bytes.Repeat([]byte{0x80}, 10), 0)
	for _, mode := range []byte{invertModeDeltaVarint, invertModeRLE} {
		for _, tail := range [][]byte{nil, {0x80}, overflow, tooLong} {
			bad := append(invertTestRecord(nil, 1, mode, 1), tail...)
			if err := decodeInvertChunkSorted(bad, 0, []uint32{2}, nil, nil); err == nil {
				t.Fatalf("skipped mode %d accepted malformed varint %x", mode, tail)
			}
		}
	}
	for _, bad := range [][]byte{
		invertTestRecord(nil, 1, invertModeDeltaVarint, 2, maxID, 1),
		invertTestRecord(nil, 1, invertModeRLE, 1, 0),
		invertTestRecord(nil, 1, invertModeRLE, 1, 0, 0),
		invertTestRecord(nil, 1, invertModeRLE, 1, maxID, 2),
		invertTestRecord(nil, 1, invertModeRLE, 2, maxID, 1, 0, 1),
	} {
		if err := decodeInvertChunkSorted(bad, 0, []uint32{2}, nil, nil); err == nil {
			t.Fatalf("skipped malformed posting %x", bad)
		}
	}
	plain := invertTestRecord(nil, 1, invertModeDeltaVarint, 3, 1, 128, 2)
	plain = invertTestRecord(plain, 2, invertModeRLE, 1, maxID, 1)
	plain = invertTestRecord(plain, 3, invertModeDeltaVarint, 3, 7, 2, 128)
	plain = invertTestRecord(plain, 4, invertModeRLE, 2, 4, 2, 3, 1)
	var got []uint32
	mustTestOK(t, decodeInvertChunkSorted(plain, 0, []uint32{3, 4}, func(_ uint64, id uint32) {
		got = append(got, id)
	}, nil))
	if !slices.Equal(got, []uint32{7, 9, 137, 4, 5, 9}) {
		t.Fatalf("postings after skipped keys = %v", got)
	}
}

func BenchmarkInvertSkipVarints(b *testing.B) {
	var plain []byte
	for k := uint64(1); k < 128; k += 2 {
		plain = invertTestRecord(plain, k, invertModeDeltaVarint, 8, 1, 4, 2, 7, 2, 5, 2, 3)
		plain = invertTestRecord(plain, k+1, invertModeRLE, 4, 3, 2, 10, 4, 1, 3, 5, 6)
	}
	plain = invertTestRecord(plain, 129, invertModeSingleton, 3)
	wanted := []uint32{129}
	b.ReportAllocs()
	for b.Loop() {
		if err := decodeInvertChunkSorted(plain, 0, wanted, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}
