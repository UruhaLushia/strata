package strata

import (
	"context"
	"encoding/binary"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unsafe"
)

func testDB(t *testing.T, now *time.Time, extra Options) (*DB, *Series) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "strata.db")
	extra.Now = func() time.Time { return *now }
	db, err := Open(path, extra)
	if err != nil {
		t.Fatal(err)
	}
	cfg := SeriesConfig{Arity: 2, SummaryOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast}, RowOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast}}
	s, err := db.Series(7, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return db, s
}

func appendValue(t *testing.T, s *Series, ts time.Time, value int64) {
	t.Helper()
	if err := s.AppendSync(ts, Summary{Values: [4]int64{value, value, value, value}}, Row{Tags: []string{"a", "b"}, Values: [4]int64{value, value, value, value}}); err != nil {
		t.Fatal(err)
	}
}

func TestNoInternalImports(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		f, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			if strings.Contains(p, "/internal/") {
				t.Fatalf("internal import in %s: %s", name, p)
			}
		}
	}
}

func TestExportedAPIZeroAny(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range f.Decls {
			d, ok := decl.(*ast.FuncDecl)
			if !ok || !d.Name.IsExported() {
				continue
			}
			ast.Inspect(d.Type, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id.Name == "any" {
					t.Errorf("exported func %s.%s uses any", name, d.Name.Name)
				}
				if _, ok := n.(*ast.InterfaceType); ok {
					t.Errorf("exported func %s.%s uses interface{}", name, d.Name.Name)
				}
				return true
			})
		}
	}
}

func TestSuperblockLayoutAndFeatures(t *testing.T) {
	var s superblock
	s.version = 1
	s.gen = 3
	s.walOffset = dataStart
	raw := encodeSlot(s)
	if len(raw) != slotSize || string(raw[:8]) != magic {
		t.Fatal("bad slot")
	}
	if got := binary.LittleEndian.Uint16(raw[200:202]); got != 0 {
		t.Fatalf("sections moved: %d", got)
	}
	if crc(raw[:slotSize-4]) != binary.LittleEndian.Uint32(raw[slotSize-4:]) {
		t.Fatal("bad slot crc")
	}
	if unsafe.Sizeof(idRow{}) != 72 || unsafe.Sizeof(sealedMeta{}) != 80 || unsafe.Sizeof(openAddrSlot{}) != 40 {
		t.Fatalf("unexpected struct sizes: idRow=%d sealedMeta=%d openAddrSlot=%d",
			unsafe.Sizeof(idRow{}), unsafe.Sizeof(sealedMeta{}), unsafe.Sizeof(openAddrSlot{}))
	}
}

func TestOptionalSectionsAndFeatureErrors(t *testing.T) {
	now := time.Now().UTC()
	db, _ := testDB(t, &now, Options{})
	path := db.path
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	slot := make([]byte, slotSize)
	if _, err = f.ReadAt(slot, 0); err != nil {
		t.Fatal(err)
	}
	payload := []byte("future-optional")
	info, _ := f.Stat()
	off := info.Size()
	if _, err = f.WriteAt(payload, off); err != nil {
		t.Fatal(err)
	}
	p := 200 + 7*32
	binary.LittleEndian.PutUint16(slot[p:p+2], 99)
	slot[p+3] = 1
	binary.LittleEndian.PutUint64(slot[p+8:p+16], uint64(off))
	binary.LittleEndian.PutUint64(slot[p+16:p+24], uint64(len(payload)))
	binary.LittleEndian.PutUint32(slot[p+24:p+28], uint32(len(payload)))
	binary.LittleEndian.PutUint32(slot[p+28:p+32], crc(payload))
	binary.LittleEndian.PutUint32(slot[slotSize-4:], crc(slot[:slotSize-4]))
	_, _ = f.WriteAt(slot, 0)
	_, _ = f.WriteAt(slot, slotSize)
	_ = f.Close()

	db, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(db.optional) != 1 {
		t.Fatalf("optional section lost: %d", len(db.optional))
	}
	if err = db.Compact(); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	db, err = Open(path, Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(db.optional) != 1 || string(db.optional[0].data) != string(payload) {
		t.Fatalf("optional section changed: %+v", db.optional)
	}
	_ = db.Close()

	// roCompat bit rejects writer; incompat bit rejects reader.
	patchFeature := func(byteOff int) {
		rf, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer rf.Close()
		if _, err = rf.ReadAt(slot, 0); err != nil {
			t.Fatal(err)
		}
		binary.LittleEndian.PutUint32(slot[byteOff:byteOff+4], 1)
		binary.LittleEndian.PutUint32(slot[slotSize-4:], crc(slot[:slotSize-4]))
		_, _ = rf.WriteAt(slot, 0)
		_, _ = rf.WriteAt(slot, slotSize)
	}
	patchFeature(16)
	if _, err = Open(path, Options{}); err != ErrReadOnlyCompat {
		t.Fatalf("read-only compat error: %v", err)
	}
	patchFeature(20)
	if _, err = Open(path, Options{ReadOnly: true}); err != ErrIncompatFormat {
		t.Fatalf("incompat error: %v", err)
	}
}

func TestWALDurabilityAndTornTail(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Minute)
	db, s := testDB(t, &now, Options{})
	ts := now.Add(-time.Minute)
	appendValue(t, s, ts, 0)
	appendValue(t, s, ts, 5)
	if db.appendSeq == 0 || db.durableSeq != db.appendSeq {
		t.Fatal("sync sequence did not advance")
	}
	path := db.path
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("UDW1\x80"))
	_ = f.Close()

	db, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err = db.Series(7, SeriesConfig{Arity: 2, SummaryOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast}, RowOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast}})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.Aggregate(context.Background(), AggregateQuery{From: ts, To: ts.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	defer res.Release()
	if res.Summary.Values[0] != 5 || res.Summary.Values[1] != 0 || res.Summary.Values[3] != 5 {
		t.Fatalf("bad recovery: %+v", res.Summary)
	}
	before, _ := os.Stat(path)
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(path)
	if before.Size() != after.Size() {
		t.Fatalf("idle Flush changed file size: %d -> %d", before.Size(), after.Size())
	}
}

func TestResetAtomicityAndRollback(t *testing.T) {
	now := time.Date(2026, 1, 2, 10, 0, 5, 0, time.UTC)
	db, s := testDB(t, &now, Options{})
	defer db.Close()
	old := s.resetEpoch
	appendValue(t, s, now, 100)
	now = now.Add(25 * time.Second)
	if err := s.ResetCurrentDay(time.UTC); err != nil {
		t.Fatal(err)
	}
	if s.resetEpoch != old+1 {
		t.Fatal("reset epoch did not advance")
	}
	appendValue(t, s, now.Add(15*time.Second), 200)
	res, err := s.Aggregate(context.Background(), AggregateQuery{From: now.Truncate(time.Minute), To: now.Truncate(time.Minute).Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	res.Release()
	if res.Summary.Values[0] != 200 {
		t.Fatalf("reset leaked old value: %+v", res.Summary)
	}

	oldEpoch, oldPos, oldSeq := s.resetEpoch, db.walPos, db.appendSeq
	db.syncHook = func() error { return os.ErrInvalid }
	if err := s.ResetCurrentDay(time.UTC); err != os.ErrInvalid {
		t.Fatalf("sync failure = %v", err)
	}
	db.syncHook = nil
	if s.resetEpoch != oldEpoch || db.walPos != oldPos || db.appendSeq != oldSeq || db.allocatedSeq != oldSeq {
		t.Fatalf("reset rollback failed: epoch=%d pos=%d seq=%d alloc=%d", s.resetEpoch, db.walPos, db.appendSeq, db.allocatedSeq)
	}
}

func TestRejectLegacyFormats(t *testing.T) {
	base := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	clock := base.Add(4 * time.Hour)
	newCompacted := func(t *testing.T, name string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), name)
		db, err := Open(path, Options{Now: func() time.Time { return clock }})
		if err != nil {
			t.Fatal(err)
		}
		s, err := db.Series(41, SeriesConfig{Arity: 2})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Append(base, Summary{Values: [4]int64{10, 20, 1, 0}}, Row{
			Tags: []string{"host-a", "tcp"}, Values: [4]int64{10, 20, 1, 0},
		}); err != nil {
			t.Fatal(err)
		}
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		return path
	}

	t.Run("Legacy64BIndex", func(t *testing.T) {
		path := newCompacted(t, "legacy64.db")
		f, err := openData(path, false)
		if err != nil {
			t.Fatal(err)
		}
		head, err := readSuper(f)
		if err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		idxd := head.sections[secTSIndex]
		if idxd.count == 0 {
			_ = f.Close()
			t.Fatal("expected non-empty SecTSIndex")
		}
		raw, err := sectionRaw(f, idxd, nil)
		if err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		raw, err = zstdDecompress(raw)
		if err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		n := int(idxd.count)
		legacy := make([]byte, n*64)
		for i := range n {
			copy(legacy[i*64:i*64+52], raw[i*104:i*104+52])
		}
		enc := newZstdEncoder(CompressionDefault, 64<<10)
		comp := enc.EncodeAll(legacy, nil)
		enc.Close()
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		off := info.Size()
		if _, err := f.WriteAt(comp, off); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		head.sections[secTSIndex].offset = uint64(off)
		head.sections[secTSIndex].length = uint64(len(comp))
		head.sections[secTSIndex].rawLen = uint32(len(legacy))
		head.sections[secTSIndex].crc = crc(comp)
		head.gen++
		if err := writeSupers(f, head); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if err := syncData(f); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		_ = f.Close()

		ro, err := Open(path, Options{ReadOnly: true, Now: func() time.Time { return clock }})
		if err == nil {
			_ = ro.Close()
			t.Fatal("expected Open to reject legacy 64B TS index")
		}
		if !strings.Contains(err.Error(), "invalid TS index entry size") {
			t.Fatalf("unexpected error for legacy 64B TS index: %v", err)
		}
	})

	t.Run("LegacyDictCodec", func(t *testing.T) {
		path := newCompacted(t, "legacy_dict.db")
		f, err := openData(path, false)
		if err != nil {
			t.Fatal(err)
		}
		head, err := readSuper(f)
		if err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if head.sections[secTSDict].length == 0 {
			_ = f.Close()
			t.Fatal("expected non-empty SecTSDict")
		}
		head.sections[secTSDict].codec = 1
		head.gen++
		if err := writeSupers(f, head); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if err := syncData(f); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		_ = f.Close()

		ro, err := Open(path, Options{ReadOnly: true, Now: func() time.Time { return clock }})
		if err == nil {
			_ = ro.Close()
			t.Fatal("expected Open to reject SecTSDict codec != 2")
		}
	})

	t.Run("LegacyRowMajorBlock", func(t *testing.T) {
		plain := make([]byte, 68+1+65)
		binary.LittleEndian.PutUint16(plain[0:2], 7)
		plain[2] = resMinuteHR
		plain[35] = 0x0f
		plain[68] = 1
		binary.LittleEndian.PutUint32(plain[69:73], 1)
		plain[69+64] = 0x0f
		enc := newZstdEncoder(CompressionDefault, 64<<10)
		raw := enc.EncodeAll(plain, nil)
		enc.Close()
		_, err := decodeBlock(raw)
		if err == nil {
			t.Fatal("expected decodeBlock to reject legacy row-major block")
		}
		if !strings.Contains(err.Error(), "invalid block marker") {
			t.Fatalf("unexpected error for legacy row-major block: %v", err)
		}
		slot := newQuerySlot()
		defer slot.close()
		if _, _, err = decodeSmallBlockCols(raw, slot); err == nil {
			t.Fatal("expected decodeSmallBlockCols to reject legacy row-major block")
		} else if !strings.Contains(err.Error(), "invalid small column block") {
			t.Fatalf("unexpected error from decodeSmallBlockCols: %v", err)
		}
	})

	t.Run("LegacyZstdWrappedStreamBlock", func(t *testing.T) {
		streamRaw, err := encodeBlockLevel(sealedBlock{
			series: 7, res: resMinuteHR, rows: make([]idRow, 256),
		}, CompressionFastest)
		if err != nil {
			t.Fatal(err)
		}
		enc := newZstdEncoder(CompressionFastest, 64<<10)
		raw := enc.EncodeAll(streamRaw[len(blockStreamsMagic):], nil)
		enc.Close()
		if _, err = decodeBlock(raw); err == nil {
			t.Fatal("expected decodeBlock to reject Zstd-wrapped stream block")
		} else if !strings.Contains(err.Error(), "invalid block marker") {
			t.Fatalf("unexpected error for Zstd-wrapped stream block: %v", err)
		}
	})

	t.Run("LegacyDeltaMagic", func(t *testing.T) {
		legacy := []byte{0x00, 0xd2, 0x8f, 0x53, 0x32, 0x00}
		if _, _, err := decodeDelta(legacy); err == nil {
			t.Fatal("expected decodeDelta to reject legacy deltaMagic")
		}
		if _, _, _, err := scanDeltaBounds(legacy); err == nil {
			t.Fatal("expected scanDeltaBounds to reject legacy deltaMagic")
		}
	})

	t.Run("LegacyKVBatchAndTruncatedKey", func(t *testing.T) {
		cur := encodeKVBatch(1, []kvOp{{kind: kvPut, region: 1, key: []byte("k"), value: []byte("v")}})
		legacy := append(append(append([]byte(nil), cur[:8]...), 0xa9, 0x4b, 0x56, 0x32), cur[8:]...)
		if _, _, err := decodeKVBatch(legacy); err == nil {
			t.Fatal("expected decodeKVBatch to reject legacy kvBatchMagic")
		}

		truncated := binary.LittleEndian.AppendUint64(nil, 1)
		truncated = binary.AppendUvarint(truncated, 1)
		truncated = append(truncated, kvPut)
		truncated = binary.LittleEndian.AppendUint16(truncated, 1)
		truncated = binary.LittleEndian.AppendUint64(truncated, 0)
		truncated = binary.AppendUvarint(truncated, 5)
		truncated = binary.AppendUvarint(truncated, 0)
		truncated = append(truncated, "abcd"...)
		if _, _, err := decodeKVBatch(truncated); err == nil {
			t.Fatal("expected decodeKVBatch to reject truncated key without underflow")
		}
	})

	t.Run("LegacyKVBlockMagic", func(t *testing.T) {
		enc := newZstdEncoder(CompressionFastest, 64<<10)
		defer enc.Close()
		cur := encodeKVBlockWithEncoder([]kvSSTEntry{{region: 1, key: []byte("k"), value: []byte("v"), modRev: 1}}, enc)
		plain, err := zstdDecompress(cur)
		if err != nil {
			t.Fatal(err)
		}
		legacy := enc.EncodeAll(append([]byte{0xb7, 0x4b, 0x56, 0x32}, plain...), nil)
		if _, err = decodeKVBlock(legacy); err == nil {
			t.Fatal("expected decodeKVBlock to reject legacy kvBlockMagic")
		}
	})
}
