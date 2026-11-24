package strata

import (
	"context"
	"encoding/binary"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
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

func TestResetAllCompactLifecycle(t *testing.T) {
	t.Run("SealedLifecycleAndRetention", func(t *testing.T) {
		now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
		opts := Options{
			Retention1m: 6 * time.Hour,
			Retention1h: 7 * 24 * time.Hour,
			Retention1d: 30 * 24 * time.Hour,
			Now:         func() time.Time { return now },
		}
		path := filepath.Join(t.TempDir(), "reset_all_lifecycle.db")
		db, err := Open(path, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if db != nil {
				_ = db.Close()
			}
		}()
		cfg := SeriesConfig{Arity: 2, SummaryOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast}, RowOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast}}
		s, err := db.Series(7, cfg)
		if err != nil {
			t.Fatal(err)
		}

		// Seed epoch=0 data covering 1m, 1h, and 1d sealed blocks.
		tsOldDay := now.Add(-72 * time.Hour)
		tsOldHour := now.Add(-2 * time.Hour)
		appendValue(t, s, tsOldDay, 10)
		appendValue(t, s, tsOldHour, 20)
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
		var has1m, has1h, has1d bool
		for _, m := range db.sealed {
			switch m.res {
			case resMinuteHR:
				has1m = true
			case resHour:
				has1h = true
			case resDay:
				has1d = true
			}
		}
		if !has1m || !has1h || !has1d {
			t.Fatalf("expected 1m/1h/1d sealed blocks, got 1m=%v 1h=%v 1d=%v", has1m, has1h, has1d)
		}

		// ResetAll, write newEpoch TS + KV, advance clock and Compact.
		seqBeforeReset := db.head.sealedWalSeq
		if err := s.ResetAll(); err != nil {
			t.Fatal(err)
		}
		tsNewHour := now.Add(-3 * time.Hour)
		tsNewMin := tsNewHour.Add(5 * time.Minute)
		appendValue(t, s, tsNewHour, 200)
		appendValue(t, s, tsNewMin, 300)
		reg, err := db.Region("meta")
		if err != nil {
			t.Fatal(err)
		}
		if err := reg.Put("state", []byte("active")); err != nil {
			t.Fatal(err)
		}
		targetSeq := db.appendSeq

		now = now.Add(10 * time.Minute)
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
		if db.head.sealedWalSeq <= seqBeforeReset || db.head.sealedWalSeq < targetSeq || db.head.sealedKVRev == 0 {
			t.Fatalf("WAL/KV stalled across ResetAll: before=%d sealed=%d target=%d kvRev=%d",
				seqBeforeReset, db.head.sealedWalSeq, targetSeq, db.head.sealedKVRev)
		}
		if len(s.rules) != 0 {
			t.Fatalf("expected ResetAll rule cleaned after seal, got %+v", s.rules)
		}
		var new1hMeta sealedMeta
		var cntNew1m, cntNew1h int
		for _, m := range db.sealed {
			if m.epoch == 0 {
				t.Fatalf("old epoch sealed block survived ResetAll: %+v", m)
			}
			switch m.res {
			case resMinuteHR:
				cntNew1m++
			case resHour:
				cntNew1h++
				new1hMeta = m
			}
		}
		if cntNew1m != 2 || cntNew1h != 1 {
			t.Fatalf("unexpected newEpoch blocks: 1m=%d 1h=%d", cntNew1m, cntNew1h)
		}

		checkState := func(curDB *DB, curS *Series) {
			t.Helper()
			res, err := curS.Aggregate(context.Background(), AggregateQuery{
				From: tsOldDay.Add(-time.Hour),
				To:   now.Add(time.Hour),
			})
			if err != nil {
				t.Fatal(err)
			}
			sum := res.Summary.Values[0]
			res.Release()
			if sum != 500 {
				t.Fatalf("unexpected Aggregate sum=%d, want 500", sum)
			}
			curReg, err := curDB.Region("meta")
			if err != nil {
				t.Fatal(err)
			}
			val, ok, err := curReg.Get("state")
			if err != nil || !ok || string(val) != "active" {
				t.Fatalf("KV mismatch: val=%q ok=%v err=%v", val, ok, err)
			}
		}
		checkState(db, s)

		// Reopen and verify cleaned rules, newEpoch TS, and KV persist.
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db = nil
		db, err = Open(path, opts)
		if err != nil {
			t.Fatal(err)
		}
		s, err = db.Series(7, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(s.rules) != 0 || s.resetEpoch != 1 {
			t.Fatalf("unexpected reopened reset state: epoch=%d rules=%+v", s.resetEpoch, s.rules)
		}
		checkState(db, s)

		// Verify an oldEpoch ResetAll rule does not dirty newEpoch 1h/1d blocks in computeRebuildMarks.
		rm := computeRebuildMarks(&compactPlan{
			oldSealed: []sealedMeta{new1hMeta},
			series: map[uint16]*seriesSnap{
				7: {id: 7, cfg: cfg, rules: []resetRule{{oldEpoch: 0, from: math.MinInt64, to: math.MaxInt64}}},
			},
			hourTarget:  hourTS(now.Unix()),
			dayTarget:   dayTS(hourTS(now.Unix())) - 86400,
			ret1mCutoff: now.Unix() - int64(opts.Retention1m/time.Second),
			ret1hCutoff: now.Unix() - int64(opts.Retention1h/time.Second),
			ret1dCutoff: now.Unix() - int64(opts.Retention1d/time.Second),
		})
		if len(rm.hour) != 0 || len(rm.day) != 0 || len(rm.dirtyHr) != 0 || len(rm.dirtyDay) != 0 {
			t.Fatalf("old ResetAll rule dirtied newEpoch blocks: %+v", rm)
		}

		// Advance past Retention1m: expired newEpoch 1m blocks expire while 1h passes through intact.
		now = now.Add(7 * time.Hour)
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
		var cnt1m, cnt1h int
		var after1hMeta sealedMeta
		for _, m := range db.sealed {
			switch m.res {
			case resMinuteHR:
				cnt1m++
			case resHour:
				cnt1h++
				after1hMeta = m
			}
		}
		if cnt1m != 0 || cnt1h != 1 {
			t.Fatalf("unexpected retention blocks after 1m expiry: 1m=%d 1h=%d", cnt1m, cnt1h)
		}
		if after1hMeta.crc != new1hMeta.crc || after1hMeta.order != new1hMeta.order || after1hMeta.values != new1hMeta.values {
			t.Fatalf("newEpoch 1h block was altered: before=%+v after=%+v", new1hMeta, after1hMeta)
		}
		res, err := s.Aggregate(context.Background(), AggregateQuery{
			From: tsNewHour.Truncate(time.Hour),
			To:   tsNewHour.Truncate(time.Hour).Add(time.Hour),
		})
		if err != nil {
			t.Fatal(err)
		}
		hourSum := res.Summary.Values[0]
		res.Release()
		if hourSum != 500 {
			t.Fatalf("unexpected 1h sum after 1m retention expiry: %d", hourSum)
		}
	})

	t.Run("UnsealedResetFrameRace", func(t *testing.T) {
		now := time.Date(2026, 6, 11, 10, 0, 15, 0, time.UTC)
		opts := Options{Now: func() time.Time { return now }}
		path := filepath.Join(t.TempDir(), "reset_all_race.db")
		db, err := Open(path, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if db != nil {
				_ = db.Close()
			}
		}()
		cfg := SeriesConfig{Arity: 2, SummaryOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast}, RowOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast}}
		s, err := db.Series(7, cfg)
		if err != nil {
			t.Fatal(err)
		}

		hotMin := now.Truncate(time.Minute)
		appendValue(t, s, now, 111)
		if err := s.ResetAll(); err != nil {
			t.Fatal(err)
		}
		appendValue(t, s, now.Add(10*time.Second), 222)

		// First Compact: hot epoch=0 delta blocks sealing, so frameReset remains in tail WAL.
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
		if len(s.rules) != 1 {
			t.Fatalf("expected unsealed ResetAll rule retained, got %+v", s.rules)
		}
		checkHotSum := func(curS *Series) {
			t.Helper()
			res, err := curS.Aggregate(context.Background(), AggregateQuery{From: hotMin, To: hotMin.Add(time.Minute)})
			if err != nil {
				t.Fatal(err)
			}
			sum := res.Summary.Values[0]
			res.Release()
			if sum != 222 {
				t.Fatalf("hot bucket leaked or lost data: sum=%d, want 222", sum)
			}
		}
		checkHotSum(s)

		// autoCompact must not spin while ResetAll is blocked by hot frame and no old sealed blocks exist.
		genBefore := db.head.gen
		db.autoCompact()
		if db.head.gen != genBefore {
			t.Fatalf("autoCompact spun unnecessarily while ResetAll blocked: gen %d -> %d", genBefore, db.head.gen)
		}

		// Reopen while frameReset is still unsealed: epoch=0 hot bucket must not resurrect.
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db = nil
		db, err = Open(path, opts)
		if err != nil {
			t.Fatal(err)
		}
		s, err = db.Series(7, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if s.resetEpoch != 1 || len(s.rules) != 1 {
			t.Fatalf("unexpected reopened state with unsealed frameReset: epoch=%d rules=%+v", s.resetEpoch, s.rules)
		}
		checkHotSum(s)

		// Advance clock 5 minutes and run autoCompact: frameReset seals and s.rules becomes empty.
		now = now.Add(5 * time.Minute)
		db.autoCompact()
		if db.head.sealedWalSeq != db.appendSeq {
			t.Fatalf("expected frameReset sealed after clock advance: sealed=%d append=%d", db.head.sealedWalSeq, db.appendSeq)
		}
		if len(s.rules) != 0 {
			t.Fatalf("expected s.rules empty after frameReset sealed, got %+v", s.rules)
		}
		if len(db.sealed) != 1 || db.sealed[0].epoch != 1 {
			t.Fatalf("unexpected sealed blocks after second compact: %+v", db.sealed)
		}
		checkHotSum(s)

		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db = nil
		db, err = Open(path, opts)
		if err != nil {
			t.Fatal(err)
		}
		s, err = db.Series(7, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(s.rules) != 0 || s.resetEpoch != 1 {
			t.Fatalf("unexpected reopened state after seal: epoch=%d rules=%+v", s.resetEpoch, s.rules)
		}
		checkHotSum(s)
	})

	t.Run("AutoCompactNoSpinOnWatermarks", func(t *testing.T) {
		now := time.Date(2026, 6, 11, 10, 15, 15, 0, time.UTC)
		db, s := testDB(t, &now, Options{})
		defer func() { _ = db.Close() }()

		// Case 1: Late out-of-order frame shrinks fineTarget behind cold delta + ResetAll.
		appendValue(t, s, now.Add(-5*time.Minute), 1)
		if err := s.ResetAll(); err != nil {
			t.Fatal(err)
		}
		appendValue(t, s, now, 2)
		appendValue(t, s, now.Add(-10*time.Minute), 3)
		genBefore := db.head.gen
		db.autoCompact()
		if db.head.gen != genBefore {
			t.Fatalf("autoCompact spun on late out-of-order watermark shrink: gen %d -> %d", genBefore, db.head.gen)
		}

		// Case 2: ResetCurrentDay blocked by preceding frame with ts == r.to when fineWatermark == r.to.
		now2 := time.Date(2026, 6, 11, 10, 0, 15, 0, time.UTC)
		db2, s2 := testDB(t, &now2, Options{})
		defer func() { _ = db2.Close() }()

		blockTS := now2.Truncate(time.Minute).Add(time.Minute) // == minuteTS(now2)+60 == r.to
		appendValue(t, s2, blockTS, 10)
		if err := s2.ResetCurrentDay(time.UTC); err != nil {
			t.Fatal(err)
		}
		now2 = now2.Add(3 * time.Minute) // fineWatermark == r.to, blocking frame still !(maxTS < fineTarget)
		genBefore2 := db2.head.gen
		db2.autoCompact()
		if db2.head.gen != genBefore2 {
			t.Fatalf("autoCompact spun when ResetCurrentDay blocked at r.to: gen %d -> %d", genBefore2, db2.head.gen)
		}
	})
}
