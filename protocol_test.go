package strata

import (
	"context"
	"encoding/binary"
	"fmt"
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
	payload := []byte("future-optional")
	patchSuperSlot(t, path, func(f *os.File, slot []byte) {
		info, _ := f.Stat()
		off := info.Size()
		if _, err := f.WriteAt(payload, off); err != nil {
			t.Fatal(err)
		}
		p := 200 + 7*32
		binary.LittleEndian.PutUint16(slot[p:p+2], 99)
		slot[p+3] = 1
		binary.LittleEndian.PutUint64(slot[p+8:p+16], uint64(off))
		binary.LittleEndian.PutUint64(slot[p+16:p+24], uint64(len(payload)))
		binary.LittleEndian.PutUint32(slot[p+24:p+28], uint32(len(payload)))
		binary.LittleEndian.PutUint32(slot[p+28:p+32], crc(payload))
	})

	db, err := Open(path, Options{})
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
		patchSuperSlot(t, path, func(_ *os.File, slot []byte) {
			binary.LittleEndian.PutUint32(slot[byteOff:byteOff+4], 1)
		})
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
		patchSuperblocks(t, path, func(f *osFile, head *superblock) {
			idxd := head.sections[secTSIndex]
			if idxd.count == 0 {
				t.Fatal("expected non-empty SecTSIndex")
			}
			raw, err := sectionRaw(f, idxd, nil)
			if err != nil {
				t.Fatal(err)
			}
			raw, err = zstdDecompress(raw)
			if err != nil {
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
				t.Fatal(err)
			}
			off := info.Size()
			if _, err := f.WriteAt(comp, off); err != nil {
				t.Fatal(err)
			}
			head.sections[secTSIndex].offset = uint64(off)
			head.sections[secTSIndex].length = uint64(len(comp))
			head.sections[secTSIndex].rawLen = uint32(len(legacy))
			head.sections[secTSIndex].crc = crc(comp)
			head.gen++
		})

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
		patchSuperblocks(t, path, func(_ *osFile, head *superblock) {
			if head.sections[secTSDict].length == 0 {
				t.Fatal("expected non-empty SecTSDict")
			}
			head.sections[secTSDict].codec = 1
			head.gen++
		})

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
			if sum := queryAggSum(t, curS, AggregateQuery{From: tsOldDay.Add(-time.Hour), To: now.Add(time.Hour)}); sum != 500 {
				t.Fatalf("unexpected Aggregate sum=%d, want 500", sum)
			}
			assertKV(t, mustRegion(t, curDB, "meta"), "state", []byte("active"), true)
		}
		checkState(db, s)

		// Reopen and verify cleaned rules, newEpoch TS, and KV persist.
		db = reopenTestDB(t, db, path, opts)
		s = mustSeries(t, db, 7, cfg)
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
		if hourSum := queryAggSum(t, s, AggregateQuery{From: tsNewHour.Truncate(time.Hour), To: tsNewHour.Truncate(time.Hour).Add(time.Hour)}); hourSum != 500 {
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
		s := mustSeries(t, db, 7, cfg)

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
			if sum := queryAggSum(t, curS, AggregateQuery{From: hotMin, To: hotMin.Add(time.Minute)}); sum != 222 {
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
		db = reopenTestDB(t, db, path, opts)
		s = mustSeries(t, db, 7, cfg)
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

		db = reopenTestDB(t, db, path, opts)
		s = mustSeries(t, db, 7, cfg)
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

func TestResetAllDictRotationAndStats(t *testing.T) {
	ctx := context.Background()
	ops := [4]MetricOp{OpSum, OpMin, OpMax, OpLast}
	cfg := SeriesConfig{Arity: 2, SummaryOps: ops, RowOps: ops}

	aggSum := func(t *testing.T, s *Series, f Filter) int64 {
		t.Helper()
		return queryAggSum(t, s, AggregateQuery{AllTime: true, Filters: []Filter{f}})
	}

	t.Run("ExternalReclaimIsolationAndRefresh", func(t *testing.T) {
		now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
		opts := Options{Now: func() time.Time { return now }}
		path := filepath.Join(t.TempDir(), "reset_dict_rotation.db")

		db, err := Open(path, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if db != nil {
				_ = db.Close()
			}
		}()

		s1 := mustSeries(t, db, 11, cfg)
		s2 := mustSeries(t, db, 22, cfg)

		const extCount = 17000
		seedTS := now.Add(-2 * time.Hour)
		seedHighCardMinute(t, s1, seedTS, extCount, func(i int) []string {
			return []string{fmt.Sprintf("s1-old-%05d", i), fmt.Sprintf("grp-%02d", i%20)}
		})
		seedHighCardMinute(t, s2, seedTS, extCount, func(i int) []string {
			return []string{fmt.Sprintf("s2-keep-%05d", i), fmt.Sprintf("cat-%02d", i%20)}
		})
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
		if db.head.sections[secTSDict].codec != 2 {
			t.Fatalf("expected dict codec=2, got %d", db.head.sections[secTSDict].codec)
		}

		s1Before, err := s1.DictStats()
		if err != nil {
			t.Fatal(err)
		}
		if len(s1Before) != 2 || s1Before[0].Dim != 0 || s1Before[0].MaxID != extCount || s1Before[0].LiveCount != extCount ||
			s1Before[0].DeadCount != 0 || s1Before[0].ExternalBytes == 0 || s1Before[0].ForwardPages == 0 || s1Before[0].ReverseBlocks == 0 {
			t.Fatalf("unexpected s1 pre-reset DictStats: %+v", s1Before)
		}
		s2Before, err := s2.DictStats()
		if err != nil {
			t.Fatal(err)
		}
		if len(s2Before) != 2 || s2Before[0].MaxID != extCount || s2Before[0].ExternalBytes == 0 {
			t.Fatalf("unexpected s2 pre-reset DictStats: %+v", s2Before)
		}
		fiBefore, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		dictSecBefore := db.head.sections[secTSDict].length

		roDB, err := Open(path, Options{ReadOnly: true, Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		defer roDB.Close()
		roS1, err := roDB.Series(11, cfg)
		if err != nil {
			t.Fatal(err)
		}
		roS2, err := roDB.Series(22, cfg)
		if err != nil {
			t.Fatal(err)
		}

		if err := s1.ResetAll(); err != nil {
			t.Fatal(err)
		}
		s1Empty, err := s1.DictStats()
		if err != nil {
			t.Fatal(err)
		}
		for d, st := range s1Empty {
			if st != (DictDimStats{Dim: d}) {
				t.Fatalf("expected zeroed DictStats immediately after ResetAll, dim=%d got=%+v", d, st)
			}
		}

		newRows := []Row{
			{Tags: []string{"s1-new-a", "new-grp"}, Values: [4]int64{10, 10, 10, 10}, Mask: 0x0f},
			{Tags: []string{"s1-new-b", "new-grp"}, Values: [4]int64{10, 10, 10, 10}, Mask: 0x0f},
			{Tags: []string{"s1-new-c", "new-grp"}, Values: [4]int64{10, 10, 10, 10}, Mask: 0x0f},
		}
		if err := s1.AppendSync(now.Add(-5*time.Minute), Summary{Values: [4]int64{30, 10, 10, 10}, Mask: 0x0f}, newRows...); err != nil {
			t.Fatal(err)
		}
		s1PostWrite, err := s1.DictStats()
		if err != nil {
			t.Fatal(err)
		}
		if s1PostWrite[0].MaxID != 3 || s1PostWrite[0].LiveCount != 3 || s1PostWrite[0].DeadCount != 0 ||
			s1PostWrite[0].InlineBytes == 0 || s1PostWrite[0].ExternalBytes != 0 || s1PostWrite[0].ForwardPages != 0 ||
			s1PostWrite[1].MaxID != 1 || s1PostWrite[1].LiveCount != 1 {
			t.Fatalf("unexpected s1 DictStats after post-reset writes: %+v", s1PostWrite)
		}

		// Incremental read-only Refresh across ResetAll + post-reset WAL delta frames.
		if err := roDB.Refresh(); err != nil {
			t.Fatal(err)
		}
		roS1Stats, err := roS1.DictStats()
		if err != nil {
			t.Fatal(err)
		}
		if roS1Stats[0].MaxID != 3 || roS1Stats[0].LiveCount != 3 || roS1Stats[0].ExternalBytes != 0 {
			t.Fatalf("unexpected roS1 DictStats after incremental Refresh: %+v", roS1Stats)
		}
		if got := aggSum(t, roS1, Filter{Dim: 0, Op: TagEq, Values: []string{"s1-new-b"}}); got != 10 {
			t.Fatalf("roS1 post-reset TagEq got=%d want=10", got)
		}
		if got := aggSum(t, roS1, Filter{Dim: 0, Op: TagEq, Values: []string{"s1-old-00001"}}); got != 0 {
			t.Fatalf("roS1 pre-reset tag leaked after Refresh: got=%d want=0", got)
		}
		if got := aggSum(t, roS2, Filter{Dim: 0, Op: TagEq, Values: []string{"s2-keep-00042"}}); got != 1 {
			t.Fatalf("roS2 damaged after s1 ResetAll Refresh: got=%d want=1", got)
		}

		// Advance clock and Compact to physically reclaim s1's old external dictionary and blocks.
		now = now.Add(10 * time.Minute)
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}

		s1After, err := s1.DictStats()
		if err != nil {
			t.Fatal(err)
		}
		if s1After[0].MaxID != 3 || s1After[0].LiveCount != 3 || s1After[0].ExternalBytes != 0 ||
			s1After[0].ForwardPages != 0 || s1After[0].ReverseBlocks != 0 || s1After[1].MaxID != 1 {
			t.Fatalf("unexpected s1 DictStats after Compact: %+v", s1After)
		}
		s2After, err := s2.DictStats()
		if err != nil {
			t.Fatal(err)
		}
		if s2After[0] != s2Before[0] || s2After[1] != s2Before[1] {
			t.Fatalf("s2 DictStats changed after s1 ResetAll+Compact: before=%+v after=%+v", s2Before, s2After)
		}
		fiAfter, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		dictSecAfter := db.head.sections[secTSDict].length
		if fiAfter.Size() >= fiBefore.Size() || dictSecAfter >= dictSecBefore {
			t.Fatalf("expected physical reclamation: file %d -> %d, dictSec %d -> %d",
				fiBefore.Size(), fiAfter.Size(), dictSecBefore, dictSecAfter)
		}

		// Read-only Refresh across generation swap.
		if err := roDB.Refresh(); err != nil {
			t.Fatal(err)
		}
		if got := aggSum(t, roS1, Filter{Dim: 0, Op: TagPrefix, Values: []string{"s1-new-"}}); got != 30 {
			t.Fatalf("roS1 post-compact TagPrefix got=%d want=30", got)
		}
		if got := aggSum(t, roS2, Filter{Dim: 0, Op: TagEq, Values: []string{"s2-keep-16999"}}); got != 1 {
			t.Fatalf("roS2 post-compact TagEq got=%d want=1", got)
		}
		_ = roDB.Close()

		// Reopen and verify full persistence, isolation, Client.DictStats, and closed-DB error.
		db = reopenTestDB(t, db, path, opts)
		s1 = mustSeries(t, db, 11, cfg)
		s2 = mustSeries(t, db, 22, cfg)
		s1Reopen, err := s1.DictStats()
		if err != nil {
			t.Fatal(err)
		}
		s2Reopen, err := s2.DictStats()
		if err != nil {
			t.Fatal(err)
		}
		if s1Reopen[0] != s1After[0] || s1Reopen[1] != s1After[1] || s2Reopen[0] != s2Before[0] || s2Reopen[1] != s2Before[1] {
			t.Fatalf("reopened DictStats mismatch: s1=%+v s2=%+v", s1Reopen, s2Reopen)
		}
		if got := aggSum(t, s1, Filter{Dim: 0, Op: TagPrefix, Values: []string{"s1-new-"}}); got != 30 {
			t.Fatalf("reopened s1 sum got=%d want=30", got)
		}
		if got := aggSum(t, s1, Filter{Dim: 0, Op: TagEq, Values: []string{"s1-old-00001"}}); got != 0 {
			t.Fatalf("reopened s1 old tag resurrected: got=%d want=0", got)
		}
		if got := aggSum(t, s2, Filter{Dim: 0, Op: TagPrefix, Values: []string{"s2-keep-0010"}}); got != 10 {
			t.Fatalf("reopened s2 sum got=%d want=10", got)
		}

		client := NewClient(db, ClientOptions{})
		cStats, err := client.DictStats(11)
		if err != nil || len(cStats) != 2 || cStats[0] != s1Reopen[0] || cStats[1] != s1Reopen[1] {
			t.Fatalf("Client.DictStats mismatch: err=%v got=%+v want=%+v", err, cStats, s1Reopen)
		}
		_ = client.Close()

		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db = nil
		if _, err := s1.DictStats(); err == nil {
			t.Fatal("expected error from Series.DictStats on closed DB")
		}
	})

	t.Run("UnsealedHotFrameBlocksResetAllCompact", func(t *testing.T) {
		now := time.Date(2026, 7, 2, 10, 0, 15, 0, time.UTC)
		opts := Options{Now: func() time.Time { return now }}
		path := filepath.Join(t.TempDir(), "reset_unsealed_dict.db")

		db, err := Open(path, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if db != nil {
				_ = db.Close()
			}
		}()
		s, err := db.Series(33, cfg)
		if err != nil {
			t.Fatal(err)
		}

		hotOld := make([]Row, 50)
		for i := range hotOld {
			hotOld[i] = Row{Tags: []string{fmt.Sprintf("hot-old-%03d", i), "g-old"}, Values: [4]int64{5, 5, 5, 5}, Mask: 0x0f}
		}
		if err := s.AppendSync(now, Summary{Values: [4]int64{250, 5, 5, 5}, Mask: 0x0f}, hotOld...); err != nil {
			t.Fatal(err)
		}
		if err := s.ResetAll(); err != nil {
			t.Fatal(err)
		}
		hotNew := []Row{
			{Tags: []string{"hot-new-0", "g-new"}, Values: [4]int64{100, 100, 100, 100}, Mask: 0x0f},
			{Tags: []string{"hot-new-1", "g-new"}, Values: [4]int64{100, 100, 100, 100}, Mask: 0x0f},
		}
		if err := s.AppendSync(now.Add(5*time.Second), Summary{Values: [4]int64{200, 100, 100, 100}, Mask: 0x0f}, hotNew...); err != nil {
			t.Fatal(err)
		}

		// Immediate Compact while epoch=0 hot delta blocks frameReset sealing in tail WAL.
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
		verifyClean := func(label string, curS *Series) {
			t.Helper()
			st, err := curS.DictStats()
			if err != nil {
				t.Fatalf("%s DictStats: %v", label, err)
			}
			if len(st) != 2 || st[0].MaxID != 2 || st[0].LiveCount != 2 || st[0].DeadCount != 0 || st[1].MaxID != 1 || st[1].LiveCount != 1 {
				t.Fatalf("%s epoch=0 dict leaked into s: %+v", label, st)
			}
			if got := aggSum(t, curS, Filter{Dim: 0, Op: TagPrefix, Values: []string{"hot-old-"}}); got != 0 {
				t.Fatalf("%s hot-old leaked: got=%d want=0", label, got)
			}
			if got := aggSum(t, curS, Filter{Dim: 0, Op: TagPrefix, Values: []string{"hot-new-"}}); got != 200 {
				t.Fatalf("%s hot-new mismatch: got=%d want=200", label, got)
			}
		}
		verifyClean("post-immediate-compact", s)

		// Reopen while frameReset is still unsealed in tail WAL.
		db = reopenTestDB(t, db, path, opts)
		s = mustSeries(t, db, 33, cfg)
		verifyClean("reopened-unsealed", s)

		// Advance clock, seal frameReset, and reopen.
		now = now.Add(5 * time.Minute)
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
		if len(s.rules) != 0 {
			t.Fatalf("expected empty rules after sealing frameReset, got %+v", s.rules)
		}
		verifyClean("post-seal-compact", s)

		db = reopenTestDB(t, db, path, opts)
		s = mustSeries(t, db, 33, cfg)
		verifyClean("reopened-sealed", s)
	})

	t.Run("PreCompactCoarseBlocksAndArityIsolation", func(t *testing.T) {
		now := time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC)
		db, err := Open(filepath.Join(t.TempDir(), "reset_precompact_coarse.db"), Options{
			Now:         func() time.Time { return now },
			Retention1m: 14 * 24 * time.Hour,
			Retention1h: 90 * 24 * time.Hour,
			Retention1d: 365 * 24 * time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		s, err := db.Series(44, cfg)
		if err != nil {
			t.Fatal(err)
		}
		oldDay := now.Add(-72 * time.Hour)
		oldHour := now.Add(-2 * time.Hour)
		if err := s.Append(oldDay, Summary{Values: [4]int64{11, 11, 11, 11}, Mask: 0x0f},
			Row{Tags: []string{"old-day", "g"}, Values: [4]int64{11, 11, 11, 11}, Mask: 0x0f},
		); err != nil {
			t.Fatal(err)
		}
		if err := s.Append(oldHour, Summary{Values: [4]int64{22, 22, 22, 22}, Mask: 0x0f},
			Row{Tags: []string{"old-hour", "g"}, Values: [4]int64{22, 22, 22, 22}, Mask: 0x0f},
		); err != nil {
			t.Fatal(err)
		}
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
		var has1m, has1h, has1d bool
		for _, m := range db.sealed {
			if m.series != 44 {
				continue
			}
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
			t.Fatalf("expected 1m/1h/1d sealed blocks before ResetAll, got 1m=%v 1h=%v 1d=%v", has1m, has1h, has1d)
		}

		// ResetAll without Compact: old 1h/1d blocks stay on disk until next Compact.
		if err := s.ResetAll(); err != nil {
			t.Fatal(err)
		}

		assertEmptyWindow := func(label string, aq AggregateQuery, rq ReadRequest, sq ScanQuery, checkExact bool) {
			t.Helper()
			if agg, err := s.Aggregate(ctx, aq); err != nil {
				t.Fatalf("%s Aggregate failed: %v", label, err)
			} else {
				if (checkExact && !agg.Exact) || agg.Summary.Values[0] != 0 || len(agg.Segments) != 0 {
					agg.Release()
					t.Fatalf("%s unexpected Aggregate: %+v", label, agg)
				}
				agg.Release()
			}
			if page, err := s.Read(ctx, rq); err != nil {
				t.Fatalf("%s Read failed: %v", label, err)
			} else {
				if (checkExact && !page.Exact) || len(page.Rows) != 0 {
					page.Release()
					t.Fatalf("%s unexpected Read: %+v", label, page)
				}
				page.Release()
			}
			if cnt, _, err := scanCountSumErr(ctx, s, sq); err != nil || cnt != 0 {
				t.Fatalf("%s Scan: err=%v rows=%d", label, err, cnt)
			}
		}

		// 1a. Empty post-ResetAll pre-Compact: AllTime + RequireExact must succeed empty with Exact=true.
		assertEmptyWindow("alltime-summary", AggregateQuery{AllTime: true, RequireExact: true},
			ReadRequest{AllTime: true, RequireExact: true, Limit: 10}, ScanQuery{AllTime: true, RequireExact: true}, true)
		if agg, err := s.Aggregate(ctx, AggregateQuery{AllTime: true, RequireExact: true, GroupDims: []int{0}}); err != nil {
			t.Fatalf("empty grouped Aggregate AllTime+RequireExact failed: %v", err)
		} else {
			if !agg.Exact || len(agg.Groups) != 1 || len(agg.Groups[0]) != 0 {
				agg.Release()
				t.Fatalf("unexpected empty grouped result: %+v", agg)
			}
			agg.Release()
		}
		if sum := queryAggSum(t, s, AggregateQuery{
			AllTime: true, RequireExact: true,
			Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"old-day"}}},
		}); sum != 0 {
			t.Fatalf("unexpected empty filtered sum=%d", sum)
		}

		// Explicit range covering old 1d/1h coarse blocks must not raise ErrResetRequiresFineData.
		spanFrom, spanTo := oldDay.Add(-time.Hour), now.Add(time.Minute)
		assertEmptyWindow("explicit-empty", AggregateQuery{From: spanFrom, To: spanTo},
			ReadRequest{From: spanFrom, To: spanTo, Limit: 10}, ScanQuery{From: spanFrom, To: spanTo}, false)

		// 1b. Append new post-ResetAll data (still pre-Compact): AllTime+RequireExact sees only new data and new RequestedFrom.
		newTS := now.Truncate(time.Minute)
		if err := s.Append(newTS, Summary{Values: [4]int64{99, 99, 99, 99}, Mask: 0x0f},
			Row{Tags: []string{"fresh-tag", "g-fresh"}, Values: [4]int64{99, 99, 99, 99}, Mask: 0x0f},
		); err != nil {
			t.Fatal(err)
		}
		for _, aq := range []AggregateQuery{
			{AllTime: true, RequireExact: true},
			{AllTime: true, RequireExact: true, Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"fresh-tag"}}}},
		} {
			agg, err := s.Aggregate(ctx, aq)
			if err != nil {
				t.Fatalf("post-write Aggregate(%+v) failed: %v", aq, err)
			}
			if !agg.Exact || agg.Summary.Values[0] != 99 || !agg.RequestedFrom.Equal(newTS) {
				agg.Release()
				t.Fatalf("unexpected post-write Aggregate: exact=%v sum=%d reqFrom=%v wantFrom=%v",
					agg.Exact, agg.Summary.Values[0], agg.RequestedFrom, newTS)
			}
			agg.Release()
		}
		if page, err := s.Read(ctx, ReadRequest{AllTime: true, RequireExact: true, Limit: 10}); err != nil {
			t.Fatalf("post-write Read AllTime+RequireExact failed: %v", err)
		} else {
			if !page.Exact || len(page.Rows) != 1 || page.Rows[0].Tags[0] != "fresh-tag" || page.Rows[0].Values[0] != 99 || !page.RequestedFrom.Equal(newTS) {
				page.Release()
				t.Fatalf("unexpected post-write Read: exact=%v reqFrom=%v rows=%+v", page.Exact, page.RequestedFrom, page.Rows)
			}
			page.Release()
		}
		if scanCnt, scanSum, err := scanCountSumErr(ctx, s, ScanQuery{AllTime: true, RequireExact: true}); err != nil || scanCnt != 1 || scanSum != 99 {
			t.Fatalf("post-write Scan AllTime+RequireExact: err=%v cnt=%d sum=%d", err, scanCnt, scanSum)
		}

		// Explicit window spanning old 1d/1h sealed blocks + new live bucket must return only new data.
		if sum := queryAggSum(t, s, AggregateQuery{From: spanFrom, To: newTS.Add(time.Minute)}); sum != 99 {
			t.Fatalf("explicit-window post-write SummaryOnly sum=%d want=99", sum)
		}
		if page, err := s.Read(ctx, ReadRequest{From: spanFrom, To: newTS.Add(time.Minute), Limit: 10}); err != nil {
			t.Fatalf("explicit-window post-write Read failed: %v", err)
		} else {
			if len(page.Rows) != 1 || page.Rows[0].Tags[0] != "fresh-tag" {
				page.Release()
				t.Fatalf("explicit-window post-write Read rows=%+v", page.Rows)
			}
			page.Release()
		}
		if scanCnt, scanSum, err := scanCountSumErr(ctx, s, ScanQuery{From: spanFrom, To: newTS.Add(time.Minute)}); err != nil || scanCnt != 1 || scanSum != 99 {
			t.Fatalf("explicit-window post-write Scan: err=%v cnt=%d sum=%d", err, scanCnt, scanSum)
		}

		// 2. Append row arity mismatch must not leak prior rows' tags into db.dict or db.pendingDict.
		statsBefore, err := s.DictStats()
		if err != nil {
			t.Fatal(err)
		}
		pendingDictBefore := len(db.pendingDict)
		if err := s.Append(newTS, Summary{Values: [4]int64{3}, Mask: 0x0f},
			Row{Tags: []string{"must-not-leak-0", "must-not-leak-1"}, Values: [4]int64{1}, Mask: 0x0f},
			Row{Tags: []string{"wrong-arity"}, Values: [4]int64{2}, Mask: 0x0f},
		); err == nil {
			t.Fatal("expected error on row arity mismatch")
		}
		statsAfter, err := s.DictStats()
		if err != nil {
			t.Fatal(err)
		}
		if statsAfter[0] != statsBefore[0] || statsAfter[1] != statsBefore[1] || len(db.pendingDict) != pendingDictBefore {
			t.Fatalf("arity-mismatch Append leaked dict state: before=%+v after=%+v pendingDict=%d->%d",
				statsBefore, statsAfter, pendingDictBefore, len(db.pendingDict))
		}

		// 3. ResetCurrentDay with expired 1m fine data and surviving 1h coarse block must still return ErrResetRequiresFineData.
		nowDay := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
		dbDay, err := Open(filepath.Join(t.TempDir(), "reset_day_coarse.db"), Options{
			Now:         func() time.Time { return nowDay },
			Retention1m: time.Hour,
			Retention1h: 24 * time.Hour,
			Retention1d: 7 * 24 * time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer dbDay.Close()
		sDay, err := dbDay.Series(55, cfg)
		if err != nil {
			t.Fatal(err)
		}
		todayEarly := time.Date(2026, 7, 6, 2, 0, 0, 0, time.UTC)
		if err := sDay.Append(todayEarly, Summary{Values: [4]int64{7, 7, 7, 7}, Mask: 0x0f},
			Row{Tags: []string{"early", "g"}, Values: [4]int64{7, 7, 7, 7}, Mask: 0x0f},
		); err != nil {
			t.Fatal(err)
		}
		if err := dbDay.Compact(); err != nil {
			t.Fatal(err)
		}
		var day1m, day1h int
		for _, m := range dbDay.sealed {
			switch m.res {
			case resMinuteHR:
				day1m++
			case resHour:
				day1h++
			}
		}
		if day1m != 0 || day1h != 1 {
			t.Fatalf("expected expired 1m and surviving 1h block, got 1m=%d 1h=%d", day1m, day1h)
		}
		if err := sDay.ResetCurrentDay(time.UTC); err != nil {
			t.Fatal(err)
		}
		if _, err := sDay.Aggregate(ctx, AggregateQuery{From: todayEarly, To: nowDay}); err != ErrResetRequiresFineData {
			t.Fatalf("ResetCurrentDay SummaryOnly window: got %v want ErrResetRequiresFineData", err)
		}
		if _, err := sDay.Aggregate(ctx, AggregateQuery{AllTime: true}); err != ErrResetRequiresFineData {
			t.Fatalf("ResetCurrentDay SummaryOnly AllTime: got %v want ErrResetRequiresFineData", err)
		}
		if _, err := sDay.Read(ctx, ReadRequest{From: todayEarly, To: nowDay, Limit: 10}); err != ErrResetRequiresFineData {
			t.Fatalf("ResetCurrentDay Read: got %v want ErrResetRequiresFineData", err)
		}
		if err := sDay.Scan(ctx, ScanQuery{From: todayEarly, To: nowDay}, func(ScanBatch) error { return nil }); err != ErrResetRequiresFineData {
			t.Fatalf("ResetCurrentDay Scan: got %v want ErrResetRequiresFineData", err)
		}
	})
}

func TestBatch1AtomicityAndStartupCleanup(t *testing.T) {
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	ops := [4]MetricOp{OpSum, OpMin, OpMax, OpLast}
	cfg := SeriesConfig{Arity: 1, SummaryOps: ops, RowOps: ops}

	t.Run("RefreshWriterNoopAndReadOnlyCorruptPreservesState", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "refresh_atomic.db")
		db, err := Open(path, Options{Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if db != nil {
				_ = db.Close()
			}
		}()
		s, err := db.Series(1, cfg)
		if err != nil {
			t.Fatal(err)
		}
		ts := now.Add(-2 * time.Hour)
		if err := s.Append(ts, Summary{Values: [4]int64{10, 10, 10, 10}, Mask: 0x0f},
			Row{Tags: []string{"k1"}, Values: [4]int64{10, 10, 10, 10}, Mask: 0x0f},
		); err != nil {
			t.Fatal(err)
		}
		if err := db.Refresh(); err != nil {
			t.Fatalf("writer Refresh failed: %v", err)
		}
		if len(db.pending) != 1 {
			t.Fatalf("writer Refresh cleared pending: len=%d", len(db.pending))
		}
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}

		roDB, err := Open(path, Options{ReadOnly: true, Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		defer roDB.Close()
		roS, err := roDB.Series(1, cfg)
		if err != nil {
			t.Fatal(err)
		}
		oldGen := roDB.head.gen
		oldHead := roDB.head

		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db = nil

		// Replace file with a next-gen superblock whose TS index CRC is corrupted so next.loadLocked fails.
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		badHead := oldHead
		badHead.gen++
		badHead.sections[secTSIndex].crc ^= 0xffffffff
		slot := encodeSlot(badHead)
		copy(raw[:slotSize], slot)
		copy(raw[slotSize:headerSize], slot)
		badPath := path + ".corrupt"
		if err := os.WriteFile(badPath, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := renameAtomic(badPath, path); err != nil {
			t.Fatal(err)
		}

		if err := roDB.Refresh(); err == nil {
			t.Fatal("expected read-only Refresh error on corrupted next generation")
		}
		if roDB.head.gen != oldGen {
			t.Fatalf("failed Refresh mutated generation: got=%d want=%d", roDB.head.gen, oldGen)
		}
		if got := queryAggSum(t, roS, AggregateQuery{
			AllTime: true,
			Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"k1"}}},
		}); got != 10 {
			t.Fatalf("query after failed Refresh lost data: got=%d want=10", got)
		}
	})

	t.Run("OpenCleansCrashTempFiles", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "cleanup.db")
		db, err := Open(path, Options{Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		stale := []string{path + ".tmp", path + ".invtmp", path + ".invchk"}
		for _, p := range stale {
			if err := os.WriteFile(p, []byte("stale"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		db, err = Open(path, Options{Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		for _, p := range stale {
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Fatalf("expected stale temp file %s removed on Open, stat err=%v", p, err)
			}
		}
	})

	t.Run("FlushLockedErrorRollbackNoDoubleCount", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "flush_rollback.db")
		db, err := Open(path, Options{Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if db != nil {
				_ = db.Close()
			}
		}()
		s, err := db.Series(1, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Append(now.Add(-2*time.Minute), Summary{Values: [4]int64{10, 10, 10, 10}, Mask: 0x0f},
			Row{Tags: []string{"m1"}, Values: [4]int64{10, 10, 10, 10}, Mask: 0x0f},
		); err != nil {
			t.Fatal(err)
		}
		if err := s.Append(now.Add(-time.Minute), Summary{Values: [4]int64{20, 20, 20, 20}, Mask: 0x0f},
			Row{Tags: []string{"m2"}, Values: [4]int64{20, 20, 20, 20}, Mask: 0x0f},
		); err != nil {
			t.Fatal(err)
		}
		origPos, origAppend, origAlloc := db.walPos, db.appendSeq, db.allocatedSeq

		// Swap in a read-only fd to force appendFrame failure during Flush and verify rollback.
		roFD, err := openData(path, true)
		if err != nil {
			t.Fatal(err)
		}
		origData := db.data
		db.data = roFD
		flushErr := db.Flush()
		db.data = origData
		_ = roFD.Close()
		if flushErr == nil {
			t.Fatal("expected Flush failure on read-only file descriptor")
		}
		if db.walPos != origPos || db.appendSeq != origAppend || db.allocatedSeq != origAlloc || len(db.pending) != 2 {
			t.Fatalf("Flush error did not roll back WAL state: pos=%d/%d append=%d/%d alloc=%d/%d pending=%d",
				db.walPos, origPos, db.appendSeq, origAppend, db.allocatedSeq, origAlloc, len(db.pending))
		}

		// Retry Sync on restored writable fd and verify reopened sum has no double count.
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
		if db.appendSeq != origAppend+2 || db.allocatedSeq != origAlloc+2 {
			t.Fatalf("unexpected seqs after retry: append=%d alloc=%d want=%d", db.appendSeq, db.allocatedSeq, origAppend+2)
		}
		reopened := reopenTestDB(t, db, path, Options{Now: func() time.Time { return now }})
		db = nil
		defer reopened.Close()
		rs := mustSeries(t, reopened, 1, cfg)
		if got := queryAggSum(t, rs, AggregateQuery{AllTime: true}); got != 30 {
			t.Fatalf("reopened sum after Flush rollback+retry got=%d want=30", got)
		}
	})
}
