package strata

import (
	"encoding/binary"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"
)

func requireOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func testValues(n int64) [4]int64 { return [4]int64{n, n, n, n} }

func testRow(n int64, tags ...string) Row { return Row{Tags: tags, Values: testValues(n)} }

func tagFilter(dim int, op TagOp, value string) []Filter {
	return []Filter{{Dim: dim, Op: op, Values: []string{value}}}
}

func assertQuerySum(t *testing.T, s *Series, q AggregateQuery, want int64) {
	t.Helper()
	if got := queryAggSum(t, s, q); got != want {
		t.Fatalf("Aggregate(%+v) sum=%d want=%d", q, got, want)
	}
}

func dictStats(t *testing.T, s *Series) []DictDimStats {
	t.Helper()
	st, err := s.DictStats()
	requireOK(t, err)
	return st
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	requireOK(t, err)
	return st.Size()
}

func TestPackageBoundaries(t *testing.T) {
	files, err := filepath.Glob("*.go")
	requireOK(t, err)
	fset := token.NewFileSet()
	for _, name := range files {
		f, err := parser.ParseFile(fset, name, nil, 0)
		requireOK(t, err)
		for _, imp := range f.Imports {
			if strings.Contains(imp.Path.Value, "/internal/") {
				t.Errorf("%s imports %s", name, imp.Path.Value)
			}
		}
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !fn.Name.IsExported() {
				continue
			}
			ast.Inspect(fn.Type, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.Ident:
					if n.Name == "any" {
						t.Errorf("%s.%s uses any", name, fn.Name.Name)
					}
				case *ast.InterfaceType:
					t.Errorf("%s.%s uses an anonymous interface", name, fn.Name.Name)
				}
				return true
			})
		}
	}
}

func TestSuperblockLayoutAndFeatures(t *testing.T) {
	for _, tc := range []struct {
		name      string
		got, want uintptr
	}{
		{"idRow", unsafe.Sizeof(idRow{}), 72}, {"sealedMeta", unsafe.Sizeof(sealedMeta{}), 80},
		{"openAddrSlot", unsafe.Sizeof(openAddrSlot{}), 40}, {"Cursor", unsafe.Sizeof(Cursor{}), 40},
	} {
		if tc.got != tc.want {
			t.Errorf("%s is %dB, want %dB", tc.name, tc.got, tc.want)
		}
	}

	f, err := openData(filepath.Join(t.TempDir(), "slots.db"), false)
	requireOK(t, err)
	defer f.Close()
	head := superblock{version: 1, gen: 3, walOffset: dataStart}
	requireOK(t, writeSupers(f, head))
	head.gen++
	head.sections[0].kind = 99
	raw := encodeSlot(head)
	if len(raw) != slotSize || string(raw[:8]) != magic || binary.LittleEndian.Uint16(raw[200:202]) != 99 {
		t.Fatal("superblock layout changed")
	}
	for _, gen := range []uint64{4, 3} {
		_, err := f.WriteAt(raw, slotSize)
		requireOK(t, err)
		got, err := readSuper(f)
		if err != nil || got.gen != gen {
			t.Fatalf("readSuper generation=%d want=%d err=%v", got.gen, gen, err)
		}
		raw[slotSize-1] ^= 1 // Fall back when the newest slot is torn.
	}
}

func TestOptionalSectionsAndFeatureErrors(t *testing.T) {
	now := time.Now()
	db, _ := testDB(t, &now, Options{})
	path := db.path
	requireOK(t, db.Close())
	payload := []byte("future-optional")
	patchSuperblocks(t, path, func(f *osFile, head *superblock) {
		off := fileSize(t, path)
		_, err := f.WriteAt(payload, off)
		requireOK(t, err)
		head.compat = 1
		head.sections[7] = sectionDesc{
			kind: 99, flags: secFlagOptional, offset: uint64(off), length: uint64(len(payload)),
			rawLen: uint32(len(payload)), crc: crc(payload),
		}
	})
	db, err := Open(path, Options{})
	requireOK(t, err)
	requireOK(t, db.Compact())
	requireOK(t, db.Close())
	reader, err := Open(path, Options{ReadOnly: true})
	requireOK(t, err)
	defer reader.Close()
	if len(reader.optional) != 1 || string(reader.optional[0].data) != string(payload) {
		t.Fatalf("optional section did not survive compaction: %+v", reader.optional)
	}
	requireOK(t, reader.Close())

	for _, readOnly := range []bool{false, true} {
		patchSuperSlot(t, path, func(_ *os.File, slot []byte) {
			binary.LittleEndian.PutUint32(slot[16:20], 1)
			if readOnly {
				binary.LittleEndian.PutUint32(slot[20:24], 1)
			}
		})
		want := ErrReadOnlyCompat
		if readOnly {
			want = ErrIncompatFormat
		}
		unexpected, err := Open(path, Options{ReadOnly: readOnly})
		if err == nil {
			_ = unexpected.Close()
		}
		if err != want {
			t.Fatalf("Open(readOnly=%t) error=%v want=%v", readOnly, err, want)
		}
		if !readOnly {
			compatible, err := Open(path, Options{ReadOnly: true})
			requireOK(t, err)
			requireOK(t, compatible.Close())
		}
	}
}

func TestWALDurabilityAndTornTail(t *testing.T) {
	now := time.Now().Truncate(time.Minute)
	db, s := testDB(t, &now, Options{})
	defer func() { _ = db.Close() }()
	ts := now.Add(-time.Minute)
	appendValue(t, s, ts, 0)
	appendValue(t, s, ts, 5)
	path, end := db.path, db.walPos
	if db.appendSeq == 0 || db.durableSeq != db.appendSeq {
		t.Fatal("sync sequence did not advance")
	}
	requireOK(t, db.Close())
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	requireOK(t, err)
	_, err = f.Write([]byte("UDW1\x80"))
	requireOK(t, err)
	requireOK(t, f.Close())
	db = reopenTestDB(t, db, path, db.opts)
	s = mustSeries(t, db, s.id, s.cfg)
	agg, err := s.Aggregate(t.Context(), AggregateQuery{From: ts, To: now})
	requireOK(t, err)
	values := agg.Summary.Values
	agg.Release()
	if values[0] != 5 || values[1] != 0 || values[3] != 5 || fileSize(t, path) != end {
		t.Fatalf("torn WAL recovery: values=%v expected length=%d", values, end)
	}
	requireOK(t, db.Flush())
	if fileSize(t, path) != end {
		t.Fatal("idle Flush changed the WAL")
	}
}

func TestResetAtomicityAndRollback(t *testing.T) {
	now := time.Date(2026, 1, 2, 10, 0, 5, 0, time.UTC)
	db, s := testDB(t, &now, Options{})
	defer db.Close()
	appendValue(t, s, now, 100)
	requireOK(t, s.ResetCurrentDay(time.UTC))
	appendValue(t, s, now.Add(15*time.Second), 200)
	assertTSSum(t, s, now.Truncate(time.Minute), now.Truncate(time.Minute).Add(time.Minute), 200)

	epoch, pos, seq := s.resetEpoch, db.walPos, db.appendSeq
	db.syncHook = func() error { return os.ErrInvalid }
	err := s.ResetCurrentDay(time.UTC)
	db.syncHook = nil
	if err != os.ErrInvalid || epoch != 1 || s.resetEpoch != epoch || db.walPos != pos || db.appendSeq != seq || db.allocatedSeq != seq {
		t.Fatalf("reset rollback: err=%v epoch=%d pos=%d seq=%d alloc=%d", err, s.resetEpoch, db.walPos, db.appendSeq, db.allocatedSeq)
	}
}

func TestRejectLegacyFormats(t *testing.T) {
	check := func(err error, message string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), message) {
			t.Fatalf("error=%v want %q", err, message)
		}
	}
	clock := time.Date(2026, 11, 24, 4, 0, 0, 0, time.UTC)
	db, series := testDB(t, &clock, Options{})
	appendValue(t, series, clock.Add(-4*time.Hour), 10)
	requireOK(t, db.Compact())
	requireOK(t, db.Close())
	base, err := os.ReadFile(db.path)
	requireOK(t, err)
	enc := newZstdEncoder(CompressionDefault, 64<<10)
	defer enc.Close()
	for _, size := range []int{64, 104, 0} {
		path := filepath.Join(t.TempDir(), "legacy.db")
		requireOK(t, os.WriteFile(path, base, 0o600))
		patchSuperblocks(t, path, func(f *osFile, head *superblock) {
			if size == 0 {
				head.sections[secTSDict].codec = 2
				return
			}
			d := &head.sections[secTSIndex]
			if d.count == 0 {
				t.Fatal("missing TS index")
			}
			packed, err := sectionRaw(f, *d, nil)
			requireOK(t, err)
			current, err := zstdDecompress(packed)
			requireOK(t, err)
			raw := make([]byte, int(d.count)*size)
			for i := range int(d.count) {
				copy(raw[i*size:i*size+52], current[i*tsIndexEntryBytes:i*tsIndexEntryBytes+52])
			}
			z := enc.EncodeAll(raw, nil)
			end := fileSize(t, path)
			_, err = f.WriteAt(z, end)
			requireOK(t, err)
			d.offset, d.length, d.rawLen, d.crc = uint64(end), uint64(len(z)), uint32(len(raw)), crc(z)
		})
		reader, err := Open(path, Options{ReadOnly: true})
		if err == nil {
			_ = reader.Close()
		}
		if size == 0 {
			check(err, "unsupported dictionary codec")
		} else {
			check(err, "invalid TS index entry size")
		}
	}

	legacyRow := make([]byte, 68+1+65)
	binary.LittleEndian.PutUint16(legacyRow, 7)
	legacyRow[2], legacyRow[35], legacyRow[68], legacyRow[69], legacyRow[133] = resMinuteHR, 0x0f, 1, 1, 0x0f
	wrappedRow := enc.EncodeAll(legacyRow, nil)
	_, err = decodeBlock(wrappedRow)
	check(err, "invalid block marker")
	slot := newQuerySlot()
	defer slot.close()
	_, _, err = decodeSmallBlockCols(wrappedRow, slot)
	check(err, "invalid small column block")
	stream, err := encodeBlockLevel(sealedBlock{series: 7, res: resMinuteHR, rows: make([]idRow, 256)}, CompressionFastest)
	requireOK(t, err)
	_, err = decodeBlock(enc.EncodeAll(stream[len(blockStreamsMagic):], nil))
	check(err, "invalid block marker")

	legacyDelta := []byte{0, 0xd2, 0x8f, 0x53, 0x32, 0}
	if _, _, err := decodeDelta(legacyDelta); err == nil {
		t.Fatal("accepted old delta magic")
	}
	if _, _, _, err := scanDeltaBounds(legacyDelta); err == nil {
		t.Fatal("scanned old delta magic")
	}
	batch := encodeKVBatch(1, []kvOp{{kind: kvPut, region: 1, key: []byte("key"), value: []byte("v")}})
	legacyBatch := append(append(slices.Clone(batch[:8]), 0xa9, 0x4b, 0x56, 0x32), batch[8:]...)
	for _, raw := range [][]byte{legacyBatch, batch[:len(batch)-2]} {
		if _, _, err := decodeKVBatch(raw); err == nil {
			t.Fatal("accepted old batch magic or truncated key")
		}
	}
	block := encodeKVBlockWithEncoder([]kvSSTEntry{{region: 1, key: []byte("k"), value: []byte("v"), modRev: 1}}, enc)
	plain, err := zstdDecompress(block)
	requireOK(t, err)
	if _, err := decodeKVBlock(enc.EncodeAll(append([]byte{0xb7, 0x4b, 0x56, 0x32}, plain...), nil)); err == nil {
		t.Fatal("accepted old KV block magic")
	}
}

func TestResetAllCompactLifecycle(t *testing.T) {
	t.Run("CoarseBlocksAndRetention", func(t *testing.T) {
		now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
		db, s := testDB(t, &now, Options{Retention1m: 6 * time.Hour})
		defer func() { _ = db.Close() }()
		oldDay, oldHour := now.Add(-72*time.Hour), now.Add(-2*time.Hour)
		appendValue(t, s, oldDay, 10)
		appendValue(t, s, oldHour, 20)
		requireOK(t, db.Compact())
		resolutions := make(map[byte]bool)
		for _, m := range db.sealed {
			resolutions[m.res] = true
		}
		if !resolutions[resMinuteHR] || !resolutions[resHour] || !resolutions[resDay] {
			t.Fatalf("missing initial resolutions: %v", resolutions)
		}

		requireOK(t, s.ResetAll())
		if lo, hi := searchBounds(db, s.id); lo != 0 || hi != 0 {
			t.Fatalf("old coarse blocks entered search bounds: %d..%d", lo, hi)
		}
		agg, err := s.Aggregate(t.Context(), AggregateQuery{AllTime: true, RequireExact: true, GroupDims: []int{0}})
		requireOK(t, err)
		empty := agg.Exact && len(agg.Segments) == 0 && len(agg.Groups) == 1 && len(agg.Groups[0]) == 0
		agg.Release()
		if !empty {
			t.Fatal("old coarse blocks entered the exact grouped query")
		}
		checkExactRows := func(wantRows int, wantSum int64, wantFrom time.Time) {
			t.Helper()
			page, err := s.Read(t.Context(), ReadRequest{AllTime: true, RequireExact: true, Limit: 10})
			requireOK(t, err)
			var readSum int64
			for _, r := range page.Rows {
				readSum += r.Values[0]
			}
			exact, rows, reqFrom := page.Exact, len(page.Rows), page.RequestedFrom
			page.Release()
			if !exact || rows != wantRows || readSum != wantSum || (!wantFrom.IsZero() && !reqFrom.Equal(wantFrom)) {
				t.Fatalf("exact AllTime Read: exact=%t rows=%d/%d sum=%d/%d from=%v wantFrom=%v", exact, rows, wantRows, readSum, wantSum, reqFrom, wantFrom)
			}
			n, sum, err := scanCountSumErr(t.Context(), s, ScanQuery{AllTime: true, RequireExact: true})
			if err != nil || n != wantRows || sum != wantSum {
				t.Fatalf("exact AllTime Scan: rows=%d/%d sum=%d/%d err=%v", n, wantRows, sum, wantSum, err)
			}
		}
		checkExactRows(0, 0, time.Time{})

		fresh := now.Add(-3 * time.Hour)
		appendValue(t, s, fresh, 200)
		appendValue(t, s, fresh.Add(time.Minute), 300)
		if lo, hi := searchBounds(db, s.id); lo != fresh.Unix() || hi != fresh.Add(2*time.Minute).Unix() {
			t.Fatalf("post-reset search bounds: %d..%d", lo, hi)
		}
		checkExactRows(2, 500, fresh)
		assertQuerySum(t, s, AggregateQuery{AllTime: true, RequireExact: true, Filters: tagFilter(0, TagEq, "a")}, 500)
		agg, err = s.Aggregate(t.Context(), AggregateQuery{AllTime: true, RequireExact: true})
		requireOK(t, err)
		exact := agg.Exact && agg.RequestedFrom.Equal(fresh) && agg.Summary.Values[0] == 500
		agg.Release()
		if !exact {
			t.Fatal("post-reset exact summary retained the old window")
		}
		requireOK(t, readCheckErr(t.Context(), s, ReadRequest{From: oldDay, To: now, Limit: 10}, func(p *PageResult) error {
			if len(p.Rows) != 2 || p.Rows[0].Tags[0] != "a" || p.Rows[1].Tags[0] != "a" {
				return fmt.Errorf("post-reset rows: %+v", p.Rows)
			}
			return nil
		}))
		n, sum, err := scanCountSumErr(t.Context(), s, ScanQuery{From: oldDay, To: now})
		if err != nil || n != 2 || sum != 500 {
			t.Fatalf("post-reset Scan rows=%d sum=%d err=%v", n, sum, err)
		}

		before := dictStats(t, s)
		pending := len(db.pendingDict)
		if err := s.Append(fresh, Summary{Values: testValues(3)}, testRow(1, "must-not-leak", "g"), testRow(2, "wrong-arity")); err == nil {
			t.Fatal("Append accepted a wrong-arity row")
		}
		if !slices.Equal(before, dictStats(t, s)) || len(db.pendingDict) != pending {
			t.Fatal("wrong-arity append changed dictionary state")
		}
		requireOK(t, mustRegion(t, db, "meta").Put("state", []byte("active")))
		target := db.appendSeq
		now = now.Add(10 * time.Minute)
		requireOK(t, db.Compact())
		if db.head.sealedWalSeq < target || db.head.sealedKVRev == 0 || len(s.rules) != 0 {
			t.Fatal("compaction did not seal the reset and KV write")
		}
		var hour sealedMeta
		for _, m := range db.sealed {
			if m.epoch == 0 {
				t.Fatal("old epoch block survived compaction")
			}
			if m.res == resHour {
				hour = m
			}
		}
		if hour.res != resHour {
			t.Fatal("no new hour block")
		}
		rm := computeRebuildMarks(&compactPlan{
			oldSealed: []sealedMeta{hour}, hourTarget: hourTS(now.Unix()),
			series: map[uint16]*seriesSnap{s.id: {rules: []resetRule{{oldEpoch: 0, from: math.MinInt64, to: math.MaxInt64}}}},
		})
		if len(rm.hour)+len(rm.day)+len(rm.dirtyHr)+len(rm.dirtyDay) != 0 {
			t.Fatal("old reset dirtied the new hour block")
		}
		db = reopenTestDB(t, db, db.path, db.opts)
		s = mustSeries(t, db, s.id, s.cfg)
		if s.resetEpoch != 1 || len(s.rules) != 0 {
			t.Fatal("reopen lost sealed reset state")
		}
		assertKV(t, mustRegion(t, db, "meta"), "state", []byte("active"), true)
		now = now.Add(7 * time.Hour)
		requireOK(t, db.Compact())
		if len(db.sealed) != 1 || db.sealed[0].res != resHour || db.sealed[0].crc != hour.crc || db.sealed[0].order != hour.order {
			t.Fatalf("hour block did not pass through minute expiry: %+v", db.sealed)
		}
		assertTSSum(t, s, fresh, fresh.Add(time.Hour), 500)
	})

	t.Run("UnsealedHotFrame", func(t *testing.T) {
		now := time.Date(2026, 6, 11, 10, 0, 15, 0, time.UTC)
		db, s := testDB(t, &now, Options{})
		defer func() { _ = db.Close() }()
		requireOK(t, s.AppendSync(now, Summary{Values: testValues(5)}, testRow(5, "old", "g")))
		requireOK(t, s.ResetAll())
		requireOK(t, s.AppendSync(now, Summary{Values: testValues(2)}, testRow(1, "new-1", "g"), testRow(1, "new-2", "g")))
		requireOK(t, db.Compact())
		gen := db.head.gen
		db.autoCompact()
		if db.head.gen != gen || len(s.rules) != 1 {
			t.Fatal("hot frame did not block sealing or autoCompact spun")
		}
		for _, sealed := range []bool{false, true} {
			db = reopenTestDB(t, db, db.path, db.opts)
			s = mustSeries(t, db, s.id, s.cfg)
			if s.resetEpoch != 1 || (len(s.rules) == 0) != sealed {
				t.Fatalf("reopened reset: epoch=%d rules=%+v sealed=%t", s.resetEpoch, s.rules, sealed)
			}
			st := dictStats(t, s)[0]
			if st.MaxID != 2 || st.LiveCount != 2 || st.DeadCount != 0 {
				t.Fatalf("old epoch dictionary leaked: %+v", st)
			}
			assertQuerySum(t, s, AggregateQuery{AllTime: true}, 2)
			assertQuerySum(t, s, AggregateQuery{AllTime: true, Filters: tagFilter(0, TagEq, "old")}, 0)
			if !sealed {
				now = now.Add(5 * time.Minute)
				db.autoCompact()
				if db.head.sealedWalSeq != db.appendSeq || len(db.sealed) != 1 || db.sealed[0].epoch != 1 {
					t.Fatal("advanced clock did not seal the new epoch")
				}
			}
		}
	})
}

func TestResetCurrentDayRequiresFineData(t *testing.T) {
	now := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{Retention1m: time.Hour})
	defer db.Close()
	early := now.Add(-10 * time.Hour)
	appendValue(t, s, early, 7)
	requireOK(t, db.Compact())
	if len(db.sealed) != 1 || db.sealed[0].res != resHour {
		t.Fatalf("expected only the coarse hour: %+v", db.sealed)
	}
	requireOK(t, s.ResetCurrentDay(time.UTC))
	ctx := t.Context()
	for _, err := range []error{
		func() error { _, err := aggSumErr(ctx, s, AggregateQuery{From: early, To: now}); return err }(),
		func() error { _, err := aggSumErr(ctx, s, AggregateQuery{AllTime: true}); return err }(),
		readCheckErr(ctx, s, ReadRequest{From: early, To: now, Limit: 10}, nil),
		s.Scan(ctx, ScanQuery{From: early, To: now}, func(ScanBatch) error { return nil }),
	} {
		if err != ErrResetRequiresFineData {
			t.Fatalf("coarse reset query: got %v want %v", err, ErrResetRequiresFineData)
		}
	}
}

func TestAutoCompactResetWatermarks(t *testing.T) {
	for _, late := range []bool{true, false} {
		now := time.Date(2026, 6, 11, 10, 15, 15, 0, time.UTC)
		db, s := testDB(t, &now, Options{})
		defer db.Close()
		if late {
			appendValue(t, s, now.Add(-5*time.Minute), 1)
			requireOK(t, s.ResetAll())
			appendValue(t, s, now, 2)
			appendValue(t, s, now.Add(-10*time.Minute), 3)
		} else {
			appendValue(t, s, now.Truncate(time.Minute).Add(time.Minute), 10)
			requireOK(t, s.ResetCurrentDay(time.UTC))
			now = now.Add(3 * time.Minute)
		}
		gen := db.head.gen
		db.autoCompact()
		if db.head.gen != gen {
			t.Fatalf("autoCompact spun on a blocked reset (late write=%t)", late)
		}
	}
}

func TestResetAllDictRotationAndStats(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{})
	defer func() { _ = db.Close() }()
	other := mustSeries(t, db, 22, s.cfg)
	seed := now.Add(-2 * time.Hour)
	const count = 17000
	for _, series := range []*Series{s, other} {
		seedHighCardMinute(t, series, seed, count, func(i int) []string {
			return []string{fmt.Sprintf("old-%05d", i), fmt.Sprintf("grp-%02d", i%20)}
		})
	}
	requireOK(t, db.Compact())
	oldSize := fileSize(t, db.path)
	oldDict := db.head.sections[secTSDict].length
	otherStats := dictStats(t, other)
	for _, series := range []*Series{s, other} {
		st := dictStats(t, series)[0]
		if st.MaxID != count || st.LiveCount != count || st.DeadCount != 0 || st.ExternalBytes == 0 || st.ForwardPages == 0 || st.ReverseBlocks == 0 {
			t.Fatalf("seed did not build an external dictionary: %+v", st)
		}
	}
	ro, err := Open(db.path, Options{ReadOnly: true})
	requireOK(t, err)
	defer ro.Close()
	rs, rother := mustSeries(t, ro, s.id, s.cfg), mustSeries(t, ro, other.id, other.cfg)

	requireOK(t, s.ResetAll())
	for dim, st := range dictStats(t, s) {
		if st != (DictDimStats{Dim: dim}) {
			t.Fatalf("ResetAll did not clear dimension %d: %+v", dim, st)
		}
	}
	seedHighCardMinute(t, s, now.Add(-5*time.Minute), 3, func(i int) []string {
		return []string{fmt.Sprintf("fresh-%d", i), "new"}
	})
	requireOK(t, db.Sync())
	verify := func(reset, keep *Series) {
		t.Helper()
		st := dictStats(t, reset)
		if st[0].MaxID != 3 || st[0].LiveCount != 3 || st[0].DeadCount != 0 || st[0].ExternalBytes != 0 || st[1].MaxID != 1 {
			t.Fatalf("unexpected rotated dictionary: %+v", st)
		}
		assertQuerySum(t, reset, AggregateQuery{AllTime: true, Filters: tagFilter(0, TagPrefix, "fresh-")}, 3)
		assertQuerySum(t, reset, AggregateQuery{AllTime: true, Filters: tagFilter(0, TagEq, "old-00042")}, 0)
		assertQuerySum(t, keep, AggregateQuery{AllTime: true, Filters: tagFilter(0, TagEq, "old-00042")}, 1)
	}
	requireOK(t, ro.Refresh())
	verify(rs, rother)
	now = now.Add(10 * time.Minute)
	requireOK(t, db.Compact())
	if fileSize(t, db.path) >= oldSize || db.head.sections[secTSDict].length >= oldDict {
		t.Fatal("compaction did not physically reclaim the rotated dictionary")
	}
	requireOK(t, ro.Refresh())
	verify(rs, rother)
	db = reopenTestDB(t, db, db.path, db.opts)
	s, other = mustSeries(t, db, s.id, s.cfg), mustSeries(t, db, other.id, other.cfg)
	verify(s, other)
	if !slices.Equal(dictStats(t, other), otherStats) {
		t.Fatal("reopen changed the untouched dictionary")
	}
	client := NewClient(db, ClientOptions{})
	defer client.Close()
	st, err := client.DictStats(s.id)
	requireOK(t, err)
	if !slices.Equal(st, dictStats(t, s)) {
		t.Fatal("client dictionary stats diverged")
	}
	requireOK(t, client.Close())
	requireOK(t, db.Close())
	if _, err := s.DictStats(); err == nil {
		t.Fatal("DictStats succeeded after Close")
	}
}

func TestBatch1AtomicityAndStartupCleanup(t *testing.T) {
	t.Run("RefreshPreservesState", func(t *testing.T) {
		now := time.Now()
		db, s := testDB(t, &now, Options{})
		defer db.Close()
		requireOK(t, s.Append(now.Add(-2*time.Hour), Summary{Values: testValues(10)}, testRow(10, "k1", "g")))
		requireOK(t, db.Refresh())
		if len(db.pending) != 1 {
			t.Fatal("writer Refresh discarded pending data")
		}
		requireOK(t, db.Compact())
		path := db.path
		reader, err := Open(path, Options{ReadOnly: true})
		requireOK(t, err)
		defer reader.Close()
		rs := mustSeries(t, reader, s.id, s.cfg)
		oldGen := reader.head.gen
		requireOK(t, db.Close())

		raw, err := os.ReadFile(path)
		requireOK(t, err)
		bad := path + ".corrupt"
		requireOK(t, os.WriteFile(bad, raw, 0o600))
		patchSuperblocks(t, bad, func(_ *osFile, h *superblock) {
			h.gen++
			h.sections[secTSIndex].crc ^= 1
		})
		requireOK(t, renameAtomic(bad, path))
		if err := reader.Refresh(); err == nil || reader.head.gen != oldGen {
			t.Fatalf("corrupt Refresh changed generation: err=%v gen=%d want=%d", err, reader.head.gen, oldGen)
		}
		assertQuerySum(t, rs, AggregateQuery{AllTime: true, Filters: tagFilter(0, TagEq, "k1")}, 10)
	})

	t.Run("OpenCleansCrashTempFiles", func(t *testing.T) {
		now := time.Now()
		db, _ := testDB(t, &now, Options{})
		requireOK(t, db.Close())
		suffixes := []string{".tmp", ".invtmp", ".invchk"}
		for _, suffix := range suffixes {
			requireOK(t, os.WriteFile(db.path+suffix, []byte("stale"), 0o600))
		}
		next := reopenTestDB(t, db, db.path, db.opts)
		defer next.Close()
		for _, suffix := range suffixes {
			if _, err := os.Stat(next.path + suffix); !os.IsNotExist(err) {
				t.Fatalf("startup did not remove %s: %v", suffix, err)
			}
		}
	})

	t.Run("FlushRollbackAndRetry", func(t *testing.T) {
		now := time.Now()
		db, s := testDB(t, &now, Options{})
		defer func() { _ = db.Close() }()
		for i := 1; i <= 2; i++ {
			n := int64(i * 10)
			requireOK(t, s.Append(now.Add(-time.Duration(i)*time.Minute), Summary{Values: testValues(n)}, testRow(n, "k", "g")))
		}
		pos, seq, allocated := db.walPos, db.appendSeq, db.allocatedSeq
		ro, err := openData(db.path, true)
		requireOK(t, err)
		db.data, ro = ro, db.data
		err = db.Flush()
		db.data, ro = ro, db.data
		requireOK(t, ro.Close())
		if err == nil || db.walPos != pos || db.appendSeq != seq || db.allocatedSeq != allocated || len(db.pending) != 2 {
			t.Fatalf("Flush did not roll back: err=%v pos=%d seq=%d allocated=%d pending=%d", err, db.walPos, db.appendSeq, db.allocatedSeq, len(db.pending))
		}
		requireOK(t, db.Sync())
		if db.appendSeq != seq+2 || db.allocatedSeq != allocated+2 {
			t.Fatal("retry allocated incorrect WAL sequences")
		}
		db = reopenTestDB(t, db, db.path, db.opts)
		assertQuerySum(t, mustSeries(t, db, s.id, s.cfg), AggregateQuery{AllTime: true}, 30)
	})
}

func TestDictGCStage2SlidingWindowChurn(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{Retention1m: 2 * time.Hour, Retention1h: 4 * time.Hour, Retention1d: 6 * time.Hour})
	defer func() { _ = db.Close() }()
	const keep, churn, rounds = 8, 1800, 13
	var promo DictDimStats
	var promoSize int64
	for r := range rounds {
		now = now.Add(12 * time.Hour)
		rows := make([]Row, 0, keep+churn+1)
		for i := range keep {
			rows = append(rows, testRow(10, fmt.Sprintf("2001:db8:ffff::%x", i+1), "persist"))
		}
		for i := range churn {
			rows = append(rows, testRow(1, fmt.Sprintf("2001:db8:%x::%x", r+1, i+1), "churn"))
		}
		rows = append(rows, testRow(5, "", "empty"))
		requireOK(t, s.Append(now.Add(-10*time.Minute), Summary{Values: [4]int64{keep*10 + churn + 5, 1, 10, 10}}, rows...))
		requireOK(t, db.Compact())
		st := dictStats(t, s)[0]
		if st.MaxID != uint32(keep+(r+1)*churn) || st.LiveCount != keep+churn || st.DeadCount != st.MaxID-st.LiveCount {
			t.Fatalf("round %d dictionary stats: %+v", r, st)
		}
		if promo.ExternalBytes == 0 && st.ExternalBytes != 0 {
			promo, promoSize = st, fileSize(t, db.path)
		}
	}
	final := dictStats(t, s)[0]
	if promo.ExternalBytes == 0 || final.ExternalBytes > 2*promo.ExternalBytes || fileSize(t, db.path) > 2*promoSize {
		t.Fatalf("dictionary size did not stabilize: promotion=%+v final=%+v", promo, final)
	}
	checkFolded := func() {
		t.Helper()
		dim := db.dict.dims[dictDimKey{series: s.id, dim: 0}]
		if dim == nil || dim.ext == nil {
			t.Fatal("missing external dictionary")
		}
		pages, folded := dim.ext.fwdPages, 0
		for _, page := range pages {
			if page.length == 0 {
				folded++
			}
		}
		if folded < 35 || final.ForwardPages != uint32(len(pages)-folded) || final.ForwardPages > 8 {
			t.Fatalf("forward pages: total=%d folded=%d live=%d", len(pages), folded, final.ForwardPages)
		}
	}
	checkFolded()
	requireOK(t, db.Compact())
	db = reopenTestDB(t, db, db.path, db.opts)
	s = mustSeries(t, db, s.id, s.cfg)
	if got := dictStats(t, s)[0]; got != final {
		t.Fatalf("GC stats changed on reopen: got=%+v want=%+v", got, final)
	}
	checkFolded()

	for _, tc := range []struct {
		op  TagOp
		tag string
		sum int64
	}{
		{TagEq, "2001:db8:ffff::1", 10}, {TagEq, fmt.Sprintf("2001:db8:%x::1", rounds), 1},
		{TagEq, "2001:db8:1::1", 0}, {TagEq, "", 5},
		{TagPrefix, "2001:db8:ffff::", keep * 10}, {TagCIDR, "2001:db8:ffff::/48", keep * 10},
	} {
		assertQuerySum(t, s, AggregateQuery{AllTime: true, Filters: tagFilter(0, tc.op, tc.tag)}, tc.sum)
	}
	top, err := s.Aggregate(t.Context(), AggregateQuery{AllTime: true, GroupDims: []int{0}, TopK: 10})
	requireOK(t, err)
	emptyGroup := len(top.Groups) == 1 && len(top.Groups[0]) == 10 && top.Groups[0][keep].Tag == "" &&
		top.Groups[0][keep].Summary.Values[0] == 5
	top.Release()
	if !emptyGroup {
		t.Fatal("grouping after GC lost the empty tag")
	}
	reader := ReadRequest{AllTime: true, Filters: tagFilter(0, TagPrefix, "2001:db8:ffff::"), Limit: 20}
	requireOK(t, readCheckErr(t.Context(), s, reader, func(page *PageResult) error {
		if len(page.Rows) != keep {
			return fmt.Errorf("Read after GC returned %d persistent rows, want %d", len(page.Rows), keep)
		}
		for _, row := range page.Rows {
			if !strings.HasPrefix(row.Tags[0], "2001:db8:ffff::") || row.Tags[1] != "persist" {
				return fmt.Errorf("Read lost persistent dictionary tags: %v", row.Tags)
			}
		}
		return nil
	}))
	old := "2001:db8:1::1"
	requireOK(t, s.AppendSync(now.Add(-5*time.Minute), Summary{Values: testValues(42)}, testRow(42, old, "reborn")))
	if got := dictStats(t, s)[0].MaxID; got != final.MaxID+1 {
		t.Fatalf("reinsertion reused a dead ID: max=%d before=%d", got, final.MaxID)
	}
	requireOK(t, db.Compact())
	assertQuerySum(t, s, AggregateQuery{AllTime: true, Filters: tagFilter(0, TagEq, old)}, 42)
}

func TestCompactWALDeltaAndTSIndex80B(t *testing.T) {
	t.Run("RoundTripAndMalformed", func(t *testing.T) {
		dicts := []dictEntry{{1, 1, 0, "alpha"}, {1 << 28, 65535, 7, "2001:db8::1"}, {42, 2, 3, ""}}
		base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC).Unix()
		var buckets []*bucket
		ops := [4]MetricOp{OpSum, OpMin, OpMax, OpLast}
		for arity := range 9 {
			b := &bucket{
				series: uint16(1 + arity%2), ts: base + int64(arity%2)*3600 + int64(arity)*60,
				epoch: uint64(arity + 1), order: uint64(100 + arity), arity: uint8(arity),
				summaryOps: ops, rowOps: [4]MetricOp{OpLast, OpMax, OpMin, OpSum},
				summary: Summary{Values: [4]int64{math.MinInt64, -1, 0, math.MaxInt64}, Mask: []uint8{0, 1, 5, 10, 15}[arity%5]},
			}
			for i, mask := range []uint8{0, 3, 6, 12} {
				row := idRow{values: [4]int64{math.MinInt64, int64(-i), int64(i * 100000), math.MaxInt64}, initMask: mask}
				for d := range row.tags {
					row.tags[d] = uint32(1000 + i*8 + d)
				}
				b.rows = append(b.rows, row)
			}
			buckets = append(buckets, b)
		}

		raw := encodeDelta(dicts, buckets)
		gotDicts, got, err := decodeDelta(raw)
		requireOK(t, err)
		if !slices.Equal(gotDicts, dicts) || len(got) != len(buckets) {
			t.Fatal("delta changed dictionaries or bucket count")
		}
		maskValues := func(values *[4]int64, mask uint8) {
			for i := range values {
				if mask&(1<<i) == 0 {
					values[i] = 0
				}
			}
		}
		for i, want := range buckets {
			want.summary.Mask = normalizeMask(want.summary.Mask)
			maskValues(&want.summary.Values, want.summary.Mask)
			for j := range want.rows {
				row := &want.rows[j]
				row.initMask = normalizeMask(row.initMask)
				clear(row.tags[want.arity:])
				maskValues(&row.values, row.initMask)
			}
			if !reflect.DeepEqual(got[i], want) {
				t.Fatalf("bucket %d: got=%+v want=%+v", i, got[i], want)
			}
		}
		lo, hi, n, err := scanDeltaBounds(raw)
		if err != nil || n != len(buckets) || lo != base || hi != buckets[7].ts {
			t.Fatalf("delta bounds: %d..%d count=%d err=%v", lo, hi, n, err)
		}
		hour, err := decodeDeltaForHour(raw, 2, base+3600)
		requireOK(t, err)
		if !reflect.DeepEqual(hour, []*bucket{buckets[1], buckets[3], buckets[5], buckets[7]}) {
			t.Fatalf("unexpected hour-specific decode: %+v", hour)
		}

		for k := 0; k < len(raw); k += max(len(raw)/16, 1) {
			if _, _, err := decodeDelta(raw[:k]); err == nil {
				t.Fatalf("accepted truncation at %d", k)
			}
		}
		trailing := append(slices.Clone(raw), 0)
		badArity := encodeDelta(nil, []*bucket{{series: 2, ts: base + 3600, arity: 9}})
		single := encodeDelta(nil, []*bucket{buckets[2]})
		size, vn := binary.Uvarint(single[2:])
		bodyTrail := append(binary.AppendUvarint([]byte{0, 1}, size+1), single[2+vn:]...)
		bodyTrail = append(bodyTrail, 0)
		for i, input := range [][]byte{raw[:len(raw)-1], trailing, badArity, bodyTrail} {
			if _, _, err := decodeDelta(input); err == nil {
				t.Errorf("accepted malformed input %d", i)
			}
		}
		if _, _, _, err := scanDeltaBounds(trailing); err == nil {
			t.Error("bounds scanner accepted trailing data")
		}
		for _, input := range [][]byte{trailing, badArity} {
			if _, err := decodeDeltaForHour(input, 2, base+3600); err == nil {
				t.Error("hour decode accepted trailing data or invalid arity")
			}
		}
	})

	t.Run("SizeReductionAndTSIndex80B", func(t *testing.T) {
		b := &bucket{series: 1, arity: 2, summary: Summary{Mask: 3}, rows: make([]idRow, 200)}
		for i := range b.rows {
			b.rows[i] = idRow{
				tags:     [8]uint32{uint32(100 + i*17), uint32(1 + i%32)},
				values:   [4]int64{int64(4096 + i*13), int64(64 + i%256)},
				initMask: 3,
			}
		}
		if size := len(encodeDelta(nil, []*bucket{b})); size >= len(b.rows)*65*35/100 {
			t.Fatalf("delta not at least 65%% smaller than legacy: %dB", size)
		}

		now := time.Date(2026, 10, 1, 14, 0, 0, 0, time.UTC)
		db, s := testDB(t, &now, Options{})
		defer func() { _ = db.Close() }()
		for i := 1; i <= 3; i++ {
			ts := now.Add(-time.Duration(i) * time.Hour)
			appendValue(t, s, ts, 10)
		}
		requireOK(t, db.Compact())
		idx := db.head.sections[secTSIndex]
		if idx.count == 0 || idx.rawLen != idx.count*80 || tsIndexEntryBytes != 80 {
			t.Fatalf("SecTSIndex layout: %+v", idx)
		}
		db = reopenTestDB(t, db, db.path, db.opts)
		assertQuerySum(t, mustSeries(t, db, s.id, s.cfg), AggregateQuery{AllTime: true}, 30)
	})
}
