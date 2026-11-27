package strata

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestTagEqLiveSnapshotWithoutMap(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{})
	defer db.Close()
	from := now.Add(-2 * time.Minute)
	requireOK(t, s.Append(from, Summary{Values: testValues(12)}, testRow(7, "first", "b"), testRow(5, "", "b")))
	requireOK(t, s.Append(from.Add(time.Minute), Summary{Values: testValues(13)}, testRow(13, "other", "b")))
	dim := db.dict.dims[dictDimKey{series: s.id}]
	dim.byKey = nil
	check := func(key string, fast, empty bool, want int64) {
		t.Helper()
		snap, _, gotEmpty, err := captureQuerySnapshot(db, s, windowSpec{from: from, to: now}, tagFilter(0, TagEq, key))
		requireOK(t, err)
		defer snap.release()
		if gotEmpty != empty || snap.livePreFiltered != fast {
			t.Fatalf("%q fast=%v empty=%v", key, snap.livePreFiltered, gotEmpty)
		}
		if !fast || empty {
			return
		}
		if !snap.hasLiveMatch || len(snap.liveFine) != 2 {
			t.Fatalf("%q lost live coverage", key)
		}
		for _, b := range snap.liveFine {
			if b.start == from.Unix() {
				if !b.candidate || len(b.rows) != 1 || b.rows[0].values[0] != want {
					t.Fatalf("%q matching bucket = %+v", key, b)
				}
			} else if b.candidate || len(b.rows) != 0 || b.rowCount != 1 || b.summary.Values[0] != 13 {
				t.Fatalf("%q noncandidate bucket = %+v", key, b)
			}
		}
	}
	check("first", true, false, 7)
	check("missing", true, true, 0)
	dim.buildSorted()
	requireOK(t, s.Append(from, Summary{}, testRow(9, "tail", "b")))
	check("first", true, false, 7)
	check("tail", true, false, 9)
	check("missing", true, true, 0)
	check("", true, false, 5)
	requireOK(t, dim.appendValue(130, "distant"))
	check("first", false, false, 0) // Large unmerged tail.
	dim.sorted = nil
	check("first", false, false, 0) // Sparse ID domain despite a small live count.
	dim.buildSorted()
	dim.extra = map[uint32]string{1: "override"}
	check("first", false, false, 0)
}

func TestWindowBlockBitmapBounds(t *testing.T) {
	const base = uint32(69)
	for _, span := range []uint32{1, 64, 256, 257} {
		bm := newBlockBitmap(base, span)
		for _, pos := range []uint32{base - 1, base, base + span - 1, base + span, base} {
			bm.set(pos)
		}
		if !bm.has(base) || !bm.has(base+span-1) || bm.has(base-1) || bm.has(base+span) || bm.count != int(min(span, 2)) ||
			(&bm.bits[0] == &bm.inline[0]) != (span <= 256) {
			t.Fatalf("bitmap span=%d count=%d bits=%v", span, bm.count, bm.bits)
		}
		if span&63 != 0 {
			bm.bits[len(bm.bits)-1] |= uint64(1) << (span & 63)
			if bm.has(base + span) {
				t.Fatalf("span %d admitted padding", span)
			}
		}
	}
}

func TestSinglePostingWindowKeepsGlobalDensityAndCoverage(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "invert"))
	requireOK(t, err)
	defer f.Close()
	const from, to = int64(3600), int64(3660)
	window := [3][]uint32{{72, 73, 74, 75, 76, 77, 78}, {71}, {70}}
	snap := querySnapshot{series: Series{id: 1}, sealed: make([]sealedMeta, 100), fineAll: make([]uint32, 98),
		hourAll: window[1], dayAll: window[2], view: &fileView{data: &osFile{f: f}}}
	snap.sealed[70] = sealedMeta{series: 1, res: resDay}
	snap.sealed[71] = sealedMeta{series: 1, res: resHour, start: from}
	for _, pos := range window[0] {
		snap.sealed[pos] = sealedMeta{series: 1, res: resMinuteHR, start: from}
	}
	key := encodeInvertKey(invertKey{series: 1, tag: 7})
	var scratch invertRecordScratch
	dense := make([]uint32, 36)
	for i := range dense {
		dense[i] = uint32(i)
	}
	dense[len(dense)-1] = 71
	// Repeated IDs are valid delta inputs and count once toward global density.
	duplicates := append(binary.LittleEndian.AppendUint64(nil, key), invertModeDeltaVarint, 41, 5)
	duplicates = append(duplicates, make([]byte, 39)...)
	duplicates = append(duplicates, 66)
	for _, tc := range []struct {
		name string
		raw  []byte
		want int
	}{
		{"global dense", slices.Clone(encodeInvertRecordAdaptive(key, dense, false, &scratch)), -1},
		{"dense skip", slices.Clone(encodeInvertRecordAdaptive(key, nil, true, &scratch)), -1},
		{"outside only", slices.Clone(encodeInvertRecordAdaptive(key, []uint32{5}, false, &scratch)), 0},
		{"duplicates and coarse", duplicates, 1},
	} {
		_, err := f.WriteAt(tc.raw, 0)
		requireOK(t, err)
		snap.invertSec = sectionDesc{length: uint64(len(tc.raw))}
		snap.invert = []invertChunkMeta{{first: key, last: key, length: uint32(len(tc.raw)), crc: crc(tc.raw)}}
		bm, empty, err := searchCandidatesSnap(&snap, window, tagFilter(0, TagEq, "tag"), []compiledFilter{{ids: []uint32{7}}}, false, nil)
		requireOK(t, err)
		if empty || (bm == nil) != (tc.want < 0) {
			t.Fatalf("%s bitmap=%v empty=%v", tc.name, bm, empty)
		}
		if bm == nil {
			continue
		}
		if bm.base != 70 || bm.span != 9 || bm.count != tc.want || bm.has(5) || bm.has(71) != (tc.want == 1) {
			t.Fatalf("%s bitmap=%+v", tc.name, bm)
		}
		sources := buildQuerySourcesFromSnap(&snap, window[0], window[1], window[2], from, to, bm)
		if len(sources.hours.idx) != 1 || sources.hours.candIdx == nil || len(sources.hours.candIdx) != tc.want {
			t.Fatalf("%s lost coarse coverage or candidate: %+v", tc.name, sources.hours)
		}
		res := queryResult{sparse: true, Exact: true}
		plans, err := planQuerySlots(&res, from, to, sources, &snap.series, queryOptions{RequireExact: true}, bm)
		if tc.want == 1 && !errors.Is(err, ErrResolutionTooCoarse) || tc.want == 0 && (err != nil || len(plans) != 0) {
			t.Fatalf("%s exact plan length=%d err=%v", tc.name, len(plans), err)
		}
	}
}

func TestTagEqLiveWindowSkipsSealedInversion(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{})
	defer db.Close()
	appendValue(t, s, now.Add(-time.Hour), 100)
	requireOK(t, db.Compact())
	if len(db.invert) == 0 {
		t.Fatal("fixture must have an inverted index outside the query window")
	}
	from := now.Add(-3 * time.Minute)
	requireOK(t, s.Append(from, Summary{Values: testValues(27)}, testRow(7, "a", "b"), testRow(20, "other", "b")))
	requireOK(t, s.Append(from.Add(time.Minute), Summary{Values: testValues(40)}, testRow(40, "other", "b")))
	to := from.Add(2 * time.Minute)
	filters := tagFilter(0, TagEq, "a")
	ws := windowSpec{from: from, to: to}
	snap, _, empty, err := captureQuerySnapshot(db, s, ws, filters)
	requireOK(t, err)
	defer snap.release()
	if empty || !snap.livePreFiltered || !snap.hasLiveMatch || len(snap.liveFine) != 2 {
		t.Fatalf("fixture did not take the live fast path: empty=%v live=%+v", empty, snap.liveFine)
	}
	for _, b := range snap.liveFine {
		if b.start == from.Unix() {
			if !b.candidate || b.rowCount != 2 || len(b.rows) != 1 || b.rows[0].values[0] != 7 || b.summary.Values[0] != 27 {
				t.Fatalf("matching live bucket=%+v", b)
			}
		} else if b.candidate || b.rows != nil || b.rowCount != 1 || b.summary.Values[0] != 40 {
			t.Fatalf("nonmatching live bucket lost coverage or summary: %+v", b)
		}
	}

	// Detach the view so a sealed inversion read fails, even though the index exists.
	view := db.view
	db.view = nil
	defer func() { db.view = view }()
	q := AggregateQuery{From: from, To: to, Filters: filters, RequireExact: true}
	assertQuerySum(t, s, q, 7)
	q.Filters = tagFilter(0, TagEq, "missing")
	miss, _, empty, err := captureQuerySnapshot(db, s, ws, q.Filters)
	requireOK(t, err)
	defer miss.release()
	if !empty || miss.dict.snapshot || len(miss.liveFine) != 0 {
		t.Fatal("unknown tag did not return before snapshot construction")
	}
	assertQuerySum(t, s, q, 0)
}

func TestInvertedSearchEqualsFullScanFuzz(t *testing.T) {
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	db, _ := testDB(t, &now, Options{})
	defer db.Close()
	ops := [4]MetricOp{OpSum, OpMin, OpMax, OpLast}
	s := mustSeries(t, db, 88, SeriesConfig{Arity: 4, SummaryOps: ops, RowOps: ops})
	base := now.Add(-24 * time.Hour)
	for i := range 24 {
		for j := range 4 {
			v := [4]int64{1, int64(i - j), int64(i + j), int64(i*4 + j)}
			row := Row{
				Tags:   []string{"host-" + strconv.Itoa(j), "app-" + strconv.Itoa(i%3), "192.0.2." + strconv.Itoa(i%8), "2001:db8::" + strconv.Itoa(j)},
				Values: v,
			}
			mustTestOK(t, s.Append(base.Add(time.Duration(i)*time.Hour), Summary{Values: v}, row))
		}
		if i == 19 {
			mustTestOK(t, db.Compact())
			if db.head.sections[secTSInvert].length == 0 {
				t.Fatal("sealed fixture has no inverted index")
			}
		}
	}

	collect := func(q ScanQuery) []Row {
		t.Helper()
		var rows []Row
		mustTestOK(t, s.Scan(t.Context(), q, func(b ScanBatch) error {
			for i := range b.Len() {
				rows = append(rows, b.CloneRow(i))
			}
			return nil
		}))
		return rows
	}
	filterCases := []Filter{
		{Dim: 0, Op: TagEq, Values: []string{"host-1", "host-2"}},
		{Dim: 0, Op: TagPrefix, Values: []string{"host-"}},
		{Dim: 1, Op: TagSuffix, Values: []string{"1", "2"}},
		{Dim: 1, Op: TagContains, Values: []string{"pp-0"}},
		{Dim: 2, Op: TagCIDR, Values: []string{"192.0.2.0/30"}},
		{Dim: 3, Op: TagCIDR, Values: []string{"2001:db8::/127"}},
		{Dim: 0, Op: TagEq, Values: []string{"absent"}},
	}
	rng := rand.New(rand.NewSource(7))
	for i := range 80 {
		q := ScanQuery{AllTime: i%3 == 0, PreferFine: true}
		if !q.AllTime {
			q.From = base.Add(time.Duration(rng.Intn(24)) * time.Hour)
			q.To = q.From.Add(time.Duration(1+rng.Intn(8)) * time.Hour)
			if i%5 == 1 {
				q.To, q.RequireExact = q.From.Add(time.Minute), true
			}
		}
		wantRows := collect(q)
		for range 1 + rng.Intn(3) {
			f := filterCases[rng.Intn(len(filterCases))]
			f.Negate = rng.Intn(3) == 0
			q.Filters = append(q.Filters, f)
			wantRows = slices.DeleteFunc(wantRows, func(r Row) bool {
				matched := slices.ContainsFunc(f.Values, func(v string) bool { return tagMatches(f.Op, r.Tags[f.Dim], v) })
				return matched == f.Negate
			})
		}
		var want Summary
		for j, row := range wantRows {
			mergeSummary(&want, Summary{Values: row.Values, Mask: row.Mask}, ops, uint64(j+1), uint64(j))
		}
		gotRows := collect(q)
		res, err := s.Aggregate(t.Context(), AggregateQuery{
			From: q.From, To: q.To, AllTime: q.AllTime, Filters: q.Filters, PreferFine: q.PreferFine,
			RequireExact: q.RequireExact,
		})
		mustTestOK(t, err)
		got := res.Summary
		res.Release()
		sameRows := slices.EqualFunc(gotRows, wantRows, func(a, b Row) bool {
			return a.Values == b.Values && a.Mask == b.Mask && slices.Equal(a.Tags, b.Tags)
		})
		if !sameRows || got != want {
			t.Fatalf("case %d filters=%+v rows=%d want=%d summary=%+v want=%+v", i, q.Filters, len(gotRows), len(wantRows), got, want)
		}
	}
}

func TestScanProjectionsAndEarlyStop(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{})
	defer db.Close()
	c := NewClient(db, ClientOptions{})
	defer c.Close()
	rows := make([]Row, 320)
	for i := range rows {
		rows[i] = Row{Tags: []string{"d-" + strconv.Itoa(i), "tcp"}, Values: [4]int64{1, int64(i + 1), 3, 4}, Mask: 0x0f}
	}
	mustTestOK(t, s.Append(now.Add(-time.Hour), Summary{}, rows...))
	mustTestOK(t, db.Compact())

	q := ScanQuery{AllTime: true, ProjectDims: 1, ProjectMetrics: 2, BatchSize: 64}
	total := 0
	var first Row
	mustTestOK(t, c.Scan(t.Context(), s.id, q, func(b ScanBatch) error {
		if b.Len() > q.BatchSize {
			t.Fatalf("batch length=%d", b.Len())
		}
		for i := range b.Len() {
			vals := b.Values(i)
			if b.TagID(i, 0) == 0 || b.TagID(i, 1) != 0 || b.Tag(i, 1) != "" ||
				vals[0] != 0 || vals[1] == 0 || vals[2] != 0 || vals[3] != 0 {
				t.Fatalf("projection row=%+v", b.CloneRow(i))
			}
			if total == 0 {
				first = b.CloneRow(i)
			}
			total++
		}
		return nil
	}))
	if total != len(rows) || first.Tags[0] != rows[0].Tags[0] || first.Values[1] != rows[0].Values[1] {
		t.Fatalf("projection total=%d first=%+v", total, first)
	}

	seen := 0
	for b, err := range c.ScanSeq(t.Context(), s.id, q) {
		mustTestOK(t, err)
		seen += b.Len()
		break
	}
	if seen != q.BatchSize || len(db.querySem) != 0 {
		t.Fatalf("ScanSeq early break seen=%d sem=%d", seen, len(db.querySem))
	}
}

func TestAdaptiveInvert5Modes(t *testing.T) {
	var scratch invertRecordScratch
	key := encodeInvertKey(invertKey{series: 1, dim: 0, tag: 7})
	contiguous, alternating := make([]uint32, 50), make([]uint32, 60)
	for i := range contiguous {
		contiguous[i] = uint32(10 + i)
	}
	for i := range alternating {
		alternating[i] = uint32(100 + 2*i)
	}
	for _, tc := range []struct {
		mode byte
		ids  []uint32
	}{
		{invertModeDenseSkip, nil},
		{invertModeSingleton, []uint32{42}},
		{invertModeRLE, contiguous},
		{invertModeBitset, alternating},
		{invertModeDeltaVarint, []uint32{5, 5000, 90000}},
	} {
		dense := tc.mode == invertModeDenseSkip
		record := encodeInvertRecordAdaptive(key, tc.ids, dense, &scratch)
		if record[8] != tc.mode {
			t.Fatalf("mode=%d want=%d", record[8], tc.mode)
		}
		var ids []uint32
		var skipped bool
		mustTestOK(t, decodeInvertChunkScan(record, map[uint64]struct{}{key: {}},
			func(_ uint64, id uint32) { ids = append(ids, id) }, func(uint64) { skipped = true }))
		if skipped != dense || !slices.Equal(ids, tc.ids) {
			t.Fatalf("mode %d round trip ids=%v dense=%v", tc.mode, ids, skipped)
		}
	}

	now := time.Date(2026, 11, 24, 1, 30, 0, 0, time.UTC)
	db, _ := testDB(t, &now, Options{})
	defer db.Close()
	s := mustSeries(t, db, 3, SeriesConfig{Arity: 1})
	base := now.Add(-90 * time.Minute)
	for m := range 72 {
		rows := []Row{{Tags: []string{"dense"}, Values: [4]int64{1}}}
		if m == 65 {
			rows = append(rows, Row{Tags: []string{"single"}, Values: [4]int64{1}})
		}
		if m >= 10 && m < 30 {
			rows = append(rows, Row{Tags: []string{"contig"}, Values: [4]int64{1}})
		}
		if m%25 == 0 || m == 71 {
			rows = append(rows, Row{Tags: []string{"sparse"}, Values: [4]int64{1}})
		}
		mustTestOK(t, s.Append(base.Add(time.Duration(m)*time.Minute), Summary{}, rows...))
	}
	mustTestOK(t, db.Compact())
	for tag, want := range map[string]int64{"dense": 72, "single": 1, "contig": 20, "sparse": 4, "absent": 0} {
		q := AggregateQuery{From: base, To: base.Add(2 * time.Hour), Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{tag}}}}
		if got := queryAggSum(t, s, q); got != want {
			t.Fatalf("tag=%s sum=%d want=%d", tag, got, want)
		}
	}
}

func TestExternalDictThreeTierPriority(t *testing.T) {
	now := time.Date(2026, 11, 24, 6, 0, 0, 0, time.UTC)
	db, _ := testDB(t, &now, Options{})
	defer db.Close()
	s := mustSeries(t, db, 22, SeriesConfig{Arity: 1})
	base := now.Add(-6 * time.Hour)
	seedHighCardMinute(t, s, base, 17000, func(i int) []string { return []string{fmt.Sprintf("m-ext-%05d", i)} })
	mustTestOK(t, db.Compact())
	seedHighCardMinute(t, s, base.Add(time.Hour), 100, func(i int) []string { return []string{fmt.Sprintf("b-tail-%03d", i)} })
	dim := db.dict.dims[dictDimKey{series: s.id, dim: 0}]
	if dim == nil || dim.ext == nil || dim.tailBase != 17001 {
		t.Fatal("fixture did not create an external dictionary and tail")
	}
	dim.extra = map[uint32]string{513: "a-extra-ext-513", 16385: "a-extra-ext-16k", 17005: "a-extra-tail-005"}
	snap := dictionarySnapshot(db.dict, s.id)
	reader := newDictTagReader(snap, s.id, 0, nil)
	defer reader.close()
	for id, want := range map[uint32]string{
		1: "m-ext-00000", 513: "a-extra-ext-513", 16385: "a-extra-ext-16k",
		17000: "m-ext-16999", 17001: "b-tail-000", 17005: "a-extra-tail-005", 17100: "b-tail-099",
	} {
		resolved, err := snap.resolve(s.id, 0, id)
		requireOK(t, err)
		if got := string(reader.resolveBytes(id)); got != want || resolved != want || reader.err != nil {
			t.Fatalf("resolveBytes(%d)=%q want=%q", id, got, want)
		}
	}
	res, err := s.Aggregate(t.Context(), AggregateQuery{From: base, To: now, GroupDims: []int{0}, TopK: 5})
	mustTestOK(t, err)
	defer res.Release()
	var tags []string
	for _, row := range res.Groups[0] {
		tags = append(tags, row.Tag)
	}
	want := []string{"a-extra-ext-16k", "a-extra-ext-513", "a-extra-tail-005", "b-tail-000", "b-tail-001"}
	if !slices.Equal(tags, want) {
		t.Fatalf("three-tier TopK=%v want=%v", tags, want)
	}
}

func TestEmptyTagFilteringAndWideCursorPagination(t *testing.T) {
	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	now := base.Add(30 * time.Minute)
	db, s := testDB(t, &now, Options{})
	defer db.Close()
	add := func(ts time.Time, tags []string, values []int64) {
		t.Helper()
		rows := make([]Row, len(tags))
		for i, tag := range tags {
			rows[i] = Row{Tags: []string{tag, "d"}, Values: [4]int64{values[i]}}
		}
		mustTestOK(t, s.AppendSync(ts, Summary{}, rows...))
	}
	add(base, []string{"", "foo", "bar"}, []int64{10, 20, 40})
	mustTestOK(t, db.Compact())
	add(now, []string{"", "foo", "baz"}, []int64{5, 15, 25})

	ctx, end := t.Context(), now.Add(time.Minute)
	for _, tc := range []struct {
		values []string
		negate bool
		count  int
		sum    int64
	}{
		{[]string{""}, false, 2, 15},
		{[]string{""}, true, 4, 100},
		{[]string{"", "foo"}, false, 4, 50},
	} {
		filters := []Filter{{Dim: 0, Op: TagEq, Values: tc.values, Negate: tc.negate}}
		agg := queryAggSum(t, s, AggregateQuery{From: base, To: end, Filters: filters})
		count, sum, err := scanCountSumErr(ctx, s, ScanQuery{From: base, To: end, Filters: filters})
		mustTestOK(t, err)
		if agg != tc.sum || count != tc.count || sum != tc.sum {
			t.Fatalf("filters=%+v Aggregate=%d Scan=(%d,%d) want=(%d,%d)", filters, agg, count, sum, tc.count, tc.sum)
		}
		page, err := s.Read(ctx, ReadRequest{From: base, To: end, Filters: filters, Limit: 20})
		mustTestOK(t, err)
		var readSum int64
		for _, row := range page.Rows {
			readSum += row.Values[0]
		}
		count = len(page.Rows)
		page.Release()
		if count != tc.count || readSum != tc.sum {
			t.Fatalf("filters=%+v Read=(%d,%d) want=(%d,%d)", filters, count, readSum, tc.count, tc.sum)
		}
	}

	q := ReadRequest{From: time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC), To: time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), Limit: 2}
	var values []int64
	for i := range 3 {
		page, err := s.Read(ctx, q)
		mustTestOK(t, err)
		for _, row := range page.Rows {
			values = append(values, row.Values[0])
		}
		if !page.RequestedFrom.Equal(q.From) || !page.RequestedTo.Equal(q.To) || page.HasMore != (i < 2) {
			t.Fatalf("wide page %d window=[%v,%v] more=%v", i, page.RequestedFrom, page.RequestedTo, page.HasMore)
		}
		next := page.NextCursor
		page.Release()
		if i < 2 {
			q.Cursor, err = ParseCursor(next.String())
			mustTestOK(t, err)
			if q.Cursor.IsZero() {
				t.Fatalf("wide page %d lost its cursor", i)
			}
		}
	}
	slices.Sort(values)
	if !slices.Equal(values, []int64{5, 10, 15, 20, 25, 40}) {
		t.Fatalf("wide pagination values=%v", values)
	}
}
