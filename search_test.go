package strata

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"
)

func testRowMatches(row Row, filters []Filter) bool {
	for _, f := range filters {
		matched := false
		if f.Dim >= 0 && f.Dim < len(row.Tags) {
			for _, value := range f.Values {
				if tagMatches(f.Op, row.Tags[f.Dim], value) {
					matched = true
					break
				}
			}
		}
		if f.Negate {
			if matched {
				return false
			}
		} else if !matched {
			return false
		}
	}
	return true
}

func scanAllRows(t *testing.T, s *Series, q ScanQuery) ([]Row, Summary) {
	t.Helper()
	var rows []Row
	var total Summary
	var order uint64
	if err := s.Scan(context.Background(), q, func(b ScanBatch) error {
		for i := range b.Len() {
			r := b.CloneRow(i)
			rows = append(rows, r)
			order++
			mergeSummary(&total, Summary{Values: r.Values, Mask: r.Mask}, s.cfg.RowOps, order, order-1)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return rows, total
}

func TestSearchBasicAndLive(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{})
	if err := s.AppendSync(now.Add(-2*time.Hour), Summary{Values: [4]int64{1, 1, 1, 1}}, Row{Tags: []string{"alpha", "net"}, Values: [4]int64{1, 1, 1, 1}}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendSync(now.Add(-time.Minute), Summary{Values: [4]int64{2, 2, 2, 2}}, Row{Tags: []string{"beta", "net"}, Values: [4]int64{2, 2, 2, 2}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendSync(now, Summary{Values: [4]int64{3, 3, 3, 3}}, Row{Tags: []string{"gamma", "live"}, Values: [4]int64{3, 3, 3, 3}}); err != nil {
		t.Fatal(err)
	}
	page, err := s.Read(ctx, ReadRequest{From: now.Add(-3 * time.Hour), To: now.Add(time.Minute), Filters: []Filter{{Dim: 0, Op: TagPrefix, Values: []string{"a"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Rows) != 1 || page.Rows[0].Tags[0] != "alpha" {
		t.Fatalf("unexpected read result: %+v", page.Rows)
	}
	page.Release()

	rows, _ := scanAllRows(t, s, ScanQuery{From: now.Add(-3 * time.Hour), To: now.Add(time.Minute), Filters: []Filter{{Dim: 0, Op: TagContains, Values: []string{"a"}}}})
	if len(rows) != 3 {
		t.Fatalf("expected alpha, beta, gamma, got %d", len(rows))
	}
	_ = db.Close()
}

func TestInvertedSearchEqualsFullScanFuzz(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "search.db")
	db, err := Open(path, Options{Now: func() time.Time { return now }, Retention1m: 14 * 24 * time.Hour, Retention1h: 90 * 24 * time.Hour, Retention1d: 365 * 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	s, err := db.Series(88, SeriesConfig{Arity: 7, SummaryOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast}, RowOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast}})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 7 * 24 {
		tags := []string{"alpha", "app-" + strconv.Itoa(i%3), "192.0.2." + strconv.Itoa(i%4), "2001:db8::" + strconv.Itoa(i%4), "cat-" + strconv.Itoa(i%2), "rule-" + strconv.Itoa(i%5), "network"}
		ts := now.Add(-7*24*time.Hour + time.Duration(i)*time.Hour)
		if err = s.Append(ts, Summary{Values: [4]int64{1, int64(i), int64(i), int64(i)}}, Row{Tags: tags, Values: [4]int64{1, int64(i), int64(i), int64(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err = s.ResetCurrentDay(time.UTC); err != nil {
		t.Fatal(err)
	}
	hotRows := []Row{
		{Tags: []string{"hot-alpha", "live-0", "198.51.100.1", "2001:db8:1::1", "hot-cat", "hot-rule", "hot-network"}, Values: [4]int64{10, 10, 10, 10}},
		{Tags: []string{"hot-beta", "live-1", "198.51.100.2", "2001:db8:1::2", "hot-cat", "hot-rule", "hot-network"}, Values: [4]int64{11, 11, 11, 11}},
		{Tags: []string{"hot-gamma", "live-2", "198.51.100.3", "2001:db8:1::3", "hot-cat", "hot-rule", "hot-network"}, Values: [4]int64{12, 12, 12, 12}},
	}
	dayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	for i := 0; i <= 12*60; i++ {
		row := hotRows[i%len(hotRows)]
		row.Values = [4]int64{1, int64(i), int64(i), int64(i)}
		if err = s.Append(dayStart.Add(time.Duration(i)*time.Minute), Summary{Values: [4]int64{1, 1, int64(i), int64(i)}}, row); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.Flush(); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(7))
	filterCases := []Filter{
		{Dim: 0, Op: TagEq, Values: []string{"alpha"}},
		{Dim: 1, Op: TagPrefix, Values: []string{"app-"}},
		{Dim: 1, Op: TagSuffix, Values: []string{"1", "2"}},
		{Dim: 4, Op: TagContains, Values: []string{"cat"}},
		{Dim: 2, Op: TagCIDR, Values: []string{"192.0.2.0/30"}},
		{Dim: 3, Op: TagCIDR, Values: []string{"2001:db8::/126"}},
		{Dim: 5, Op: TagEq, Values: []string{"rule-1", "rule-3"}},
		{Dim: 0, Op: TagEq, Values: []string{"alpha"}, Negate: true},
		{Dim: 0, Op: TagPrefix, Values: []string{"hot-"}},
		{Dim: 1, Op: TagPrefix, Values: []string{"live-"}},
		{Dim: 4, Op: TagContains, Values: []string{"hot"}},
		{Dim: 5, Op: TagEq, Values: []string{"hot-rule"}, Negate: true},
	}
	for i := range 80 {
		filterCount := 1 + rng.Intn(3)
		filters := make([]Filter, filterCount)
		hasPositive := false
		for j := range filters {
			filters[j] = filterCases[rng.Intn(len(filterCases))]
			hasPositive = hasPositive || !filters[j].Negate
		}
		if i%3 == 0 {
			filters[0].Negate = true
		}
		if !hasPositive {
			filters[len(filters)-1].Negate = false
		}
		sq := ScanQuery{Filters: filters, RequireExact: rng.Intn(3) == 0, PreferFine: true}
		if rng.Intn(3) != 0 {
			sq.From = now.Add(-time.Duration(rng.Intn(7*24)) * time.Hour)
			sq.To = sq.From.Add(time.Duration(1+rng.Intn(6*24)) * time.Hour)
			if sq.To.After(now.Add(time.Minute)) {
				sq.To = now.Add(time.Minute)
			}
		} else {
			sq.AllTime = true
		}

		collectFiltered := func(q ScanQuery) ([]Row, Summary, error) {
			var rows []Row
			var sum Summary
			var order uint64
			err := s.Scan(ctx, q, func(b ScanBatch) error {
				for idx := range b.Len() {
					r := b.CloneRow(idx)
					rows = append(rows, r)
					order++
					mergeSummary(&sum, Summary{Values: r.Values, Mask: r.Mask}, s.cfg.RowOps, order, order-1)
				}
				return nil
			})
			return rows, sum, err
		}

		gotRows, gotSum, err := collectFiltered(sq)
		if err != nil {
			if err == ErrResetRequiresFineData {
				sq.AllTime = false
				sq.From, sq.To = now, now.Add(time.Minute)
				sq.PreferFine = true
				gotRows, gotSum, err = collectFiltered(sq)
			}
			if sq.RequireExact && err == ErrResolutionTooCoarse {
				sq.RequireExact = false
				gotRows, gotSum, err = collectFiltered(sq)
			}
			if err != nil {
				t.Fatal(err)
			}
		}

		// Compare against Aggregate with the same query.
		aggRes, err := s.Aggregate(ctx, AggregateQuery{
			From: sq.From, To: sq.To, AllTime: sq.AllTime,
			Filters: sq.Filters, PreferFine: sq.PreferFine, RequireExact: sq.RequireExact,
		})
		if err != nil {
			t.Fatal(err)
		}
		if aggRes.Summary != gotSum {
			t.Fatalf("case=%d aggregate vs scan summary mismatch: agg=%+v scan=%+v", i, aggRes.Summary, gotSum)
		}
		aggRes.Release()

		// Compare against unfiltered scan filtered in-memory.
		unfiltered := ScanQuery{From: sq.From, To: sq.To, AllTime: sq.AllTime, PreferFine: sq.PreferFine, RequireExact: sq.RequireExact}
		rawRows, _, err := collectFiltered(unfiltered)
		if err == ErrResetRequiresFineData {
			rawRows, err = fullScanFineRows(s, sq.From, sq.To, sq.AllTime)
		}
		if err != nil && unfiltered.RequireExact && err == ErrResolutionTooCoarse {
			unfiltered.RequireExact = false
			rawRows, _, err = collectFiltered(unfiltered)
		}
		if err != nil {
			t.Fatal(err)
		}
		var wantRows []Row
		var wantSum Summary
		var wantOrder uint64
		for _, r := range rawRows {
			if !testRowMatches(r, sq.Filters) {
				continue
			}
			wantRows = append(wantRows, r)
			wantOrder++
			mergeSummary(&wantSum, Summary{Values: r.Values, Mask: r.Mask}, s.cfg.RowOps, wantOrder, wantOrder-1)
		}
		if gotSum != wantSum || !reflect.DeepEqual(gotRows, wantRows) {
			t.Fatalf("case=%d mismatch: gotSum=%+v wantSum=%+v gotRows=%d wantRows=%d", i, gotSum, wantSum, len(gotRows), len(wantRows))
		}
	}
	_ = db.Close()
}

func fullScanFineRows(s *Series, from, to time.Time, allTime bool) ([]Row, error) {
	db := s.db
	db.mu.RLock()
	defer db.mu.RUnlock()
	fromSec, toSec := from.Unix(), to.Unix()
	if allTime {
		fromSec, toSec = searchBounds(db, s.id)
	}
	var fine []sealedBlock
	for _, meta := range db.sealed {
		if meta.series != s.id || meta.res != resMinuteHR || meta.end() <= fromSec || meta.start >= toSec {
			continue
		}
		if blockExcludedRules(s.rules, meta.start, meta.end(), meta.epoch) {
			continue
		}
		decoded, err := loadSealedBlockFromView(db.view, db.head.sections[secTSBlocks], meta)
		if err != nil {
			return nil, err
		}
		fine = append(fine, decoded)
	}
	for key, b := range db.live {
		if key.series != s.id || b.ts+60 <= fromSec || b.ts >= toSec || blockExcludedRules(s.rules, b.ts, b.ts+60, b.epoch) {
			continue
		}
		fine = append(fine, sealedBlock{
			series: s.id, res: resMinuteHR, start: b.ts, end: b.ts + 60,
			epoch: b.epoch, order: b.order, rows: cloneRows(b.rows),
		})
	}
	sortBlocks(fine)
	var rows []Row
	for _, b := range fine {
		for _, idr := range b.rows {
			tags := make([]string, s.cfg.Arity)
			for d := range tags {
				tags[d] = db.dict.resolve(s.id, uint8(d), idr.tags[d])
			}
			rows = append(rows, Row{Tags: tags, Values: idr.values, Mask: idr.initMask})
		}
	}
	return rows, nil
}

func TestScanProjectionsAndEarlyStop(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	c, err := OpenClient(filepath.Join(t.TempDir(), "scan.db"), ClientOptions{DB: Options{Now: func() time.Time { return now }}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s, _ := c.RegisterSeries(1, SeriesConfig{Arity: 3, SummaryOps: [4]MetricOp{OpSum}, RowOps: [4]MetricOp{OpSum}})
	rows := make([]Row, 320)
	for i := range rows {
		rows[i] = Row{Tags: []string{"d-" + strconv.Itoa(i), "a-" + strconv.Itoa(i%5), "tcp"}, Values: [4]int64{int64(i + 1), int64((i + 1) * 10), 3, 4}, Mask: 0x0f}
	}
	if err := s.Append(now.Add(-time.Hour), Summary{Values: [4]int64{1}}, rows...); err != nil {
		t.Fatal(err)
	}
	if err := c.Compact(); err != nil {
		t.Fatal(err)
	}

	total := 0
	var cloned []Row
	err = c.Scan(ctx, 1, ScanQuery{AllTime: true, ProjectDims: 1 << 0, ProjectMetrics: 1 << 1, BatchSize: 64}, func(b ScanBatch) error {
		if b.Len() > 64 {
			t.Fatalf("batch exceeded BatchSize: %d", b.Len())
		}
		for i := range b.Len() {
			if b.TagID(i, 0) == 0 || b.TagID(i, 1) != 0 || b.TagID(i, 2) != 0 {
				t.Fatalf("dim projection mismatch: %d %d %d", b.TagID(i, 0), b.TagID(i, 1), b.TagID(i, 2))
			}
			vals := b.Values(i)
			if vals[0] != 0 || vals[1] == 0 || vals[2] != 0 || vals[3] != 0 {
				t.Fatalf("metric projection mismatch: %v", vals)
			}
			if len(cloned) < 2 {
				cloned = append(cloned, b.CloneRow(i))
			}
			total++
		}
		return nil
	})
	if err != nil || total != 320 || len(cloned) != 2 || cloned[0].Tags[0] == cloned[1].Tags[0] {
		t.Fatalf("Scan projection failed: total=%d err=%v cloned=%+v", total, err, cloned)
	}

	seen := 0
	for b, seqErr := range c.ScanSeq(ctx, 1, ScanQuery{AllTime: true, BatchSize: 64}) {
		if seqErr != nil {
			t.Fatal(seqErr)
		}
		seen += b.Len()
		break
	}
	if seen != 64 || len(c.DB().querySem) != 0 {
		t.Fatalf("ScanSeq early break seen=%d sem=%d", seen, len(c.DB().querySem))
	}
}

func TestAdaptiveInvert5Modes(t *testing.T) {
	var sc invertRecordScratch
	k := encodeInvertKey(invertKey{series: 1, dim: 0, tag: 7})
	contig := make([]uint32, 50)
	for i := range contig {
		contig[i] = uint32(10 + i)
	}
	alternating := make([]uint32, 60)
	for i := range alternating {
		alternating[i] = uint32(100 + i*2)
	}
	for _, tc := range []struct {
		name  string
		ids   []uint32
		dense bool
		mode  byte
	}{
		{"denseSkip", nil, true, invertModeDenseSkip},
		{"singleton", []uint32{42}, false, invertModeSingleton},
		{"rle", contig, false, invertModeRLE},
		{"bitset", alternating, false, invertModeBitset},
		{"deltaVarint", []uint32{5, 5000, 90000}, false, invertModeDeltaVarint},
	} {
		rec := append([]byte(nil), encodeInvertRecordAdaptive(k, tc.ids, tc.dense, &sc)...)
		if rec[8] != tc.mode {
			t.Fatalf("%s: mode=%d want=%d", tc.name, rec[8], tc.mode)
		}
		var gotIDs []uint32
		gotDense := false
		if err := decodeInvertChunkScan(rec, map[uint64]struct{}{k: {}}, func(_ uint64, id uint32) {
			gotIDs = append(gotIDs, id)
		}, func(_ uint64) {
			gotDense = true
		}); err != nil {
			t.Fatalf("%s decode err: %v", tc.name, err)
		}
		if gotDense != tc.dense || !slices.Equal(gotIDs, tc.ids) {
			t.Fatalf("%s roundtrip mismatch: dense=%v ids=%v", tc.name, gotDense, gotIDs)
		}
	}

	// End-to-end equivalence across passthrough Compact for dense/singleton/rle/sparse/orphan tags.
	base := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	clock := base.Add(90 * time.Minute)
	db, err := Open(filepath.Join(t.TempDir(), "modes.db"), Options{
		Now: func() time.Time { return clock }, Retention1m: 7 * 24 * time.Hour, Retention1h: 30 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, _ := db.Series(3, SeriesConfig{Arity: 1})
	for m := range 72 {
		ts := base.Add(time.Duration(m) * time.Minute)
		rows := []Row{{Tags: []string{"dense"}, Values: [4]int64{1}}}
		if m == 65 {
			rows = append(rows, Row{Tags: []string{"single"}, Values: [4]int64{1}})
		}
		if m >= 10 && m < 30 {
			rows = append(rows, Row{Tags: []string{"contig"}, Values: [4]int64{1}})
		}
		if m == 0 || m == 25 || m == 50 || m == 71 {
			rows = append(rows, Row{Tags: []string{"sparse"}, Values: [4]int64{1}})
		}
		if err := s.Append(ts, Summary{Values: [4]int64{int64(len(rows))}}, rows...); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for tag, want := range map[string]int64{"dense": 72, "single": 1, "contig": 20, "sparse": 4} {
		res, err := s.Aggregate(ctx, AggregateQuery{From: base, To: base.Add(2 * time.Hour), Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{tag}}}})
		if err != nil || res.Summary.Values[0] != want {
			t.Fatalf("tag=%s got=%+v want=%d err=%v", tag, res, want, err)
		}
		res.Release()
	}
}

func TestExternalDictThreeTierPriority(t *testing.T) {
	base := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	clock := base.Add(6 * time.Hour)
	db, err := Open(filepath.Join(t.TempDir(), "tiers.db"), Options{Now: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s, _ := db.Series(22, SeriesConfig{Arity: 1})
	rows := make([]Row, 17000)
	for i := range rows {
		rows[i] = Row{Tags: []string{fmt.Sprintf("m-ext-%05d", i)}, Values: [4]int64{50, 50, 1, 0}}
	}
	if err := s.Append(base, Summary{Values: [4]int64{17000 * 50, 17000 * 50, 17000, 0}}, rows...); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if db.head.sections[secTSDict].codec != 1 {
		t.Fatalf("expected external dict codec=1, got %d", db.head.sections[secTSDict].codec)
	}

	tailRows := make([]Row, 100)
	for i := range tailRows {
		tailRows[i] = Row{Tags: []string{fmt.Sprintf("b-tail-%03d", i)}, Values: [4]int64{50, 50, 1, 0}}
	}
	if err := s.Append(base.Add(time.Hour), Summary{Values: [4]int64{100 * 50, 100 * 50, 100, 0}}, tailRows...); err != nil {
		t.Fatal(err)
	}
	dim := db.dict.dims[dictDimKey{series: 22, dim: 0}]
	if dim == nil || dim.ext == nil || dim.tailBase != 17001 {
		t.Fatalf("unexpected external dim state: ext=%v tailBase=%d", dim != nil && dim.ext != nil, dim.tailBase)
	}
	dim.extra = map[uint32]string{
		513:   "a-extra-ext-513",
		16385: "a-extra-ext-16k",
		17005: "a-extra-tail-005",
	}

	snap := dictionarySnapshot(db.dict, 22)
	reader := newDictTagReader(snap, 22, 0, nil)
	defer reader.close()
	for _, id := range []uint32{1, 513, 16385, 17000, 17001, 17005, 17100} {
		if got, want := string(reader.resolveBytes(id)), snap.resolve(22, 0, id); got != want {
			t.Fatalf("resolveBytes(%d)=%q want %q", id, got, want)
		}
	}
	res, err := s.Aggregate(context.Background(), AggregateQuery{From: base, To: clock, GroupDims: []int{0}, TopK: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer res.Release()
	if res.Groups[0][0].Tag != "a-extra-ext-16k" || res.Groups[0][1].Tag != "a-extra-ext-513" || res.Groups[0][2].Tag != "a-extra-tail-005" {
		t.Fatalf("3-tier priority mismatch (extra > tail > ext): %+v", res.Groups[0][:3])
	}
}

func TestEmptyTagFilteringAndWideCursorPagination(t *testing.T) {
	base := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	now := base.Add(30 * time.Minute)
	db, err := Open(filepath.Join(t.TempDir(), "empty_tag.db"), Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	cfg := SeriesConfig{Arity: 2, SummaryOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast}, RowOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast}}
	s, err := db.Series(31, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AppendSync(base, Summary{Values: [4]int64{70, 10, 40, 40}},
		Row{Tags: []string{"", "d1"}, Values: [4]int64{10, 10, 10, 10}},
		Row{Tags: []string{"foo", "d1"}, Values: [4]int64{20, 20, 20, 20}},
		Row{Tags: []string{"bar", "d1"}, Values: [4]int64{40, 40, 40, 40}},
	); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendSync(now, Summary{Values: [4]int64{45, 5, 25, 25}},
		Row{Tags: []string{"", "d2"}, Values: [4]int64{5, 5, 5, 5}},
		Row{Tags: []string{"foo", "d2"}, Values: [4]int64{15, 15, 15, 15}},
		Row{Tags: []string{"baz", "d2"}, Values: [4]int64{25, 25, 25, 25}},
	); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	cases := []struct {
		name      string
		filters   []Filter
		wantCount int
		wantSum   int64
	}{
		{
			name:      "eq-empty",
			filters:   []Filter{{Dim: 0, Op: TagEq, Values: []string{""}}},
			wantCount: 2,
			wantSum:   15,
		},
		{
			name:      "neg-eq-empty",
			filters:   []Filter{{Dim: 0, Op: TagEq, Values: []string{""}, Negate: true}},
			wantCount: 4,
			wantSum:   100,
		},
		{
			name:      "eq-empty-and-foo",
			filters:   []Filter{{Dim: 0, Op: TagEq, Values: []string{"", "foo"}}},
			wantCount: 4,
			wantSum:   50,
		},
	}
	for _, tc := range cases {
		agg, err := s.Aggregate(ctx, AggregateQuery{From: base, To: now.Add(time.Minute), Filters: tc.filters})
		if err != nil {
			t.Fatalf("%s Aggregate: %v", tc.name, err)
		}
		aggSum := agg.Summary.Values[0]
		agg.Release()
		if aggSum != tc.wantSum {
			t.Fatalf("%s Aggregate sum=%d want=%d", tc.name, aggSum, tc.wantSum)
		}

		scanCount, scanSum, err := scanCountSumErr(ctx, s, ScanQuery{From: base, To: now.Add(time.Minute), Filters: tc.filters})
		if err != nil {
			t.Fatalf("%s Scan: %v", tc.name, err)
		}
		if scanCount != tc.wantCount || scanSum != tc.wantSum {
			t.Fatalf("%s Scan count=%d sum=%d want count=%d sum=%d", tc.name, scanCount, scanSum, tc.wantCount, tc.wantSum)
		}

		page, err := s.Read(ctx, ReadRequest{From: base, To: now.Add(time.Minute), Filters: tc.filters, Limit: 20})
		if err != nil {
			t.Fatalf("%s Read: %v", tc.name, err)
		}
		var readSum int64
		for _, r := range page.Rows {
			readSum += r.Values[0]
		}
		readCount := len(page.Rows)
		page.Release()
		if readCount != tc.wantCount || readSum != tc.wantSum {
			t.Fatalf("%s Read count=%d sum=%d want count=%d sum=%d", tc.name, readCount, readSum, tc.wantCount, tc.wantSum)
		}
	}

	wideFrom := time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC)
	wideTo := time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	var cur Cursor
	var totalRows, pages int
	var totalSum int64
	for {
		page, err := s.Read(ctx, ReadRequest{From: wideFrom, To: wideTo, Limit: 2, Cursor: cur})
		if err != nil {
			t.Fatalf("wide Read page %d: %v", pages, err)
		}
		if page.RequestedFrom.Unix() != wideFrom.Unix() || page.RequestedTo.Unix() != wideTo.Unix() {
			page.Release()
			t.Fatalf("wide window mismatch: got [%v, %v] want [%v, %v]", page.RequestedFrom, page.RequestedTo, wideFrom, wideTo)
		}
		for _, r := range page.Rows {
			totalRows++
			totalSum += r.Values[0]
		}
		hasMore := page.HasMore
		nextCur := page.NextCursor
		page.Release()
		pages++
		if !hasMore {
			break
		}
		if nextCur.IsZero() {
			t.Fatalf("expected non-zero NextCursor on page %d", pages)
		}
		parsedCur, parseErr := ParseCursor(nextCur.String())
		if parseErr != nil || parsedCur.IsZero() {
			t.Fatalf("ParseCursor failed on page %d: err=%v zero=%v", pages, parseErr, parsedCur.IsZero())
		}
		cur = parsedCur
	}
	if pages < 3 || totalRows != 6 || totalSum != 115 {
		t.Fatalf("wide Read pagination mismatch: pages=%d rows=%d sum=%d", pages, totalRows, totalSum)
	}
}
