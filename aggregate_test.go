package strata

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestAggregateParallelDeterministicAllOps(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	db, _ := testDB(t, &now, Options{})
	defer db.Close()
	ops := [4]MetricOp{OpSum, OpMin, OpMax, OpLast}
	cfg := SeriesConfig{Arity: 3, SummaryOps: ops, RowOps: ops}
	s := mustSeries(t, db, 1, cfg)
	from := now.Add(-20 * time.Hour)
	for h := range 16 {
		rows := make([]Row, 320)
		var sum Summary
		for i := range rows {
			row := Row{
				Tags:   []string{fmt.Sprintf("dom-%d", i%40), fmt.Sprintf("app-%d", i/40), fmt.Sprintf("net-%d", i%2)},
				Values: [4]int64{int64(h + 1), int64(1000 - 10*h - i%7), int64(100*h + i), int64(1000*h + i)},
				Mask:   0x0f,
			}
			if h%2 == 1 && i%2 == 0 {
				row.Mask = 0x07
			}
			rows[i] = row
			mergeSummary(&sum, Summary{Values: row.Values, Mask: row.Mask}, ops, uint64(i+1), uint64(i))
		}
		mustTestOK(t, s.Append(from.Add(time.Duration(h)*time.Hour), sum, rows...))
	}
	mustTestOK(t, db.Compact())
	v := [4]int64{7, 1, 99999, 77777}
	mustTestOK(t, s.AppendSync(now.Add(-time.Minute), Summary{Values: v}, Row{Tags: []string{"dom-0", "app-0", "net-0"}, Values: v}))
	mustTestOK(t, db.Close())

	par := reopenTestDB(t, nil, db.path, Options{ReadOnly: true, Now: db.opts.Now, MemoryBudget: MemoryBudget{QueryWorkers: 2}})
	defer par.Close()
	seq := reopenTestDB(t, nil, db.path, Options{ReadOnly: true, Now: db.opts.Now, MemoryProfile: MemoryConstrained})
	defer seq.Close()
	if par.policy.queryWorkers <= 1 || seq.policy.queryWorkers != 1 {
		t.Fatal("fixture requires different query worker counts")
	}
	parallel, serial := mustSeries(t, par, s.id, cfg), mustSeries(t, seq, s.id, cfg)
	for i, q := range []AggregateQuery{
		{GroupDims: []int{0, 1}},
		{GroupDims: []int{0, 1}, Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"dom-2"}}}},
		{GroupDims: []int{0, 2}, Filters: []Filter{{Dim: 1, Op: TagEq, Values: []string{"app-3"}}, {Dim: 2, Op: TagEq, Values: []string{"net-1"}, Negate: true}}},
	} {
		q.From, q.To = from, now
		got, err := parallel.Aggregate(t.Context(), q)
		mustTestOK(t, err)
		want, err := serial.Aggregate(t.Context(), q)
		mustTestOK(t, err)
		if got.Exact != want.Exact || !got.EffectiveFrom.Equal(want.EffectiveFrom) || !got.EffectiveTo.Equal(want.EffectiveTo) ||
			got.Summary != want.Summary || !slices.Equal(got.Segments, want.Segments) || !reflect.DeepEqual(got.Groups, want.Groups) {
			t.Fatalf("query %d parallel summary=%+v serial summary=%+v", i, got.Summary, want.Summary)
		}
		got.Release()
		want.Release()
	}
}

func TestSummaryOnlyZeroDiskReadConsistency(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{})
	defer db.Close()
	start := now.Add(-5 * 24 * time.Hour)
	for h := range 5 * 24 {
		v := int64(h + 1)
		vals := [4]int64{v, v, v, v}
		mustTestOK(t, s.Append(start.Add(time.Duration(h)*time.Hour), Summary{Values: vals}, Row{Tags: []string{"h", "tcp"}, Values: vals}))
	}
	mustTestOK(t, db.Compact())
	mustTestOK(t, s.ResetCurrentDay(time.UTC))
	mustTestOK(t, db.Sync())
	ctx := t.Context()
	sErr := s.Scan(ctx, ScanQuery{From: start, To: now}, func(ScanBatch) error { return nil })
	_, aErr := s.Aggregate(ctx, AggregateQuery{From: start, To: now})
	if !errors.Is(sErr, ErrResetRequiresFineData) || !errors.Is(aErr, ErrResetRequiresFineData) {
		t.Fatalf("reset boundary Scan=%v SummaryOnly=%v", sErr, aErr)
	}
	mustTestOK(t, db.Compact())
	v := [4]int64{99, 2, 500, 888}
	mustTestOK(t, s.AppendSync(now.Add(-2*time.Minute), Summary{Values: v}, Row{Tags: []string{"live", "udp"}, Values: v}))
	mustTestOK(t, db.Close())

	ro := reopenTestDB(t, nil, db.path, Options{ReadOnly: true, Now: db.opts.Now})
	defer ro.Close()
	reader := mustSeries(t, ro, s.id, s.cfg)
	today := now.Truncate(24 * time.Hour)
	for _, q := range []AggregateQuery{
		{From: start, To: now},
		{From: start, To: today},
		{From: start, To: now, PreferFine: true},
	} {
		grouped := q
		grouped.GroupDims = []int{0}
		want, err := reader.Aggregate(ctx, grouped)
		mustTestOK(t, err)
		view := ro.view
		ro.view = nil
		got, err := reader.Aggregate(ctx, q)
		ro.view = view
		mustTestOK(t, err)
		if got.Summary != want.Summary || got.Exact != want.Exact || len(got.Segments) != len(want.Segments) {
			t.Fatalf("SummaryOnly without view %+v: got=(%+v,%v,%d) want=(%+v,%v,%d)",
				q, got.Summary, got.Exact, len(got.Segments), want.Summary, want.Exact, len(want.Segments))
		}
		got.Release()
		want.Release()
	}
}

func TestOpLastPreservedAcrossCompactAndHourRollup(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{})
	defer db.Close()
	appendRow := func(at time.Time, first, second string, last int64) {
		t.Helper()
		v := [4]int64{1, last, last, last}
		mustTestOK(t, s.Append(at, Summary{Values: v}, Row{Tags: []string{first, second}, Values: v}))
	}
	ts := now.Add(-10 * time.Minute)
	appendRow(ts, "tag-z", "shared", 100)
	seedHighCardMinute(t, s, ts, 300, func(i int) []string { return []string{"mid-" + strconv.Itoa(i), "other"} })
	appendRow(ts, "tag-a", "shared", 999)

	hour := now.Add(-4 * time.Hour)
	for m := range 60 {
		tag, value := "b", int64(m+1)
		if m == 0 || m == 59 {
			tag = "a"
		}
		if m == 59 {
			value = 600
		}
		appendRow(hour.Add(time.Duration(m)*time.Minute), tag, "g", value)
	}
	minuteQ := AggregateQuery{
		From: ts, To: ts.Add(time.Minute), PreferFine: true, GroupDims: []int{1},
		Filters: []Filter{{Dim: 1, Op: TagEq, Values: []string{"shared"}}},
	}
	checkLast := func(q AggregateQuery, want int64) {
		t.Helper()
		res, err := s.Aggregate(t.Context(), q)
		mustTestOK(t, err)
		defer res.Release()
		if res.Summary.Values[3] != want || len(res.Groups) != 1 || len(res.Groups[0]) != 1 || res.Groups[0][0].Summary.Values[3] != want {
			t.Fatalf("OpLast want=%d summary=%+v groups=%+v", want, res.Summary, res.Groups)
		}
		if q.From.Equal(hour) && (len(res.Segments) != 1 || res.Segments[0].Meta.Resolution != time.Hour) {
			t.Fatalf("fixture did not use 1h rollup: %+v", res.Segments)
		}
	}
	checkLast(minuteQ, 999)
	mustTestOK(t, db.Compact())
	checkLast(minuteQ, 999)
	checkLast(AggregateQuery{From: hour, To: hour.Add(time.Hour), GroupDims: []int{1}}, 600)
}

func TestAggregateTopKHighCardPartitionedAndReverseLex(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{})
	defer db.Close()
	const slots, perSlot = 3, 6000
	for m := range slots {
		rows := make([]Row, perSlot)
		var sum Summary
		for i := range rows {
			idx := m*perSlot + i
			v := [4]int64{500, 10, 50, 1000}
			if idx%3000 == 0 {
				v[0], v[3] = 9000, int64(1000+m*10+i%7)
			}
			rows[i] = Row{Tags: []string{"gw", fmt.Sprintf("ip-%05d", slots*perSlot-1-idx)}, Values: v, Mask: 0x0f}
			mergeSummary(&sum, Summary{Values: v, Mask: 0x0f}, s.cfg.RowOps, uint64(i+1), uint64(i))
		}
		mustTestOK(t, s.Append(now.Add(time.Duration(m-10)*time.Minute), sum, rows...))
	}
	mustTestOK(t, db.Compact())
	mustTestOK(t, db.Close())

	run := func(profile MemoryProfile, budget MemoryBudget) (Summary, []GroupRow) {
		reader := reopenTestDB(t, nil, db.path, Options{ReadOnly: true, Now: db.opts.Now, MemoryProfile: profile, MemoryBudget: budget})
		defer reader.Close()
		s := mustSeries(t, reader, s.id, s.cfg)
		res, err := s.Aggregate(t.Context(), AggregateQuery{AllTime: true, GroupDims: []int{1}, TopK: 20})
		mustTestOK(t, err)
		defer res.Release()
		return res.Summary, slices.Clone(res.Groups[0])
	}
	got, constrained := run(MemoryConstrained, MemoryBudget{})
	want, balanced := run(MemoryBalanced, MemoryBudget{QueryBytes: 12 << 20, QueryWorkers: 4})
	if got != want || !slices.Equal(constrained, balanced) {
		t.Fatalf("partitioned TopK parity mismatch: constrained=%+v balanced=%+v", constrained, balanced)
	}
	if len(balanced) != 20 || balanced[0].Tag != "ip-02999" || balanced[0].Summary.Values[0] != 9000 || balanced[6].Tag != "ip-00000" {
		t.Fatalf("unexpected TopK ordering: %+v", balanced)
	}
}

func TestAggregateEmptyTagUniformAcrossBackends(t *testing.T) {
	now := time.Date(2026, 11, 24, 4, 0, 0, 0, time.UTC)
	for _, fixture := range []struct {
		count int
		topKs []int
	}{{2, []int{0}}, {17001, []int{0, 10}}} {
		db, _ := testDB(t, &now, Options{})
		defer db.Close()
		s := mustSeries(t, db, 42, SeriesConfig{Arity: 1})
		rows := make([]Row, fixture.count)
		rows[0] = Row{Tags: []string{""}, Values: [4]int64{50000, 2, 3, 4}}
		for i := 1; i < len(rows); i++ {
			rows[i] = Row{Tags: []string{fmt.Sprintf("h-%05d", i)}, Values: [4]int64{1}}
		}
		mustTestOK(t, s.Append(now.Add(-4*time.Hour), Summary{}, rows...))
		mustTestOK(t, db.Compact())
		for _, topK := range fixture.topKs {
			res, err := s.Aggregate(t.Context(), AggregateQuery{AllTime: true, GroupDims: []int{0}, TopK: topK})
			mustTestOK(t, err)
			wantLen := fixture.count
			if topK > 0 {
				wantLen = topK
			}
			groups := res.Groups[0]
			if len(groups) != wantLen || groups[0].Tag != "" || groups[0].Summary.Values != rows[0].Values {
				t.Fatalf("rows=%d TopK=%d got %d groups or incorrect empty group", fixture.count, topK, len(groups))
			}
			res.Release()
		}
	}
}

func TestRollupLocalHoursDSTCounts(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	mustTestOK(t, err)
	for _, tc := range []struct {
		from    time.Time
		hours   int
		repeats int
	}{
		{time.Date(2026, 3, 8, 5, 0, 0, 0, time.UTC), 23, 1},
		{time.Date(2026, 11, 1, 4, 0, 0, 0, time.UTC), 25, 2},
	} {
		segments := make([]SegmentSummary, tc.hours)
		for h := range segments {
			at := tc.from.Add(time.Duration(h) * time.Hour)
			segments[h] = SegmentSummary{Meta: SegmentMeta{CoveredFrom: at, CoveredTo: at.Add(time.Hour), Resolution: time.Hour}, Summary: Summary{Values: [4]int64{1}, Mask: 1}}
		}
		buckets := RollupLocalHours(segments, loc, [4]MetricOp{OpSum, OpMin, OpMax, OpLast})
		var offsets []int
		for _, bucket := range buckets {
			if bucket.Hour == 1 {
				offsets = append(offsets, bucket.OffsetSeconds)
			}
		}
		if len(buckets) != tc.hours || len(offsets) != tc.repeats || tc.repeats == 2 && offsets[0] == offsets[1] {
			t.Fatalf("DST %s buckets=%d hour 1 offsets=%v", tc.from, len(buckets), offsets)
		}
	}
}
