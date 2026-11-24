package strata

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestAggregateParallelDeterministicAllOps(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "parallel-agg.db")
	cfg := SeriesConfig{
		Arity:      3,
		SummaryOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast},
		RowOps:     [4]MetricOp{OpSum, OpMin, OpMax, OpLast},
	}

	db, err := Open(path, Options{
		Now:         func() time.Time { return now },
		Retention1m: 14 * 24 * time.Hour,
		Retention1h: 90 * 24 * time.Hour,
		Retention1d: 365 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := db.Series(1, cfg)
	if err != nil {
		t.Fatal(err)
	}
	start := now.Add(-20 * time.Hour)
	for h := range 16 {
		ts := start.Add(time.Duration(h) * time.Hour)
		rows := make([]Row, 320)
		var sum Summary
		for r := range rows {
			mask := uint8(0x0f)
			if h%2 == 1 && r%2 == 0 {
				mask = 0x07
			}
			vals := [4]int64{int64(h + 1), int64(1000 - h*10 - r%7), int64(h*100 + r), int64(h*1000 + r)}
			rows[r] = Row{
				Tags:   []string{"dom-" + strconv.Itoa(r%40), "app-" + strconv.Itoa(r%8), "net-" + strconv.Itoa(r%2)},
				Values: vals,
				Mask:   mask,
			}
			mergeSummary(&sum, Summary{Values: vals, Mask: mask}, cfg.SummaryOps, uint64(r+1), uint64(r))
		}
		if err := s.Append(ts, sum, rows...); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(now.Add(-time.Minute), Summary{Values: [4]int64{7, 1, 99999, 77777}, Mask: 0x0f},
		Row{Tags: []string{"dom-0", "app-0", "net-0"}, Values: [4]int64{7, 1, 99999, 77777}, Mask: 0x0f}); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	parDB, err := Open(path, Options{ReadOnly: true, MemoryProfile: MemoryBalanced, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer parDB.Close()
	seqDB, err := Open(path, Options{ReadOnly: true, MemoryProfile: MemoryConstrained, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer seqDB.Close()

	parC, seqC := NewClient(parDB, ClientOptions{}), NewClient(seqDB, ClientOptions{})
	defer parC.Close()
	defer seqC.Close()

	queries := []AggregateQuery{
		{From: start, To: now, GroupDims: []int{0, 1}},
		{From: start, To: now, Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"dom-2"}}}, GroupDims: []int{0, 1}},
		{From: start, To: now, Filters: []Filter{{Dim: 1, Op: TagEq, Values: []string{"app-3"}}, {Dim: 2, Op: TagEq, Values: []string{"net-1"}, Negate: true}}, GroupDims: []int{0, 2}},
		{From: start, To: now},
	}
	for qi, q := range queries {
		got, err := parC.Aggregate(ctx, 1, q)
		if err != nil {
			t.Fatalf("query %d parallel failed: %v", qi, err)
		}
		want, err := seqC.Aggregate(ctx, 1, q)
		if err != nil {
			t.Fatalf("query %d sequential failed: %v", qi, err)
		}
		if got.Exact != want.Exact || !got.EffectiveFrom.Equal(want.EffectiveFrom) || !got.EffectiveTo.Equal(want.EffectiveTo) {
			t.Fatalf("query %d meta mismatch: got=(%v,%v,%v) want=(%v,%v,%v)", qi, got.Exact, got.EffectiveFrom, got.EffectiveTo, want.Exact, want.EffectiveFrom, want.EffectiveTo)
		}
		if got.Summary != want.Summary {
			t.Fatalf("query %d summary mismatch: got=%+v want=%+v", qi, got.Summary, want.Summary)
		}
		if !reflect.DeepEqual(got.Segments, want.Segments) {
			t.Fatalf("query %d segments mismatch: got=%d want=%d", qi, len(got.Segments), len(want.Segments))
		}
		if !reflect.DeepEqual(got.Groups, want.Groups) {
			t.Fatalf("query %d groups mismatch:\n got=%+v\nwant=%+v", qi, got.Groups, want.Groups)
		}
		got.Release()
		want.Release()
	}
}

func TestSummaryOnlyZeroDiskReadConsistency(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "summary-only.db")
	cfg := SeriesConfig{
		Arity:      2,
		SummaryOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast},
		RowOps:     [4]MetricOp{OpSum, OpMin, OpMax, OpLast},
	}
	client, err := OpenClient(path, ClientOptions{
		DB: Options{
			Now:         func() time.Time { return now },
			Retention1m: 14 * 24 * time.Hour,
			Retention1h: 90 * 24 * time.Hour,
			Retention1d: 365 * 24 * time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := client.RegisterSeries(1, cfg)
	if err != nil {
		t.Fatal(err)
	}
	start := now.Add(-5 * 24 * time.Hour)
	for h := range 5 * 24 {
		ts := start.Add(time.Duration(h) * time.Hour)
		v := int64(h + 1)
		if err := s.Append(ts, Summary{Values: [4]int64{v, v, v, v}}, Row{Tags: []string{"h", "tcp"}, Values: [4]int64{v, v, v, v}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.ResetCurrentDay(time.UTC); err != nil {
		t.Fatal(err)
	}
	if err := client.Sync(); err != nil {
		t.Fatal(err)
	}

	// Before compacting the reset rule, Scan and SummaryOnly must agree on ErrResetRequiresFineData.
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	if sErr := s.Scan(ctx, ScanQuery{From: start, To: now}, func(ScanBatch) error { return nil }); sErr != ErrResetRequiresFineData {
		t.Fatalf("expected ErrResetRequiresFineData from Scan, got %v", sErr)
	}
	if _, aErr := s.Aggregate(ctx, AggregateQuery{From: start, To: now}); aErr != ErrResetRequiresFineData {
		t.Fatalf("expected ErrResetRequiresFineData from SummaryOnly, got %v", aErr)
	}

	if err := client.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(now.Add(-2*time.Minute), Summary{Values: [4]int64{99, 2, 500, 888}}, Row{Tags: []string{"live", "udp"}, Values: [4]int64{99, 2, 500, 888}}); err != nil {
		t.Fatal(err)
	}
	if err := client.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	ro, err := Open(path, Options{ReadOnly: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	roSeries, err := ro.Series(1, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Compute reference using disk-reading grouped Aggregate (forces full disk read of blocks).
	wantFull, err := roSeries.Aggregate(ctx, AggregateQuery{From: start, To: now, GroupDims: []int{0}})
	if err != nil {
		t.Fatal(err)
	}
	defer wantFull.Release()
	wantMixed, err := roSeries.Aggregate(ctx, AggregateQuery{From: start, To: todayStart, GroupDims: []int{0}})
	if err != nil {
		t.Fatal(err)
	}
	defer wantMixed.Release()
	wantFine, err := roSeries.Aggregate(ctx, AggregateQuery{From: start, To: now, PreferFine: true, GroupDims: []int{0}})
	if err != nil {
		t.Fatal(err)
	}
	defer wantFine.Release()

	// Detach file view so any accidental disk read fails; SummaryOnly must succeed with zero disk I/O.
	savedView := ro.view
	ro.view = nil
	defer func() { ro.view = savedView }()

	gotFull, err := roSeries.Aggregate(ctx, AggregateQuery{From: start, To: now})
	if err != nil {
		t.Fatalf("SummaryOnly full failed without file view: %v", err)
	}
	defer gotFull.Release()
	if gotFull.Summary != wantFull.Summary || gotFull.Exact != wantFull.Exact || len(gotFull.Segments) != len(wantFull.Segments) {
		t.Fatalf("SummaryOnly full mismatch: got=(%+v,%v,%d) want=(%+v,%v,%d)", gotFull.Summary, gotFull.Exact, len(gotFull.Segments), wantFull.Summary, wantFull.Exact, len(wantFull.Segments))
	}

	gotMixed, err := roSeries.Aggregate(ctx, AggregateQuery{From: start, To: todayStart})
	if err != nil {
		t.Fatalf("SummaryOnly mixed failed without file view: %v", err)
	}
	defer gotMixed.Release()
	if gotMixed.Summary != wantMixed.Summary || gotMixed.Exact != wantMixed.Exact || len(gotMixed.Segments) != len(wantMixed.Segments) {
		t.Fatalf("SummaryOnly mixed mismatch: got=(%+v,%v,%d) want=(%+v,%v,%d)", gotMixed.Summary, gotMixed.Exact, len(gotMixed.Segments), wantMixed.Summary, wantMixed.Exact, len(wantMixed.Segments))
	}

	gotFine, err := roSeries.Aggregate(ctx, AggregateQuery{From: start, To: now, PreferFine: true})
	if err != nil {
		t.Fatalf("SummaryOnly PreferFine failed without file view: %v", err)
	}
	defer gotFine.Release()
	if gotFine.Summary != wantFine.Summary || gotFine.Exact != wantFine.Exact || len(gotFine.Segments) != len(wantFine.Segments) {
		t.Fatalf("SummaryOnly PreferFine mismatch: got=(%+v,%v,%d) want=(%+v,%v,%d)", gotFine.Summary, gotFine.Exact, len(gotFine.Segments), wantFine.Summary, wantFine.Exact, len(wantFine.Segments))
	}
}

func TestOpLastPreservedAcrossCompactAndHourRollup(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	db, err := Open(filepath.Join(t.TempDir(), "oplast.db"), Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	cfg := SeriesConfig{
		Arity:      2,
		SummaryOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast},
		RowOps:     [4]MetricOp{OpSum, OpMin, OpMax, OpLast},
	}
	s, err := db.Series(1, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := now.Add(-10 * time.Minute)
	if err := s.Append(ts, Summary{Values: [4]int64{1, 100, 100, 100}, Mask: 0x0f},
		Row{Tags: []string{"tag-z", "shared"}, Values: [4]int64{1, 100, 100, 100}, Mask: 0x0f}); err != nil {
		t.Fatal(err)
	}
	fillers := make([]Row, 300)
	for i := range fillers {
		fillers[i] = Row{Tags: []string{"mid-" + strconv.Itoa(i), "other"}, Values: [4]int64{1, 50, 50, 50}, Mask: 0x0f}
	}
	if err := s.Append(ts, Summary{Values: [4]int64{300, 50, 50, 50}, Mask: 0x0f}, fillers...); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ts, Summary{Values: [4]int64{1, 999, 999, 999}, Mask: 0x0f},
		Row{Tags: []string{"tag-a", "shared"}, Values: [4]int64{1, 999, 999, 999}, Mask: 0x0f}); err != nil {
		t.Fatal(err)
	}

	// Also verify multi-minute 1h rollup keeps the chronologically latest row even when its tag sorts earlier.
	hourBase := time.Date(2026, 9, 2, 8, 0, 0, 0, time.UTC)
	if err := s.Append(hourBase, Summary{Values: [4]int64{1, 1, 1, 1}, Mask: 0x0f},
		Row{Tags: []string{"a", "g"}, Values: [4]int64{1, 1, 1, 1}, Mask: 0x0f}); err != nil {
		t.Fatal(err)
	}
	for m := 1; m < 59; m++ {
		v := int64(m + 1)
		if err := s.Append(hourBase.Add(time.Duration(m)*time.Minute), Summary{Values: [4]int64{1, v, v, v}, Mask: 0x0f},
			Row{Tags: []string{"b", "g"}, Values: [4]int64{1, v, v, v}, Mask: 0x0f}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Append(hourBase.Add(59*time.Minute), Summary{Values: [4]int64{1, 600, 600, 600}, Mask: 0x0f},
		Row{Tags: []string{"a", "g"}, Values: [4]int64{1, 600, 600, 600}, Mask: 0x0f}); err != nil {
		t.Fatal(err)
	}

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	agg, err := s.Aggregate(ctx, AggregateQuery{
		From: ts, To: ts.Add(time.Minute), PreferFine: true,
		Filters: []Filter{{Dim: 1, Op: TagEq, Values: []string{"shared"}}}, GroupDims: []int{1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if agg.Summary.Values[3] != 999 || len(agg.Groups[0]) != 1 || agg.Groups[0][0].Summary.Values[3] != 999 {
		t.Fatalf("minute OpLast mismatch: sum=%+v groups=%+v", agg.Summary, agg.Groups)
	}
	agg.Release()

	agg1h, err := s.Aggregate(ctx, AggregateQuery{From: hourBase, To: hourBase.Add(time.Hour), GroupDims: []int{1}})
	if err != nil {
		t.Fatal(err)
	}
	if agg1h.Summary.Values[3] != 600 || len(agg1h.Groups[0]) != 1 || agg1h.Groups[0][0].Summary.Values[3] != 600 {
		t.Fatalf("1h rollup OpLast mismatch: sum=%+v groups=%+v", agg1h.Summary, agg1h.Groups)
	}
	agg1h.Release()
}

func TestAggregateTopKHighCardPartitionedAndReverseLex(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	dbPath := filepath.Join(t.TempDir(), "part-topk.db")
	ops := [4]MetricOp{OpSum, OpMin, OpMax, OpLast}

	wdb, err := Open(dbPath, Options{Now: func() time.Time { return now }, MemoryProfile: MemoryBalanced})
	if err != nil {
		t.Fatal(err)
	}
	ws, err := wdb.Series(1, SeriesConfig{Arity: 2, SummaryOps: ops, RowOps: ops})
	if err != nil {
		t.Fatal(err)
	}
	// 18,000 unique keys across 2 partitions in reverse lexicographical order with tied scores.
	const totalKeys = 18000
	const slotCount = 3
	const perSlot = totalKeys / slotCount
	for slot := range slotCount {
		ts := now.Add(time.Duration(slot-10) * time.Minute)
		rows := make([]Row, perSlot)
		var sum Summary
		for i := range perSlot {
			idx := slot*perSlot + i
			tag := fmt.Sprintf("ip-%05d", totalKeys-1-idx)
			score := int64(500)
			if idx%3000 == 0 {
				score = 9000
			}
			v := [4]int64{score, 10, 50, 1000}
			if score == 9000 {
				v[3] = int64(1000 + slot*10 + i%7)
			}
			rows[i] = Row{Tags: []string{"gw", tag}, Values: v, Mask: 0x0f}
			mergeSummary(&sum, Summary{Values: v, Mask: 0x0f}, ops, uint64(i+1), uint64(i))
		}
		if err := ws.Append(ts, sum, rows...); err != nil {
			t.Fatal(err)
		}
	}
	if err := wdb.Compact(); err != nil {
		t.Fatal(err)
	}
	_ = wdb.Close()

	dbCon, err := Open(dbPath, Options{ReadOnly: true, Now: func() time.Time { return now }, MemoryProfile: MemoryConstrained})
	if err != nil {
		t.Fatal(err)
	}
	defer dbCon.Close()
	dbBal, err := Open(dbPath, Options{
		ReadOnly: true, Now: func() time.Time { return now },
		MemoryProfile: MemoryBalanced, MemoryBudget: MemoryBudget{QueryBytes: 12 << 20, QueryWorkers: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer dbBal.Close()

	sCon, _ := dbCon.Series(1, SeriesConfig{Arity: 2, SummaryOps: ops, RowOps: ops})
	sBal, _ := dbBal.Series(1, SeriesConfig{Arity: 2, SummaryOps: ops, RowOps: ops})
	q := AggregateQuery{AllTime: true, GroupDims: []int{1}, TopK: 20}
	resCon, err := sCon.Aggregate(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	defer resCon.Release()
	resBal, err := sBal.Aggregate(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	defer resBal.Release()

	if resCon.Summary != resBal.Summary || !slices.Equal(resCon.Groups[0], resBal.Groups[0]) {
		t.Fatalf("partitioned TopK parity mismatch: con=%+v bal=%+v", resCon.Groups[0], resBal.Groups[0])
	}
	// Tied score=500 winners after the 6 score=9000 entries must be lexicographically smallest ("ip-00000" ..).
	if len(resBal.Groups[0]) != 20 || resBal.Groups[0][0].Summary.Values[0] != 9000 || resBal.Groups[0][6].Tag != "ip-00000" {
		t.Fatalf("unexpected TopK tie-break ordering: %+v", resBal.Groups[0][:8])
	}
}

func TestAggregateEmptyTagUniformAcrossBackends(t *testing.T) {
	base := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	clock := base.Add(4 * time.Hour)
	ctx := context.Background()

	// Dense (low-card) backend.
	dbLow, err := Open(filepath.Join(t.TempDir(), "empty_low.db"), Options{Now: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	defer dbLow.Close()
	sLow, _ := dbLow.Series(42, SeriesConfig{Arity: 2})
	if err := sLow.Append(base, Summary{Values: [4]int64{300, 600, 3, 0}},
		Row{Tags: []string{"", "tcp"}, Values: [4]int64{200, 400, 2, 0}},
		Row{Tags: []string{"example.com", "tcp"}, Values: [4]int64{100, 200, 1, 0}},
	); err != nil {
		t.Fatal(err)
	}
	if err := dbLow.Compact(); err != nil {
		t.Fatal(err)
	}
	resDense, err := sLow.Aggregate(ctx, AggregateQuery{From: base, To: clock, GroupDims: []int{0}})
	if err != nil {
		t.Fatal(err)
	}
	defer resDense.Release()
	if len(resDense.Groups[0]) != 2 || resDense.Groups[0][0].Tag != "" || resDense.Groups[0][0].Summary.Values[0] != 200 {
		t.Fatalf("dense empty tag mismatch: %+v", resDense.Groups[0])
	}

	// High-card (>16384 tags) testing both open (TopK=0) and partitioned (TopK=10) backends.
	dbHigh, err := Open(filepath.Join(t.TempDir(), "empty_high.db"), Options{Now: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	defer dbHigh.Close()
	sHigh, _ := dbHigh.Series(43, SeriesConfig{Arity: 1})
	rows := make([]Row, 17001)
	rows[0] = Row{Tags: []string{""}, Values: [4]int64{50000, 50000, 5, 0}}
	for i := 1; i < len(rows); i++ {
		rows[i] = Row{Tags: []string{fmt.Sprintf("h-%05d", i)}, Values: [4]int64{1, 2, 1, 0}}
	}
	if err := sHigh.Append(base, Summary{Values: [4]int64{50000 + 17000, 50000 + 34000, 17005, 0}}, rows...); err != nil {
		t.Fatal(err)
	}
	if err := dbHigh.Compact(); err != nil {
		t.Fatal(err)
	}
	resOpen, err := sHigh.Aggregate(ctx, AggregateQuery{From: base, To: clock, GroupDims: []int{0}})
	if err != nil {
		t.Fatal(err)
	}
	defer resOpen.Release()
	if resOpen.Groups[0][0].Tag != "" || resOpen.Groups[0][0].Summary.Values[0] != 50000 {
		t.Fatalf("open empty tag mismatch: %+v", resOpen.Groups[0][0])
	}
	resPart, err := sHigh.Aggregate(ctx, AggregateQuery{From: base, To: clock, GroupDims: []int{0}, TopK: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer resPart.Release()
	if len(resPart.Groups[0]) != 10 || resPart.Groups[0][0].Tag != "" || resPart.Groups[0][0].Summary.Values[0] != 50000 {
		t.Fatalf("partitioned TopK empty tag mismatch: %+v", resPart.Groups[0][0])
	}
}

func TestRollupLocalHoursDSTCounts(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	makeSegments := func(from, to time.Time) []SegmentSummary {
		var segs []SegmentSummary
		for ts := from; ts.Before(to); ts = ts.Add(time.Hour) {
			segs = append(segs, SegmentSummary{Meta: SegmentMeta{
				CoveredFrom: ts, CoveredTo: ts.Add(time.Hour), Resolution: time.Hour, Exact: true,
			}, Summary: Summary{Values: [4]int64{1, 1, 1, 1}, Mask: 0x0f}})
		}
		return segs
	}
	spring := RollupLocalHours(makeSegments(time.Date(2026, 3, 8, 5, 0, 0, 0, time.UTC), time.Date(2026, 3, 9, 4, 0, 0, 0, time.UTC)), loc, [4]MetricOp{OpSum, OpMin, OpMax, OpLast})
	fall := RollupLocalHours(makeSegments(time.Date(2026, 11, 1, 4, 0, 0, 0, time.UTC), time.Date(2026, 11, 2, 5, 0, 0, 0, time.UTC)), loc, [4]MetricOp{OpSum, OpMin, OpMax, OpLast})
	if len(spring) != 23 || len(fall) != 25 {
		t.Fatalf("DST bucket counts spring=%d fall=%d", len(spring), len(fall))
	}
}
