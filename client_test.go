package strata

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestClientLifecycleAndOwnership(t *testing.T) {
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "client.db")
	opts := Options{Now: func() time.Time { return now }}
	c, err := OpenClient(path, ClientOptions{DB: opts, DefaultWindow: time.Hour})
	mustTestOK(t, err)
	defer c.Close()
	db := c.DB()
	cfg := SeriesConfig{Arity: 2}
	s, err := c.RegisterSeries(1, cfg)
	mustTestOK(t, err)
	if cached, err := c.Series(1, cfg); err != nil || cached != s {
		t.Fatalf("cached series=%v err=%v", cached, err)
	}
	if _, err := c.Series(1, SeriesConfig{Arity: 1}); err == nil {
		t.Fatal("client accepted a different config for cached series")
	}
	appendValue(t, s, now.Add(-time.Minute), 10)

	var captured *AggregateResult
	sentinel := errors.New("callback failure")
	q := AggregateQuery{GroupDims: []int{0}}
	err = c.WithAggregate(t.Context(), 1, q, func(res *AggregateResult) error {
		captured = res
		if len(res.Groups) != 1 || len(res.Groups[0]) != 1 {
			t.Fatalf("unexpected groups=%+v", res.Groups)
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) || captured == nil || captured.Groups != nil || captured.Segments != nil {
		t.Fatalf("WithAggregate callback err=%v released result=%+v", err, captured)
	}
	mustTestOK(t, c.Close())
	mustTestOK(t, c.Close())
	if c.DB() != nil || !db.closed {
		t.Fatal("closing owned client did not close its DB")
	}
	if _, err := c.Aggregate(t.Context(), 1, q); !errors.Is(err, errClientClosed) {
		t.Fatalf("closed Aggregate err=%v", err)
	}
	if err := c.NewBatch().Commit(); !errors.Is(err, errClientClosed) {
		t.Fatalf("closed NewBatch err=%v", err)
	}

	opts.ReadOnly = true
	db = reopenTestDB(t, nil, path, opts)
	defer db.Close()
	borrowed := NewClient(db, ClientOptions{DefaultWindow: time.Hour})
	res, err := borrowed.Aggregate(t.Context(), 1, AggregateQuery{})
	mustTestOK(t, err)
	if res.Summary.Values[0] != 10 {
		t.Fatalf("borrowed client failed to resolve persisted series: %+v", res.Summary)
	}
	res.Release()
	mustTestOK(t, borrowed.Close())
	assertTSSum(t, mustSeries(t, db, 1, cfg), now.Add(-time.Hour), now, 10)
}

func TestResultReleaseIdempotentAndBounded(t *testing.T) {
	(*PageResult)(nil).Release()
	(*AggregateResult)(nil).Release()
	page := &PageResult{Rows: make([]PageRow, 8), tagBuf: make([]string, 16)}
	agg := &AggregateResult{Segments: make([]SegmentSummary, 4), Groups: [][]GroupRow{make([]GroupRow, 8)}}
	for range 2 {
		page.Release()
		agg.Release()
	}
	if page.Rows != nil || page.tagBuf != nil || agg.Segments != nil || agg.Groups != nil {
		t.Fatalf("Release left buffers: page=%+v aggregate=%+v", page, agg)
	}

	reclaimPools()
	scratch := make([]byte, 0, maxZstdScratchCap+1)
	read := make([]byte, 0, maxReadBufCap+1)
	putZstdScratch(&scratch, scratch)
	putReadBuf(&read)
	if zstdDecodeScratchPool.Get() != nil || readBufPool.Get() != nil {
		t.Fatal("Release pooled an oversized buffer")
	}
}

func TestWindowGuardrails4Levels(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{})
	defer db.Close()
	c := NewClient(db, ClientOptions{DefaultWindow: 2 * time.Hour})
	defer c.Close()
	for i := range 5 {
		appendValue(t, s, now.Add(-time.Duration(i+1)*time.Hour), 10)
	}

	ctx := t.Context()
	_, readErr := s.Read(ctx, ReadRequest{})
	scanErr := s.Scan(ctx, ScanQuery{}, func(ScanBatch) error { return nil })
	_, aggErr := s.Aggregate(ctx, AggregateQuery{})
	for op, err := range map[string]error{"Read": readErr, "Scan": scanErr, "Aggregate": aggErr} {
		if !errors.Is(err, ErrWindowRequired) {
			t.Fatalf("Series.%s without window err=%v", op, err)
		}
	}
	for _, q := range []AggregateQuery{
		{AllTime: true, From: now.Add(-time.Hour)},
		{AllTime: true, Window: time.Hour},
		{From: now, To: now.Add(-time.Hour)},
		{Window: -time.Minute},
		{From: now.Add(-time.Hour), Window: time.Hour},
	} {
		if _, err := s.Aggregate(ctx, q); !errors.Is(err, ErrInvalidWindow) {
			t.Fatalf("invalid window %+v err=%v", q, err)
		}
	}
	if _, err := s.Aggregate(ctx, AggregateQuery{To: now}); !errors.Is(err, ErrWindowRequired) {
		t.Fatalf("To without Window err=%v", err)
	}

	for _, tc := range []struct {
		q    AggregateQuery
		want int64
	}{
		{AggregateQuery{}, 20},
		{AggregateQuery{Window: time.Hour}, 10},
		{AggregateQuery{To: now, Window: 3 * time.Hour}, 30},
		{AggregateQuery{From: now.Add(-4 * time.Hour), To: now}, 40},
		{AggregateQuery{AllTime: true}, 50},
	} {
		res, err := c.Aggregate(ctx, s.id, tc.q)
		mustTestOK(t, err)
		got := res.Summary.Values[0]
		res.Release()
		if got != tc.want {
			t.Fatalf("window %+v sum=%d want=%d", tc.q, got, tc.want)
		}
	}
}

func TestMemoryProfileBudgetAndCtxCancel(t *testing.T) {
	balanced := compileMemPolicy(Options{MemoryProfile: MemoryBalanced})
	constrained := compileMemPolicy(Options{MemoryProfile: MemoryConstrained})
	if balanced.queryBytes != 8<<20 || constrained.queryBytes != 2<<20 ||
		constrained.queryWorkers != 1 || constrained.compactWorkers != 1 ||
		constrained.queryBatchRows != 512 || constrained.zstdEncoderWindow != 64<<10 {
		t.Fatalf("MemoryProfile policies: balanced=%+v constrained=%+v", balanced, constrained)
	}
	custom := compileMemPolicy(Options{
		MemoryBudget: MemoryBudget{QueryBytes: 100, CompactBytes: 100, QueryWorkers: 99, QueryBatchRows: 10},
	})
	if custom.queryBytes != 1<<20 || custom.compactBytes != 2<<20 || custom.queryWorkers != 2 || custom.queryBatchRows != 64 {
		t.Fatalf("budget clamping: %+v", custom)
	}

	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{MemoryProfile: MemoryConstrained})
	defer db.Close()
	appendValue(t, s, now.Add(-time.Hour), 1)
	mustTestOK(t, db.Compact())

	db.querySem <- struct{}{}
	defer func() { <-db.querySem }()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	res, err := s.Aggregate(ctx, AggregateQuery{AllTime: true})
	mustTestOK(t, err)
	res.Release()
	canceled, stop := context.WithCancel(t.Context())
	stop()
	if _, err := s.Aggregate(canceled, AggregateQuery{AllTime: true}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled SummaryOnly err=%v", err)
	}
	deadline, stop := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer stop()
	if _, err := s.Aggregate(deadline, AggregateQuery{AllTime: true, GroupDims: []int{0}}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("grouped Aggregate with saturated querySem err=%v", err)
	}
}

func TestClientDictStats(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{})
	defer db.Close()
	c := NewClient(db, ClientOptions{})
	defer c.Close()
	if _, err := c.DictStats(99); !errors.Is(err, errSeriesUnregistered) {
		t.Fatalf("unknown series err=%v", err)
	}
	seedHighCardMinute(t, s, now.Add(-time.Hour), 2, func(i int) []string {
		return []string{[]string{"alpha", "beta"}[i], "x"}
	})
	got, err := c.DictStats(s.id)
	mustTestOK(t, err)
	want, err := s.DictStats()
	mustTestOK(t, err)
	if len(got) != 2 || !slices.Equal(got, want) || got[0].MaxID != 2 || got[0].LiveCount != 2 || got[1].MaxID != 1 {
		t.Fatalf("Client.DictStats=%+v Series.DictStats=%+v", got, want)
	}
	mustTestOK(t, c.Close())
	if _, err := c.DictStats(s.id); !errors.Is(err, errClientClosed) {
		t.Fatalf("closed DictStats err=%v", err)
	}
}
