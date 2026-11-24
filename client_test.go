package strata

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestClientLifecycleAndOwnership(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "client.db")
	cfg := SeriesConfig{
		Arity:      3,
		SummaryOps: [4]MetricOp{OpSum, OpSum, OpSum, OpSum},
		RowOps:     [4]MetricOp{OpSum, OpSum, OpSum, OpSum},
	}

	client, err := OpenClient(path, ClientOptions{
		DB:               Options{Now: func() time.Time { return now }},
		DefaultWindow:    24 * time.Hour,
		DefaultPageLimit: 50,
		MaxPageLimit:     200,
	})
	if err != nil {
		t.Fatal(err)
	}
	if client.DB() == nil {
		t.Fatal("expected non-nil DB from OpenClient")
	}
	s, err := client.RegisterSeries(1, cfg)
	if err != nil {
		t.Fatal(err)
	}
	sCached, err := client.Series(1, cfg)
	if err != nil || sCached != s {
		t.Fatalf("expected cached series handle, got %v err=%v", sCached, err)
	}
	if _, err := client.Series(1, SeriesConfig{Arity: 2}); err == nil {
		t.Fatal("expected config mismatch error")
	}

	r1, err := client.Region("meta")
	if err != nil {
		t.Fatal(err)
	}
	if r1Again, err := client.Region("meta"); err != nil || r1Again != r1 {
		t.Fatalf("expected cached named region, got %v err=%v", r1Again, err)
	}
	if err := r1.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	r2, err := client.Region("meta2")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.NewBatch().In(r1).Put("k2", []byte("v2")).In(r2).Put("k", []byte("w")).Commit(); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := r2.Get("k"); err != nil || !ok || string(v) != "w" {
		t.Fatalf("client batch get=%q ok=%v err=%v", v, ok, err)
	}

	base := now.Add(-2 * time.Hour)
	for i := range 3 {
		ts := base.Add(time.Duration(i) * time.Minute)
		row := Row{Tags: []string{"d" + strconv.Itoa(i), "app", "tcp"}, Values: [4]int64{10, 20, 1, 0}}
		if err := s.Append(ts, Summary{Values: row.Values}, row); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := client.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := client.Compact(); err != nil {
		t.Fatal(err)
	}

	// DefaultWindow kicks in when From/To/Window/AllTime are zero.
	var gotRows int
	if err := client.WithPage(ctx, 1, ReadRequest{}, func(page *PageResult) error {
		gotRows = len(page.Rows)
		return nil
	}); err != nil || gotRows != 3 {
		t.Fatalf("WithPage gotRows=%d err=%v", gotRows, err)
	}

	// Filtered Scan + ScanSeq.
	var gotScan int
	for batch, err := range client.ScanSeq(ctx, 1, ScanQuery{
		From: base, To: now,
		Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"d1"}}},
	}) {
		if err != nil {
			t.Fatal(err)
		}
		gotScan += batch.Len()
	}
	if gotScan != 1 {
		t.Fatalf("ScanSeq gotScan=%d", gotScan)
	}

	sentinel := errors.New("callback boom")
	var capturedAgg *AggregateResult
	err = client.WithAggregate(ctx, 1, AggregateQuery{From: base, To: now, GroupDims: []int{0}}, func(res *AggregateResult) error {
		capturedAgg = res
		if len(res.Groups) != 1 || len(res.Groups[0]) != 3 {
			t.Fatalf("unexpected groups: %+v", res.Groups)
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected callback error, got %v", err)
	}
	if capturedAgg == nil || capturedAgg.Groups != nil || capturedAgg.Segments != nil {
		t.Fatalf("expected WithAggregate to release result even on callback error")
	}

	client.Reclaim()
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
	if client.DB() != nil {
		t.Fatal("closed client should return nil DB")
	}
	if _, err := client.Aggregate(ctx, 1, AggregateQuery{From: base, To: now}); err == nil {
		t.Fatal("expected error after Close")
	}
	if err := client.NewBatch().In(r1).Put("after", []byte("close")).Commit(); !errors.Is(err, errClientClosed) {
		t.Fatalf("expected errClientClosed from closed client NewBatch, got %v", err)
	}

	// Borrowed client via NewClient must not close underlying DB.
	db, err := Open(path, Options{ReadOnly: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	borrowed := NewClient(db, ClientOptions{DefaultWindow: 24 * time.Hour})
	if err := borrowed.Refresh(); err != nil {
		t.Fatal(err)
	}
	var aggTotal int64
	if err := borrowed.WithAggregate(ctx, 1, AggregateQuery{}, func(res *AggregateResult) error {
		aggTotal = res.Summary.Values[0]
		return nil
	}); err != nil || aggTotal != 30 {
		t.Fatalf("borrowed client auto-resolved series failed: total=%d err=%v", aggTotal, err)
	}
	if err := borrowed.Close(); err != nil {
		t.Fatal(err)
	}
	// Underlying DB must still be open and readable.
	sAfter, err := db.Series(1, cfg)
	if err != nil {
		t.Fatalf("db closed by borrowed client: %v", err)
	}
	resAfter, err := sAfter.Aggregate(ctx, AggregateQuery{From: base, To: now})
	if err != nil || resAfter.Summary.Values[0] != 30 {
		t.Fatalf("db unusable after borrowed client close: res=%+v err=%v", resAfter, err)
	}
	resAfter.Release()
}

func TestResultReleaseIdempotentAndBounded(t *testing.T) {
	var nilPage *PageResult
	var nilAgg *AggregateResult
	nilPage.Release()
	nilAgg.Release()

	pRes := &PageResult{
		Rows:   make([]PageRow, 8),
		tagBuf: make([]string, 16),
	}
	pRes.Release()
	pRes.Release()
	if pRes.Rows != nil || pRes.tagBuf != nil {
		t.Fatalf("PageResult fields not cleared: %+v", pRes)
	}

	aRes := &AggregateResult{
		Segments: make([]SegmentSummary, 4),
		Groups:   [][]GroupRow{make([]GroupRow, 8)},
	}
	aRes.Release()
	aRes.Release()
	if aRes.Segments != nil || aRes.Groups != nil {
		t.Fatalf("AggregateResult fields not cleared: %+v", aRes)
	}

	reclaimPools()
	bigScratch := make([]byte, 0, maxZstdScratchCap+1024)
	putZstdScratch(&bigScratch, bigScratch)
	if v := zstdDecodeScratchPool.Get(); v != nil {
		t.Fatalf("oversized zstd scratch buffer was pooled")
	}
	bigRead := make([]byte, 0, maxReadBufCap+1024)
	putReadBuf(&bigRead)
	if v := readBufPool.Get(); v != nil {
		t.Fatalf("oversized read buffer was pooled")
	}
}

func TestWindowGuardrails4Levels(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	c, err := OpenClient(filepath.Join(t.TempDir(), "window.db"), ClientOptions{
		DB:            Options{Now: func() time.Time { return now }},
		DefaultWindow: 2 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	s, err := c.RegisterSeries(1, SeriesConfig{Arity: 2, SummaryOps: [4]MetricOp{OpSum}, RowOps: [4]MetricOp{OpSum}})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		ts := now.Add(-time.Duration(i+1) * time.Hour)
		if err := s.Append(ts, Summary{Values: [4]int64{10}}, Row{Tags: []string{"h", "tcp"}, Values: [4]int64{10}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Read(ctx, ReadRequest{}); !errors.Is(err, ErrWindowRequired) {
		t.Fatalf("Series.Read zero window: got %v", err)
	}
	if err := s.Scan(ctx, ScanQuery{}, func(ScanBatch) error { return nil }); !errors.Is(err, ErrWindowRequired) {
		t.Fatalf("Series.Scan zero window: got %v", err)
	}
	if _, err := s.Aggregate(ctx, AggregateQuery{}); !errors.Is(err, ErrWindowRequired) {
		t.Fatalf("Series.Aggregate zero window: got %v", err)
	}
	for _, q := range []AggregateQuery{
		{AllTime: true, From: now.Add(-time.Hour)},
		{AllTime: true, Window: time.Hour},
		{From: now, To: now.Add(-time.Hour)},
		{Window: -time.Minute},
		{From: now.Add(-time.Hour), Window: time.Hour},
	} {
		if _, err := s.Aggregate(ctx, q); !errors.Is(err, ErrInvalidWindow) {
			t.Fatalf("expected ErrInvalidWindow for %+v, got %v", q, err)
		}
	}
	if _, err := s.Aggregate(ctx, AggregateQuery{To: now}); !errors.Is(err, ErrWindowRequired) {
		t.Fatalf("To-only without Window: got %v", err)
	}

	res, err := s.Aggregate(ctx, AggregateQuery{To: now, Window: 3 * time.Hour})
	if err != nil || res.Summary.Values[0] != 30 {
		t.Fatalf("To+Window got=%+v err=%v", res, err)
	}
	res.Release()
	res, err = s.Aggregate(ctx, AggregateQuery{AllTime: true})
	if err != nil || res.Summary.Values[0] != 50 {
		t.Fatalf("AllTime got=%+v err=%v", res, err)
	}
	res.Release()
	res, err = c.Aggregate(ctx, 1, AggregateQuery{})
	if err != nil || res.Summary.Values[0] != 20 {
		t.Fatalf("Client DefaultWindow got=%+v err=%v", res, err)
	}
	res.Release()
}

func TestMemoryProfileBudgetAndCtxCancel(t *testing.T) {
	pBal := compileMemPolicy(Options{MemoryProfile: MemoryBalanced})
	pCon := compileMemPolicy(Options{MemoryProfile: MemoryConstrained})
	if pBal.queryBytes != 8<<20 || pCon.queryBytes != 2<<20 || pCon.queryWorkers != 1 || pCon.compactWorkers != 1 || pCon.queryBatchRows != 512 || pCon.zstdEncoderWindow != 64<<10 {
		t.Fatalf("unexpected compiled policies: bal=%+v con=%+v", pBal, pCon)
	}
	pCustom := compileMemPolicy(Options{
		MemoryBudget: MemoryBudget{QueryBytes: 100, CompactBytes: 100, QueryWorkers: 99, QueryBatchRows: 10},
	})
	if pCustom.queryBytes != 1<<20 || pCustom.compactBytes != 2<<20 || pCustom.queryWorkers != 2 || pCustom.queryBatchRows != 64 {
		t.Fatalf("budget clamping failed: %+v", pCustom)
	}

	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	db, err := Open(filepath.Join(t.TempDir(), "mem.db"), Options{MemoryProfile: MemoryConstrained, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, _ := db.Series(1, SeriesConfig{Arity: 1, SummaryOps: [4]MetricOp{OpSum}, RowOps: [4]MetricOp{OpSum}})
	if err := s.Append(now.Add(-time.Hour), Summary{Values: [4]int64{1}}, Row{Tags: []string{"x"}, Values: [4]int64{1}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	// Saturate querySem (cap=1 under MemoryConstrained): SummaryOnly bypasses querySem (while honoring ctx.Err),
	// whereas grouped Aggregate acquires querySem and returns DeadlineExceeded.
	db.querySem <- struct{}{}
	defer func() { <-db.querySem }()
	if res, err := s.Aggregate(context.Background(), AggregateQuery{AllTime: true}); err != nil {
		t.Fatalf("SummaryOnly should bypass saturated semaphore, got %v", err)
	} else {
		res.Release()
	}
	canceledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if _, err := s.Aggregate(canceledCtx, AggregateQuery{AllTime: true}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected Canceled on canceled SummaryOnly ctx, got %v", err)
	}
	cancelCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := s.Aggregate(cancelCtx, AggregateQuery{AllTime: true, GroupDims: []int{0}}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded on saturated semaphore, got %v", err)
	}
}
