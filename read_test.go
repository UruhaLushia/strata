package strata

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestReadPaginationCursorZeroDiskSkipAndCompactShift(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	clock := now
	path := filepath.Join(t.TempDir(), "read.db")
	c, err := OpenClient(path, ClientOptions{
		DB: Options{
			Now:         func() time.Time { return clock },
			Retention1m: 14 * 24 * time.Hour,
			Retention1h: 90 * 24 * time.Hour,
			Retention1d: 365 * 24 * time.Hour,
		},
		DefaultPageLimit: 10,
		MaxPageLimit:     50,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cfg := SeriesConfig{Arity: 2, SummaryOps: [4]MetricOp{OpSum}, RowOps: [4]MetricOp{OpSum}}
	s, err := c.RegisterSeries(1, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Write 4 minutes (10:00..10:03), 7 rows each = 28 rows total.
	base := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	for m := range 4 {
		rows := make([]Row, 7)
		for r := range 7 {
			netTag := "tcp"
			if r%2 == 1 {
				netTag = "udp"
			}
			rows[r] = Row{Tags: []string{"m" + strconv.Itoa(m) + "-r" + strconv.Itoa(r), netTag}, Values: [4]int64{int64(m*10 + r + 1)}}
		}
		if err := s.Append(base.Add(time.Duration(m)*time.Minute), Summary{Values: [4]int64{1}}, rows...); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Compact(); err != nil {
		t.Fatal(err)
	}

	// Limit validation.
	if _, err := c.Read(ctx, 1, ReadRequest{AllTime: true, Limit: -1}); !errors.Is(err, ErrPageLimitExceeded) {
		t.Fatalf("Limit=-1 got %v", err)
	}
	if _, err := c.Read(ctx, 1, ReadRequest{AllTime: true, Limit: 51}); !errors.Is(err, ErrPageLimitExceeded) {
		t.Fatalf("Limit=51 got %v", err)
	}

	// Exact multiple traversal: 28 rows / Limit 7 = 4 pages, last page HasMore == false.
	var cur Cursor
	var allTags []string
	for p := range 10 {
		page, err := c.Read(ctx, 1, ReadRequest{From: base, To: base.Add(4 * time.Minute), Limit: 7, Cursor: cur, PreferFine: true})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range page.Rows {
			allTags = append(allTags, r.Tags[0])
		}
		hasMore := page.HasMore
		nextRaw := page.NextCursor.String()
		page.Release()
		if !hasMore {
			if p != 3 {
				t.Fatalf("expected 4 pages, stopped at page %d", p+1)
			}
			if nextRaw != "" {
				t.Fatalf("expected empty NextCursor when !HasMore, got %q", nextRaw)
			}
			break
		}
		parsed, err := ParseCursor(nextRaw)
		if err != nil {
			t.Fatalf("ParseCursor failed: %v", err)
		}
		cur = parsed
	}
	if len(allTags) != 28 {
		t.Fatalf("expected 28 paginated rows, got %d", len(allTags))
	}

	// Filtered Offset skip & +1 probe exactness: 4 tcp rows per block = 16 total.
	filtPage, err := c.Read(ctx, 1, ReadRequest{
		From: base, To: base.Add(4 * time.Minute), PreferFine: true,
		Filters: []Filter{{Dim: 1, Op: TagEq, Values: []string{"tcp"}}},
		Offset:  12, Limit: 4,
	})
	if err != nil || len(filtPage.Rows) != 4 || filtPage.HasMore {
		t.Fatalf("filtered offset+limit exact boundary failed: err=%v len=%d hasMore=%v", err, len(filtPage.Rows), filtPage.HasMore)
	}
	filtPage.Release()

	// Cursor tampering & filter mismatch checks.
	if _, err := ParseCursor("invalid-cursor"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("bad cursor got %v", err)
	}
	firstPage, err := c.Read(ctx, 1, ReadRequest{From: base, To: base.Add(4 * time.Minute), Limit: 5, PreferFine: true})
	if err != nil || !firstPage.HasMore {
		t.Fatalf("first page failed: err=%v hasMore=%v", err, firstPage.HasMore)
	}
	savedCur := firstPage.NextCursor
	firstPage.Release()
	if _, err := c.Read(ctx, 1, ReadRequest{From: base, To: base.Add(4 * time.Minute), Limit: 5, Offset: 1, Cursor: savedCur, PreferFine: true}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("Offset+Cursor should fail with ErrInvalidCursor, got %v", err)
	}
	if _, err := c.Read(ctx, 1, ReadRequest{From: base, To: base.Add(4 * time.Minute), Limit: 5, Filters: []Filter{{Dim: 1, Op: TagEq, Values: []string{"udp"}}}, Cursor: savedCur, PreferFine: true}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("filter mismatch should fail with ErrInvalidCursor, got %v", err)
	}

	// Unfiltered O(1) zero-disk-read Offset skip: add 3 live rows at 10:04, skip all 28 sealed rows with view detached!
	if err := s.Append(base.Add(4*time.Minute), Summary{Values: [4]int64{9}},
		Row{Tags: []string{"live-0", "tcp"}, Values: [4]int64{9}},
		Row{Tags: []string{"live-1", "tcp"}, Values: [4]int64{9}},
		Row{Tags: []string{"live-2", "tcp"}, Values: [4]int64{9}},
	); err != nil {
		t.Fatal(err)
	}
	savedView := c.DB().view
	c.DB().view = nil
	livePage, err := c.Read(ctx, 1, ReadRequest{From: base, To: base.Add(5 * time.Minute), Offset: 28, Limit: 5, PreferFine: true})
	c.DB().view = savedView
	if err != nil || len(livePage.Rows) != 3 || livePage.HasMore || livePage.Rows[0].Tags[0] != "live-0" {
		t.Fatalf("zero-disk-read offset skip failed: err=%v page=%+v", err, livePage)
	}
	livePage.Release()

	// Cross-Compact ResolutionShifted: day-aligned start (00:00) tests same-start shift
	// (1m -> 1d/1h at 00:00) and mid-day cursor (00:02) tests skipping to next day's block.
	dayBase := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	c2, err := OpenClient(filepath.Join(t.TempDir(), "shift.db"), ClientOptions{
		DB: Options{
			Now:         func() time.Time { return clock },
			Retention1m: 14 * 24 * time.Hour,
			Retention1h: 90 * 24 * time.Hour,
			Retention1d: 365 * 24 * time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	s2, err := c2.RegisterSeries(1, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for m := range 4 {
		rows := make([]Row, 7)
		for r := range 7 {
			rows[r] = Row{Tags: []string{"s" + strconv.Itoa(m) + "-r" + strconv.Itoa(r), "tcp"}, Values: [4]int64{int64(m*10 + r + 1)}}
		}
		if err := s2.Append(dayBase.Add(time.Duration(m)*time.Minute), Summary{Values: [4]int64{1}}, rows...); err != nil {
			t.Fatal(err)
		}
	}
	if err := s2.Append(dayBase.Add(24*time.Hour), Summary{Values: [4]int64{50}}, Row{Tags: []string{"nextday-0", "tcp"}, Values: [4]int64{50}}); err != nil {
		t.Fatal(err)
	}
	pSame, err := c2.Read(ctx, 1, ReadRequest{From: dayBase, To: dayBase.Add(25 * time.Hour), Limit: 5})
	if err != nil || !pSame.HasMore {
		t.Fatalf("pSame failed: err=%v", err)
	}
	sameStartCur := pSame.NextCursor
	pSame.Release()
	pMid, err := c2.Read(ctx, 1, ReadRequest{From: dayBase, To: dayBase.Add(25 * time.Hour), Offset: 14, Limit: 3})
	if err != nil || !pMid.HasMore {
		t.Fatalf("pMid failed: err=%v", err)
	}
	midCur := pMid.NextCursor
	pMid.Release()

	clock = dayBase.Add(20 * 24 * time.Hour)
	if err := c2.Compact(); err != nil {
		t.Fatal(err)
	}
	shiftedSameStart, err := c2.Read(ctx, 1, ReadRequest{From: dayBase, To: dayBase.Add(25 * time.Hour), Limit: 5, Cursor: sameStartCur})
	if err != nil || !shiftedSameStart.ResolutionShifted || len(shiftedSameStart.Rows) == 0 {
		t.Fatalf("same-start shift failed: err=%v page=%+v", err, shiftedSameStart)
	}
	shiftedSameStart.Release()

	shiftedMid, err := c2.Read(ctx, 1, ReadRequest{From: dayBase, To: dayBase.Add(25 * time.Hour), Limit: 5, Cursor: midCur})
	if err != nil || !shiftedMid.ResolutionShifted || len(shiftedMid.Rows) != 1 || shiftedMid.Rows[0].Tags[0] != "nextday-0" {
		t.Fatalf("mid-day shift failed: err=%v page=%+v", err, shiftedMid)
	}
	shiftedMid.Release()
}

func TestLightReadLaneIsolationAndFallback(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	ops := [4]MetricOp{OpSum, OpMin, OpMax, OpLast}

	db, err := Open(filepath.Join(t.TempDir(), "light-read.db"), Options{
		Now:           func() time.Time { return now },
		MemoryProfile: MemoryConstrained,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	client := NewClient(db, ClientOptions{})
	defer client.Close()

	s, err := db.Series(1, SeriesConfig{Arity: 2, SummaryOps: ops, RowOps: ops})
	if err != nil {
		t.Fatal(err)
	}
	appendRows := func(ts time.Time, n int, prefix string) {
		rows := make([]Row, n)
		for i := range rows {
			rows[i] = Row{
				Tags:   []string{fmt.Sprintf("%s-g-%02d", prefix, i%10), fmt.Sprintf("%s-k-%04d", prefix, i)},
				Values: [4]int64{int64(i + 1), 1, int64(i + 5), int64(i + 10)},
				Mask:   0x0f,
			}
		}
		if err := s.Append(ts, Summary{Values: [4]int64{int64(n), 1, int64(n + 5), int64(n + 10)}, Mask: 0x0f}, rows...); err != nil {
			t.Fatal(err)
		}
	}

	// Recent light-eligible blocks, an oversized (>4096 rows) block, and 70 minute blocks (>64 plans).
	appendRows(now.Add(-10*time.Minute), 80, "sm1")
	appendRows(now.Add(-5*time.Minute), 80, "sm2")
	appendRows(now.Add(-30*time.Minute), 4500, "big")
	for m := range 70 {
		appendRows(now.Add(-time.Duration(180-m)*time.Minute), 2, "many")
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	// ParseCursor must reject rowSkip > math.MaxInt32 (MF-2).
	badSkipCur := Cursor{
		winFromNs: now.Add(-time.Hour).UnixNano(),
		winToNs:   now.UnixNano(),
		queryHash: cursorQueryHash(1, nil, false, false),
		slotStart: now.Add(-10 * time.Minute).Unix(),
		rowSkip:   uint32(math.MaxInt32) + 1,
		slotRes:   resMinuteHR,
		valid:     true,
	}
	if _, err := ParseCursor(badSkipCur.String()); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("ParseCursor should reject rowSkip > MaxInt32, got %v", err)
	}

	// Saturate querySem (cap=1 under MemoryConstrained).
	db.querySem <- struct{}{}
	querySemHeld := true
	releaseQuerySem := func() {
		if querySemHeld {
			<-db.querySem
			querySemHeld = false
		}
	}
	defer releaseQuerySem()

	// Eligible small-page Read & bounded TagEq Read succeed via lightReadSem while querySem is saturated.
	p, err := s.Read(ctx, ReadRequest{From: now.Add(-15 * time.Minute), To: now, Limit: 200})
	if err != nil || len(p.Rows) != 160 {
		t.Fatalf("eligible light Read failed: len=%d err=%v", len(p.Rows), err)
	}
	p.Release()
	pEq, err := s.Read(ctx, ReadRequest{
		From: now.Add(-15 * time.Minute), To: now, Limit: 50,
		Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"sm1-g-00"}}},
	})
	if err != nil || len(pEq.Rows) != 8 {
		t.Fatalf("TagEq light Read failed: len=%d err=%v", len(pEq.Rows), err)
	}
	pEq.Release()

	// Oversized TagEq values (>16), non-TagEq ops (MF-1), overflow/large-rowSkip cursors (MF-2),
	// large limit/offset/window, >4096-row block, and >64 plans MUST fall back to querySem.
	tooManyVals := make([]string, lightReadMaxFilterValues+1)
	for i := range tooManyVals {
		tooManyVals[i] = fmt.Sprintf("sm1-g-%02d", i)
	}
	overflowCur := Cursor{
		winFromNs: math.MinInt64,
		winToNs:   math.MaxInt64,
		queryHash: cursorQueryHash(1, nil, false, false),
		slotStart: now.Add(-10 * time.Minute).Unix(),
		slotRes:   resMinuteHR,
		valid:     true,
	}
	mustFallback := []struct {
		name string
		req  ReadRequest
	}{
		{"limit-500", ReadRequest{From: now.Add(-15 * time.Minute), To: now, Limit: 500}},
		{"offset-500", ReadRequest{From: now.Add(-15 * time.Minute), To: now, Offset: 500, Limit: 50}},
		{"all-time", ReadRequest{AllTime: true, Limit: 50}},
		{"window-90d", ReadRequest{Window: 90 * 24 * time.Hour, Limit: 50}},
		{"mf1-tag-contains", ReadRequest{From: now.Add(-15 * time.Minute), To: now, Limit: 50, Filters: []Filter{{Dim: 0, Op: TagContains, Values: []string{"sm1"}}}}},
		{"mf1-too-many-eq-values", ReadRequest{From: now.Add(-15 * time.Minute), To: now, Limit: 50, Filters: []Filter{{Dim: 0, Op: TagEq, Values: tooManyVals}}}},
		{"mf2-overflow-cursor-span", ReadRequest{Cursor: overflowCur, Limit: 50}},
		{"mf2-large-rowskip-cursor", ReadRequest{Cursor: badSkipCur, Limit: 50}},
		{"stage2-block-over-4096", ReadRequest{From: now.Add(-35 * time.Minute), To: now.Add(-25 * time.Minute), Limit: 50}},
		{"stage2-over-64-plans", ReadRequest{From: now.Add(-185 * time.Minute), To: now.Add(-105 * time.Minute), Limit: 50, PreferFine: true}},
	}
	for _, tc := range mustFallback {
		tCtx, cancel := context.WithTimeout(ctx, 15*time.Millisecond)
		_, rErr := s.Read(tCtx, tc.req)
		cancel()
		if !errors.Is(rErr, context.DeadlineExceeded) {
			t.Fatalf("%s: expected DeadlineExceeded on saturated querySem, got %v", tc.name, rErr)
		}
		if len(db.lightReadSem) != 0 {
			t.Fatalf("%s: lightReadSem leaked: len=%d", tc.name, len(db.lightReadSem))
		}
	}

	// Stage-2 fallback succeeds seamlessly once querySem is free.
	releaseQuerySem()
	pBig, err := s.Read(ctx, ReadRequest{From: now.Add(-35 * time.Minute), To: now.Add(-25 * time.Minute), Limit: 50})
	if err != nil || len(pBig.Rows) != 50 {
		t.Fatalf("Stage-2 fallback Read failed: len=%d err=%v", len(pBig.Rows), err)
	}
	pBig.Release()

	// 1-slot lightReadSem saturation honors ctx.Done().
	db.lightReadSem <- struct{}{}
	tCtx, cancel := context.WithTimeout(ctx, 15*time.Millisecond)
	_, lErr := s.Read(tCtx, ReadRequest{From: now.Add(-15 * time.Minute), To: now, Limit: 200})
	cancel()
	<-db.lightReadSem
	if !errors.Is(lErr, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded on saturated lightReadSem, got %v", lErr)
	}

	client.Reclaim()
	if len(db.lightReadSlots) != 0 || len(db.lightReadSem) != 0 || len(db.querySem) != 0 {
		t.Fatalf("resource leak: lightSlots=%d lightSem=%d querySem=%d", len(db.lightReadSlots), len(db.lightReadSem), len(db.querySem))
	}
}
