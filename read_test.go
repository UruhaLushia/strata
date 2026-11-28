package strata

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"
)

func scanSelectionFixture(t *testing.T) (*Series, []Row, int) {
	t.Helper()
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{})
	t.Cleanup(func() { _ = db.Close() })
	var selected []Row
	sparse := 0
	for block := range 2 {
		rows := make([]Row, 320)
		for i := range rows {
			keep := block == 0 && i%47 == 5 || block == 1 && i%5 != 0
			tag := "drop"
			if keep {
				tag = "keep"
			}
			mask := uint8(1 + i%15)
			vals := testValues(int64(block*1000 + i))
			rows[i] = Row{Tags: []string{tag, fmt.Sprintf("%d/%d", block, i)}, Values: vals, Mask: mask}
			if keep {
				want := rows[i]
				for m := range 4 {
					if mask&(1<<m) == 0 {
						want.Values[m] = 0
					}
				}
				selected = append(selected, want)
				if block == 0 {
					sparse++
				}
			}
		}
		requireOK(t, s.Append(now.Add(time.Duration(block-60)*time.Minute), Summary{}, rows...))
	}
	requireOK(t, db.Compact())
	return s, selected, sparse
}

func TestReadPaginationAcrossSparseAndDenseSelections(t *testing.T) {
	s, want, sparse := scanSelectionFixture(t)
	q := ReadRequest{AllTime: true, Filters: tagFilter(0, TagEq, "keep"), Offset: sparse - 2, Limit: 64, RequireExact: true}
	for pos := q.Offset; pos < len(want); {
		page, err := s.Read(t.Context(), q)
		requireOK(t, err)
		n := min(q.Limit, len(want)-pos)
		more := pos+n < len(want)
		if len(page.Rows) != n || !page.Exact || page.HasMore != more || page.NextCursor.IsZero() == more {
			page.Release()
			t.Fatalf("wrong page length, exactness or continuation at offset %d", pos)
		}
		for i, got := range page.Rows {
			row := want[pos+i]
			if !slices.Equal(got.Tags, row.Tags) || got.Values != row.Values || got.Mask != row.Mask {
				t.Fatalf("row %d = %+v, want %+v", pos+i, got, row)
			}
		}
		q.Cursor, q.Offset = page.NextCursor, 0
		page.Release()
		pos += n
	}
}

func TestReadIndexedSelectionDoesNotReplaceScratch(t *testing.T) {
	block := sealedBlock{rows: []idRow{{tags: [8]uint32{1}}, {tags: [8]uint32{2}}}}
	var err error
	block.encoded, err = encodeBlockWithEncoder(block, nil)
	requireOK(t, err)
	scratch := []int32{-1}
	initial := &scratch[0]
	page := PageResult{Rows: make([]PageRow, 0, 1), tagBuf: make([]string, 0, 1)}
	filters := []compiledFilter{{ids: []uint32{1}, single: 1}}
	_, taken, _, err := readPageFromBlock(block, SegmentMeta{}, dictionary{}, 0, 1, filters, false, 0, 1, &querySlot{}, &scratch, &page)
	requireOK(t, err)
	if taken != 1 || len(scratch) != 1 || &scratch[0] != initial || scratch[0] != -1 {
		t.Fatalf("indexed read took %d rows and changed scratch to %v", taken, scratch)
	}
}

func TestReadPaginationCursorZeroDiskSkipAndCompactShift(t *testing.T) {
	clock := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	ctx := t.Context()
	seed := func(s *Series, base time.Time) []string {
		t.Helper()
		var tags []string
		for m := range 4 {
			rows := make([]Row, 7)
			for i := range rows {
				tag, net := fmt.Sprintf("m%d-r%d", m, i), "tcp"
				if i%2 != 0 {
					net = "udp"
				}
				tags = append(tags, tag)
				rows[i] = Row{Tags: []string{tag, net}, Values: [4]int64{1}}
			}
			requireOK(t, s.Append(base.Add(time.Duration(m)*time.Minute), Summary{Values: [4]int64{7}}, rows...))
		}
		return tags
	}

	db, s := testDB(t, &clock, Options{})
	defer db.Close()
	c := NewClient(db, ClientOptions{MaxPageLimit: 50})
	defer c.Close()
	read := func(q ReadRequest) *PageResult {
		t.Helper()
		page, err := c.Read(ctx, s.id, q)
		requireOK(t, err)
		return page
	}
	base := clock.Add(-2 * time.Hour)
	want := seed(s, base)
	requireOK(t, db.Compact())
	for _, limit := range []int{-1, 51} {
		if _, err := c.Read(ctx, s.id, ReadRequest{AllTime: true, Limit: limit}); !errors.Is(err, ErrPageLimitExceeded) {
			t.Fatalf("limit=%d err=%v", limit, err)
		}
	}

	req := ReadRequest{From: base, To: base.Add(4 * time.Minute), Limit: 7, PreferFine: true}
	var got []string
	var first Cursor
	for i := range 4 {
		page := read(req)
		if page.HasMore != (i < 3) || page.NextCursor.IsZero() != (i == 3) {
			t.Fatalf("page %d hasMore=%v cursor=%q", i, page.HasMore, page.NextCursor)
		}
		for _, row := range page.Rows {
			got = append(got, row.Tags[0])
		}
		cursor, err := ParseCursor(page.NextCursor.String())
		requireOK(t, err)
		page.Release()
		req.Cursor = cursor
		if i == 0 {
			first = cursor
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("paginated tags=%v want=%v", got, want)
	}

	filtered := req
	filtered.Offset, filtered.Limit = 12, 4
	filtered.Filters = []Filter{{Dim: 1, Op: TagEq, Values: []string{"tcp"}}}
	page := read(filtered)
	if len(page.Rows) != 4 || page.HasMore || page.Rows[0].Tags[0] != "m3-r0" {
		t.Fatalf("filtered offset boundary page=%+v", page)
	}
	page.Release()

	if _, err := ParseCursor("invalid-cursor"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("bad cursor err=%v", err)
	}
	for _, q := range []ReadRequest{
		{Cursor: first, Offset: 1, PreferFine: true},
		{Cursor: first, Filters: filtered.Filters, PreferFine: true},
	} {
		if _, err := c.Read(ctx, s.id, q); !errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("incompatible cursor request %+v err=%v", q, err)
		}
	}

	appendValue(t, s, req.To, 9)
	savedView := db.view
	db.view = nil
	page, err := c.Read(ctx, s.id, ReadRequest{From: base, To: req.To.Add(time.Minute), Offset: len(want), Limit: 5, PreferFine: true})
	db.view = savedView
	requireOK(t, err)
	if len(page.Rows) != 1 || page.HasMore || page.Rows[0].Values[0] != 9 {
		t.Fatalf("zero-disk offset skip page=%+v", page)
	}
	page.Release()

	day := clock.Truncate(24 * time.Hour)
	shiftDB, shift := testDB(t, &clock, Options{})
	defer shiftDB.Close()
	seed(shift, day)
	requireOK(t, shift.Append(day.Add(24*time.Hour), Summary{Values: [4]int64{50}}, Row{Tags: []string{"nextday", "tcp"}, Values: [4]int64{50}}))
	readShift := func(req ReadRequest) *PageResult {
		t.Helper()
		p, err := shift.Read(ctx, req)
		requireOK(t, err)
		return p
	}
	window := ReadRequest{From: day, To: day.Add(25 * time.Hour), Limit: 5}
	same := readShift(window)
	midReq := window
	midReq.Offset, midReq.Limit = 14, 3
	mid := readShift(midReq)
	sameStart, midDay := same.NextCursor, mid.NextCursor
	same.Release()
	mid.Release()
	if sameStart.IsZero() || midDay.IsZero() || sameStart.slotStart != day.Unix() || midDay.slotStart <= day.Unix() {
		t.Fatalf("unexpected initial cursor positions: same=%+v mid=%+v", sameStart, midDay)
	}

	clock = day.Add(20 * 24 * time.Hour)
	requireOK(t, shiftDB.Compact())
	for _, cursor := range []Cursor{sameStart, midDay} {
		window.Cursor = cursor
		p := readShift(window)
		if !p.ResolutionShifted || len(p.Rows) == 0 || cursor == midDay && (len(p.Rows) != 1 || p.Rows[0].Tags[0] != "nextday") {
			t.Fatalf("compacted cursor %+v page=%+v", cursor, p)
		}
		p.Release()
	}
}

func TestLightReadLaneIsolationAndFallback(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{MemoryProfile: MemoryConstrained})
	defer db.Close()
	client := NewClient(db, ClientOptions{})
	defer client.Close()
	seed := func(minutesAgo, n int) {
		t.Helper()
		seedHighCardMinute(t, s, now.Add(-time.Duration(minutesAgo)*time.Minute), n, func(i int) []string {
			return []string{fmt.Sprintf("g-%02d", i%10), fmt.Sprint(i)}
		})
	}
	seed(10, 80)
	seed(5, 80)
	seed(30, 4500)
	for i := range 70 {
		seed(180-i, 2)
	}
	requireOK(t, db.Compact())

	recent := ReadRequest{From: now.Add(-15 * time.Minute), To: now, Limit: 200}
	big := ReadRequest{From: now.Add(-35 * time.Minute), To: now.Add(-25 * time.Minute), Limit: 50}
	plans := ReadRequest{From: now.Add(-185 * time.Minute), To: now.Add(-105 * time.Minute), Limit: 50, PreferFine: true}
	overflow := Cursor{
		winFromSec: math.MinInt64, winToSec: math.MaxInt64,
		queryHash: cursorQueryHash(s.id, nil, false, false),
		slotStart: now.Add(-10 * time.Minute).Unix(), slotRes: resMinuteHR, valid: true,
	}
	badSkip := overflow
	badSkip.winFromSec, badSkip.winToSec, badSkip.rowSkip = recent.From.Unix(), recent.To.Unix(), uint32(math.MaxInt32)+1
	if _, err := ParseCursor(badSkip.String()); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("ParseCursor accepted rowSkip > MaxInt32: %v", err)
	}

	checkRows := func(req ReadRequest, want int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		p, err := s.Read(ctx, req)
		requireOK(t, err)
		defer p.Release()
		if len(p.Rows) != want {
			t.Fatalf("Read rows=%d want=%d", len(p.Rows), want)
		}
	}
	func() {
		db.querySem <- struct{}{}
		defer func() { <-db.querySem }()
		checkRows(recent, 160)
		eq := recent
		eq.Filters = []Filter{{Dim: 0, Op: TagEq, Values: []string{"g-00"}}}
		checkRows(eq, 16)

		values := make([]string, lightReadMaxFilterValues+1)
		for i := range values {
			values[i] = fmt.Sprintf("g-%02d", i)
		}
		for _, q := range []ReadRequest{
			{Limit: 500},
			{Offset: 500},
			{AllTime: true},
			{Window: 90 * 24 * time.Hour},
			{Filters: []Filter{{Dim: 0, Op: TagContains, Values: []string{"g-"}}}},
			{Filters: []Filter{{Dim: 0, Op: TagEq, Values: values}}},
			{Cursor: overflow}, {Cursor: badSkip},
			big, plans,
		} {
			if q.From.IsZero() && q.Cursor.IsZero() && !q.AllTime && q.Window == 0 {
				q.From, q.To = recent.From, recent.To
			}
			if q.Limit == 0 {
				q.Limit = 50
			}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Millisecond)
			_, err := s.Read(ctx, q)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) || len(db.lightReadSem) != 0 {
				t.Fatalf("fallback request %+v err=%v light semaphore=%d", q, err, len(db.lightReadSem))
			}
		}
	}()
	checkRows(big, 50)

	db.lightReadSem <- struct{}{}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Millisecond)
	defer cancel()
	_, err := s.Read(ctx, recent)
	<-db.lightReadSem
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("saturated light semaphore err=%v", err)
	}
	client.Reclaim()
	if len(db.lightReadSlots) != 0 || len(db.lightReadSem) != 0 || len(db.querySem) != 0 {
		t.Fatal("Read or Reclaim retained a slot or semaphore token")
	}
}
