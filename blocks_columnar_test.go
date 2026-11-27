package strata

import (
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/UruhaLushia/strata/codec"
	"github.com/UruhaLushia/strata/codec/zstd"
)

func TestColumnarBlockRoundTripModes(t *testing.T) {
	codecs := testCodecTable(t, zstd.New(zstd.Fastest))
	small := []idRow{
		{tags: [8]uint32{1, 255, 256, 65536, 0, 2, 3, 4}, values: [4]int64{0, -1, 256, 1 << 32}, initMask: 1},
		{tags: [8]uint32{2, 256, 65535, 65537, 0, 4, 5, 6}, values: [4]int64{1, 2, 65536, -2}, initMask: 2},
		{tags: [8]uint32{3, 257, 65536, ^uint32(0), 0, 6, 7, 8}, values: [4]int64{2, 3, 1 << 40, 3}, initMask: 4},
	}
	large := make([]idRow, 512)
	for i := range large {
		large[i] = idRow{
			tags:     [8]uint32{uint32(i), 7, uint32(i * 1000)},
			values:   [4]int64{int64(i), -int64(i), 1 << 40},
			initMask: uint8(i%15 + 1),
		}
	}
	for _, rows := range [][]idRow{small, large} {
		want := sealedBlock{
			series: 7, res: resMinuteHR, start: 10, end: 70, epoch: 3, order: 9,
			summary: Summary{Values: [4]int64{1, -2, 3, 4}, Mask: 0x0f}, rows: rows,
		}
		for level := zstd.Fastest; level <= zstd.Best; level++ {
			raw, err := encodeBlockLevel(want, level)
			mustTestOK(t, err)
			if isStreamBlock(raw) != (len(rows) >= 256) {
				t.Fatalf("unexpected block envelope for %d rows", len(rows))
			}
			got, err := decodeBlock(raw, codec.IDZstd, codecs)
			mustTestOK(t, err)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("round trip changed %d-row block at compression level %d", len(rows), level)
			}
		}
	}
}

func TestAggregateMixedSmallAndStreamBlocks(t *testing.T) {
	now := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{})
	defer db.Close()
	base := now.Add(-2 * time.Minute)
	first, last := [4]int64{3, 9, 2, 7}, [4]int64{4, 1, 8, 2}
	mustTestOK(t, s.Append(base, Summary{Values: first}, Row{Tags: []string{"shared", "x"}, Values: first}))
	rows := make([]Row, 300)
	rows[0] = Row{Tags: []string{"shared", "x"}, Values: last}
	for i := 1; i < len(rows); i++ {
		rows[i] = Row{Tags: []string{"large-" + strconv.Itoa(i), "x"}, Values: [4]int64{1}}
	}
	mustTestOK(t, s.Append(base.Add(time.Minute), Summary{Values: last}, rows...))
	mustTestOK(t, db.Compact())
	res, err := s.Aggregate(t.Context(), AggregateQuery{From: base, To: now, GroupDims: []int{0}, PreferFine: true})
	mustTestOK(t, err)
	defer res.Release()
	want := GroupRow{Tag: "shared", Summary: Summary{Values: [4]int64{7, 1, 8, 2}, Mask: 0x0f}}
	if len(res.Groups) != 1 || len(res.Groups[0]) != len(rows) || !slices.Contains(res.Groups[0], want) {
		t.Fatalf("mixed block groups do not contain %+v: %+v", want, res.Groups)
	}
}

func TestColumnarFilterPushdownAndAdaptiveSelection(t *testing.T) {
	codecs := testCodecTable(t, zstd.New(zstd.Fastest))
	rows := make([]idRow, 600)
	for i := range rows {
		rows[i] = idRow{tags: [8]uint32{uint32(i/2 + 1), uint32(i%10 + 1), 1, uint32(i%2 + 1)}, initMask: 1}
	}
	raw, err := encodeBlockLevel(sealedBlock{rows: rows}, zstd.Fastest)
	mustTestOK(t, err)
	view, err := parseColumnarBlock(raw, codec.IDZstd)
	mustTestOK(t, err)
	slot := &querySlot{codecs: codecs}
	defer slot.close()
	eq := func(dim int, id uint32) compiledFilter {
		return compiledFilter{dim: dim, ids: []uint32{id}, single: id}
	}
	for _, tc := range []struct {
		name    string
		filters []compiledFilter
		mode    selMode
		count   int
	}{
		{"const mismatch", []compiledFilter{eq(2, 2)}, selRange, 0},
		{"out of width", []compiledFilter{eq(1, 999)}, selRange, 0},
		{"missing sorted key", []compiledFilter{eq(0, 65000)}, selRange, 0},
		{"intersect duplicate sorted key", []compiledFilter{eq(2, 1), eq(1, 2), eq(0, 151)}, selIdx, 1},
		{"half match", []compiledFilter{eq(3, 1)}, selBits, 300},
		{"full match", []compiledFilter{{dim: 3, ids: []uint32{1, 2}, small: [8]uint32{1, 2}, smallLen: 2}}, selRange, 600},
	} {
		sel, err := filterStreamView(view, tc.filters, true, slot)
		mustTestOK(t, err)
		if sel.count() != tc.count || sel.mode != tc.mode {
			t.Fatalf("%s selection mode=%d count=%d", tc.name, sel.mode, sel.count())
		}
		if tc.count == 1 {
			isRange, lo, indices := sel.sliceRows(0, 1, nil)
			if isRange && lo != 301 || !isRange && !slices.Equal(indices, []int32{301}) {
				t.Fatalf("intersection selected row %d/%v, want 301", lo, indices)
			}
		}
		sel.release()
	}
}

func TestSoAScanBatchAndColCacheSelectivityFallback(t *testing.T) {
	var view columnarBlockView
	view.count = 10000
	for _, col := range []int{0, 1, 8, 9, 10, 11} {
		view.descs[col] = columnStreamDesc{mode: modeU32, width: 4}
	}
	view.descs[12] = columnStreamDesc{mode: modeConst, value: 0x0f}
	const colMask = 3 | 0x1f<<8
	slot := newQuerySlot()
	defer slot.close()
	for _, count := range []int{2499, 2500} {
		cache := newBlockColCache(view, colMask, 1, 512<<10, slot)
		cache.prepareOutput(colMask, count, false)
		if cache.fullBlock != (count == 2500) || !cache.fullBlock && cache.availPackBytes() < 256*cache.packRowBytes(colMask, false) {
			t.Fatalf("selected=%d fullBlock=%v packBytes=%d", count, cache.fullBlock, cache.availPackBytes())
		}
	}

	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	db, _ := testDB(t, &now, Options{})
	defer db.Close()
	ops := [4]MetricOp{OpSum, OpMin, OpMax, OpLast}
	cfg := SeriesConfig{Arity: 4, SummaryOps: ops, RowOps: ops}
	s := mustSeries(t, db, 1, cfg)
	for minute, count := range []int{4000, 40, 30} {
		rows := make([]Row, count)
		var sum Summary
		for i := range rows {
			row := Row{
				Tags:   []string{fmt.Sprintf("u-%02d", i%10), fmt.Sprintf("h-%03d", i%320), "const", strconv.Itoa(i)},
				Values: [4]int64{int64(i % 200), int64(i*100 + 7), int64(70000 + i*13), -int64(1000000 + i*997)},
				Mask:   0x0f,
			}
			if i%11 == 0 {
				row.Mask = 0x05
			}
			rows[i] = row
			mergeSummary(&sum, Summary{Values: row.Values, Mask: row.Mask}, ops, uint64(i+1), uint64(i))
		}
		mustTestOK(t, s.Append(now.Add(time.Duration(minute-5)*time.Minute), sum, rows...))
		if minute == 1 {
			mustTestOK(t, db.Compact())
		}
	}
	mustTestOK(t, db.Close())

	run := func(opts Options, filters []Filter) (Summary, []GroupRow, []Row) {
		opts.Now, opts.ReadOnly = db.opts.Now, true
		reader := reopenTestDB(t, nil, db.path, opts)
		defer reader.Close()
		s := mustSeries(t, reader, 1, cfg)
		q := AggregateQuery{AllTime: true, Filters: filters, GroupDims: []int{0}}
		agg, err := s.Aggregate(t.Context(), q)
		mustTestOK(t, err)
		defer agg.Release()
		var rows []Row
		mustTestOK(t, s.Scan(t.Context(), ScanQuery{
			AllTime: true, Filters: filters, BatchSize: 128, ProjectDims: 0x05, ProjectMetrics: 0x0a,
		}, func(b ScanBatch) error {
			for i := range b.Len() {
				if b.TagID(i, 1) != 0 || b.TagID(i, 3) != 0 || b.Values(i)[0] != 0 || b.Values(i)[2] != 0 {
					return fmt.Errorf("unprojected field leaked")
				}
				rows = append(rows, b.CloneRow(i))
			}
			return nil
		}))
		return agg.Summary, slices.Clone(agg.Groups[0]), rows
	}
	for _, filters := range [][]Filter{nil, {{Dim: 0, Op: TagEq, Values: []string{"u-03"}}}} {
		want, wantGroups, wantRows := run(Options{MemoryBudget: MemoryBudget{QueryBytes: 16 << 20}}, filters)
		got, gotGroups, gotRows := run(Options{MemoryProfile: MemoryConstrained, MemoryBudget: MemoryBudget{QueryBytes: 1 << 20}}, filters)
		if got != want || !slices.Equal(gotGroups, wantGroups) || len(gotRows) == 0 || !reflect.DeepEqual(gotRows, wantRows) {
			t.Fatalf("cache fallback mismatch for filters %+v", filters)
		}
	}
}

func TestColumnarStreamsRawFallback(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 30, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{Codec: zstd.New(zstd.Fastest)})
	defer func() { _ = db.Close() }()
	ts := now.Add(-5 * time.Minute)
	tags := func(a, b int) []string {
		return []string{"d0-" + strconv.Itoa(100+a), "d1-" + strconv.Itoa(100+b)}
	}
	// Pre-intern all 255 IDs in each dimension so the high-entropy streams use modeU8.
	seedHighCardMinute(t, s, ts.Add(-5*time.Minute), 255, func(i int) []string { return tags(i, i) })
	rows := make([]Row, 523)
	var seen [255][255]bool
	rng := uint64(0x9e3779b97f4a7c15)
	next := func() uint64 {
		rng ^= rng << 13
		rng ^= rng >> 7
		rng ^= rng << 17
		return rng
	}
	for i := range rows {
		a, b := int(next()%255), int(next()%255)
		for seen[a][b] {
			a, b = int(next()%255), int(next()%255)
		}
		seen[a][b] = true
		rows[i] = Row{
			Tags: tags(a, b), Mask: 0x0f,
			Values: [4]int64{int64(1 + i%97 + int(next()%19)), int64(1000 - i), int64(2000 + i), int64(next())},
		}
	}
	mustTestOK(t, s.AppendSync(ts, Summary{}, rows...))
	mustTestOK(t, db.Compact())
	db = reopenTestDB(t, db, db.path, db.opts)
	s = mustSeries(t, db, s.id, s.cfg)
	i := slices.IndexFunc(db.sealed, func(m sealedMeta) bool {
		return m.series == s.id && m.res == resMinuteHR && m.start == ts.Unix()
	})
	if i < 0 {
		t.Fatal("missing compacted STR2 block")
	}
	m := db.sealed[i]
	raw, err := sectionBlock(db.data, db.head.sections[secTSBlocks], m.offset, m.length, nil)
	mustTestOK(t, err)
	view, err := parseColumnarBlock(raw, codec.IDZstd)
	mustTestOK(t, err)
	if !view.unsorted {
		t.Fatal("raw dim 0 lost its unsorted flag")
	}
	slot := newQuerySlot()
	defer slot.close()
	for _, idx := range []int{0, 1, 11} {
		d := view.descs[idx]
		if d.flags&streamFlagRaw == 0 || d.length != d.rawLen {
			t.Fatalf("stream %d did not store raw bytes: %+v", idx, d)
		}
		col, err := decodeStreamColInto(view, idx, slot)
		mustTestOK(t, err)
		if len(col.data) == 0 || &col.data[0] != &view.plain[d.offset] {
			t.Fatalf("stream %d copied its payload", idx)
		}
	}
	decoded, err := decodeBlock(raw, codec.IDZstd, db.codecs)
	mustTestOK(t, err)
	const target = 271
	if len(decoded.rows) != len(rows) || decoded.rows[target].values != rows[target].Values {
		t.Fatal("raw block round trip changed rows or order")
	}

	// The first payload starts immediately after the fixed-width descriptor directory.
	const metric, descSize = 11, 24
	pos := 1 + int(view.descs[0].offset) - (streamColumnCount-metric)*descSize + 20
	bad := slices.Clone(raw)
	binary.LittleEndian.PutUint32(bad[pos:pos+4], view.descs[metric].rawLen-1)
	if _, err := decodeBlock(bad, codec.IDZstd, db.codecs); err == nil || !strings.Contains(err.Error(), "invalid raw column stream length") {
		t.Fatalf("decodeBlock corrupt rawLen err=%v", err)
	}
	if _, err := parseColumnarBlock(bad, codec.IDZstd); err == nil {
		t.Fatal("column parser accepted corrupt rawLen")
	}

	want := rows[target]
	filters := []Filter{{Dim: 0, Op: TagEq, Values: want.Tags[:1]}, {Dim: 1, Op: TagEq, Values: want.Tags[1:]}}
	result, err := s.Aggregate(t.Context(), AggregateQuery{From: ts, To: ts.Add(time.Minute), Filters: filters, PreferFine: true})
	mustTestOK(t, err)
	defer result.Release()
	if result.Summary.Values != want.Values {
		t.Fatalf("unsorted raw lookup=%v want=%v", result.Summary.Values, want.Values)
	}

	// The fixed seed yields exactly two rows with the first tag.
	first, second := rows[0], rows[112]
	wantRows := []Row{first, second}
	q := ReadRequest{From: ts, To: ts.Add(time.Minute), Filters: []Filter{{Dim: 0, Op: TagEq, Values: first.Tags[:1]}}, Limit: 1, PreferFine: true}
	func() {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		for range cap(db.querySem) {
			db.querySem <- struct{}{}
		}
		defer func() {
			for range cap(db.querySem) {
				<-db.querySem
			}
		}()
		for i, want := range wantRows {
			page, err := s.Read(ctx, q)
			mustTestOK(t, err)
			defer page.Release()
			if len(page.Rows) != 1 || page.HasMore != (i == 0) || page.NextCursor.IsZero() != (i == 1) ||
				page.Rows[0].Values != want.Values || !slices.Equal(page.Rows[0].Tags, want.Tags) {
				t.Fatalf("raw filtered Read page %d: rows=%+v more=%v cursor=%q want=%+v", i+1, page.Rows, page.HasMore, page.NextCursor, want)
			}
			q.Cursor = page.NextCursor
		}
	}()
	var scanned []Row
	mustTestOK(t, s.Scan(t.Context(), ScanQuery{From: q.From, To: q.To, Filters: q.Filters, PreferFine: true}, func(b ScanBatch) error {
		for i := range b.Len() {
			scanned = append(scanned, b.CloneRow(i))
		}
		return nil
	}))
	if len(scanned) != len(wantRows) {
		t.Fatalf("raw filtered Scan len=%d want=%d", len(scanned), len(wantRows))
	}
	for i, want := range wantRows {
		if scanned[i].Values != want.Values || !slices.Equal(scanned[i].Tags, want.Tags) {
			t.Fatalf("raw filtered Scan row %d=%+v want=%+v", i, scanned[i], want)
		}
	}
}
