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

func TestColumnarSingleEqRangeSelection(t *testing.T) {
	const lo, hi = 11, 311
	for _, colType := range []struct {
		mode   byte
		width  int
		target uint32
	}{
		{modeU8, 1, 0x81}, {modeU16, 2, 0x8123}, {modeU32, 4, 0x81234567},
	} {
		for _, tc := range []struct {
			stride int
			hi     int
			mode   selMode
		}{{0, hi, selRange}, {10, hi, selIdx}, {2, hi, selBits}, {2, lo + 256, selIdx}, {1, hi, selRange}} {
			data := make([]byte, (hi+1)*colType.width)
			var want []int32
			for i := lo; i < tc.hi && tc.stride != 0; i += tc.stride {
				for j := range colType.width {
					data[i*colType.width+j] = byte(colType.target >> (8 * j))
				}
				want = append(want, int32(i))
			}
			col := decodedCol{mode: colType.mode, data: data}
			f := compiledFilter{ids: []uint32{colType.target}, single: colType.target}
			sel := scanRangeAdaptive(col, hi+1, lo, tc.hi, f)
			if sel.mode != tc.mode || sel.count() != len(want) {
				t.Fatalf("width %d, stride %d: mode=%d count=%d", colType.width, tc.stride, sel.mode, sel.count())
			}
			isRange, start, got := sel.sliceRows(0, sel.count(), nil)
			if isRange && len(want) > 0 && start != lo || !isRange && !slices.Equal(got, want) {
				t.Fatalf("width %d, stride %d: rows at %d or %v, want %v", colType.width, tc.stride, start, got, want)
			}
			sel.release()
			if colType.width < 4 && tc.stride == 1 {
				f.single += 1 << (8 * colType.width)
				f.ids[0] = f.single
				miss := scanRangeAdaptive(col, hi+1, lo, hi, f)
				if miss.count() != 0 || miss.idxPool != nil || miss.bitsPool != nil {
					t.Fatalf("width %d accepted an oversized target", colType.width)
				}
			}
		}
	}
}

func TestAppendPageConstantTags(t *testing.T) {
	const series = 1
	d := dictionary{dims: map[dictDimKey]*dictDim{{series: series, dim: 1}: {extra: map[uint32]string{7: "fixed"}}}}
	var cols [streamColumnCount]decodedCol
	cols[0], cols[1] = decodedCol{mode: modeConst}, decodedCol{mode: modeConst, val: 7}
	cols[8] = decodedCol{mode: modeU8, data: []byte{10, 20, 30, 40}}
	for i := 9; i < 12; i++ {
		cols[i] = decodedCol{mode: modeZero}
	}
	cols[12] = decodedCol{mode: modeConst, val: 1}
	page := PageResult{Rows: make([]PageRow, 0, 2), tagBuf: make([]string, 0, 4)}
	mustTestOK(t, appendPageFromCols(&cols, SegmentMeta{}, d, series, 2, false, 0, []int32{1, 3}, 2, &page))
	for i, row := range page.Rows {
		if !slices.Equal(row.Tags, []string{"", "fixed"}) || row.Values[0] != int64(20+i*20) || row.Mask != 1 {
			t.Fatalf("row %d = %+v", i, row)
		}
	}
	view := columnarBlockView{plain: []byte{0xff}, count: 4, codecID: codec.IDZstd}
	view.descs[0] = columnStreamDesc{mode: modeConst}
	view.descs[1] = columnStreamDesc{mode: modeConst, value: 7}
	view.descs[8] = columnStreamDesc{mode: modeU8, width: 1, length: 1, rawLen: 4}
	slot := &querySlot{codecs: testCodecTable(t, zstd.New(zstd.Fastest))}
	defer slot.close()
	page.Rows, page.tagBuf = page.Rows[:0], page.tagBuf[:0]
	if err := appendPageFromStream(view, SegmentMeta{}, d, series, 2, true, 0, nil, 1, slot, &page); err == nil {
		t.Fatal("constant tag materialization swallowed a column codec error")
	}
	d.dims[dictDimKey{series: series, dim: 1}] = &dictDim{
		tailBase: 8, ext: &dictExtDim{baseNextID: 8, fwdPages: []dictPageDesc{{length: 1}}},
	}
	page.Rows, page.tagBuf = page.Rows[:0], page.tagBuf[:0]
	mustTestOK(t, appendPageFromCols(&cols, SegmentMeta{}, d, series, 2, true, 0, nil, 0, &page))
	if err := appendPageFromCols(&cols, SegmentMeta{}, d, series, 2, true, 0, nil, 1, &page); err == nil {
		t.Fatal("constant tag resolution swallowed an unreadable dictionary page")
	}
}

func TestDenseRangeConstantMetrics(t *testing.T) {
	const lo, hi, slotIdx, orderOffset = 4, 133, 2, 17
	var cols [streamColumnCount]decodedCol
	tags, values := make([]byte, hi), make([]byte, hi)
	for i := range tags {
		tags[i], values[i] = byte(i%3), byte(i)
	}
	cols[0] = decodedCol{mode: modeU8, data: tags}
	cols[8] = decodedCol{mode: modeConst, val: 7}
	cols[9] = decodedCol{mode: modeConst, val: ^uint64(4)}
	cols[10] = decodedCol{mode: modeZero}
	cols[11] = decodedCol{mode: modeU8, data: values}
	for _, ops := range [][4]MetricOp{{OpSum, OpSum, OpSum, OpSum}, {OpSum, OpMin, OpMax, OpLast}} {
		newGroups := func() aggregateGroups {
			lastCount, lastIdx := countLastOps(ops)
			g := aggregateGroups{dense: make([]Summary, 2), lastCount: lastCount, lastIdx: lastIdx}
			if lastCount == 1 {
				g.denseLastOrder = make([]uint64, 2)
			}
			g.add(1, Summary{Values: [4]int64{9, 3, 0, 1}, Mask: 0xb}, ops, 1)
			return g
		}
		got, want := newGroups(), newGroups()
		for i := lo; i < hi; i++ {
			order := uint64(slotIdx+1)<<32 | uint64(orderOffset+i-lo+1)
			want.add(uint32(tags[i]), Summary{Values: [4]int64{7, -5, 0, int64(values[i])}, Mask: 0xf}, ops, order)
		}
		if got.lastCount == 0 {
			aggregateRangeDenseAllSum(&cols, lo, hi, 0, &got)
		} else {
			aggregateRangeDenseMixed(&cols, lo, hi, slotIdx, orderOffset, 0, &got, ops)
		}
		if !slices.Equal(got.dense, want.dense) || !slices.Equal(got.denseLastOrder, want.denseLastOrder) {
			t.Fatalf("ops %v: summaries %v/order %v, want %v/%v", ops, got.dense, got.denseLastOrder, want.dense, want.denseLastOrder)
		}
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
