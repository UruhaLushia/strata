package strata

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestColumnarBlockRoundTripModes(t *testing.T) {
	rows := []idRow{
		{tags: [8]uint32{1, 255, 256, 65536, 0, 2, 3, 4}, values: [4]int64{0, -1, 256, 1 << 32}, initMask: 1},
		{tags: [8]uint32{2, 256, 65535, 65537, 0, 4, 5, 6}, values: [4]int64{1, 2, 65536, -2}, initMask: 2},
		{tags: [8]uint32{3, 257, 65536, ^uint32(0), 0, 6, 7, 8}, values: [4]int64{2, 3, 1 << 40, 3}, initMask: 4},
	}
	want := sealedBlock{
		series: 7, res: resMinuteHR, start: 10, end: 70, epoch: 3, order: 9,
		summary: Summary{Values: [4]int64{1, -2, 3, 4}, Mask: 0x0f}, rows: rows,
	}
	raw, err := encodeBlockLevel(want, CompressionFastest)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBlock(raw)
	if err != nil {
		t.Fatalf("%x: %v", raw[:min(8, len(raw))], err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("columnar round trip mismatch:\n got=%+v\nwant=%+v", got, want)
	}
	for level := CompressionFastest; level <= CompressionBest; level++ {
		raw, err := encodeBlockLevel(want, level)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = decodeBlock(raw); err != nil {
			t.Fatalf("level %d decode: %v", level, err)
		}
	}
}

func TestColumnarLargeBlockIndependentStreams(t *testing.T) {
	rows := make([]idRow, 512)
	for i := range rows {
		rows[i].tags[0] = uint32(i)
		rows[i].tags[1] = 7
		rows[i].tags[2] = uint32(i * 1000)
		rows[i].values[0] = int64(i)
		rows[i].values[1] = -int64(i)
		rows[i].values[2] = 1 << 40
		rows[i].initMask = uint8(i%15 + 1)
	}
	want := sealedBlock{series: 3, res: resMinuteHR, start: 1, end: 2, epoch: 4, order: 5, summary: Summary{Mask: 15}, rows: rows}
	raw, err := encodeBlockLevel(want, CompressionBetter)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, blockStreamsMagic[:]) {
		t.Fatalf("large block missing plaintext stream envelope: %x", raw[:min(8, len(raw))])
	}
	got, err := decodeBlock(raw)
	if err != nil {
		t.Fatalf("%x: %v", raw[:min(8, len(raw))], err)
	}
	if !reflect.DeepEqual(got, want) {
		for i := range want.rows {
			if got.rows[i] != want.rows[i] {
				t.Fatalf("large columnar row %d mismatch: got=%+v want=%+v", i, got.rows[i], want.rows[i])
			}
		}
		t.Fatalf("large columnar metadata mismatch: got=%+v want=%+v", got, want)
	}
}

func scanTestEncoded(t *testing.T, raw []byte, series uint16, arity int, d dictionary, filters []compiledFilter) ([]Row, Summary) {
	t.Helper()
	slot := newQuerySlot()
	defer slot.close()
	view, err := parseColumnarBlock(raw)
	if err != nil {
		t.Fatal(err)
	}
	var out []Row
	var sum Summary
	var order uint64
	var idxBuf []int32
	fakeDB := &DB{dict: d, policy: compileMemPolicy(Options{})}
	if err := scanStreamBlock(context.Background(), view, Summary{}, SegmentMeta{}, fakeDB, series, arity, uint8((1<<arity)-1), 0x0f, 128, true, filters, slot, &idxBuf, func(b ScanBatch) error {
		for i := range b.Len() {
			r := b.CloneRow(i)
			out = append(out, r)
			order++
			mergeSummary(&sum, Summary{Values: r.Values, Mask: r.Mask}, [4]MetricOp{OpSum, OpSum, OpSum, OpSum}, order, order-1)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out, sum
}

func TestColumnarDirectMaterializeFilter(t *testing.T) {
	rows := make([]idRow, 512)
	dict := newDict()
	for i := range rows {
		id, _ := dict.intern(3, 0, "tag-"+strconv.Itoa(i))
		rows[i].tags[0] = id
		rows[i].values[0] = int64(i)
		rows[i].initMask = 1
	}
	raw, err := encodeBlockLevel(sealedBlock{rows: rows}, CompressionFastest)
	if err != nil {
		t.Fatal(err)
	}
	id := dict.lookup(3, 0, "tag-7")
	filter := []compiledFilter{{dim: 0, ids: []uint32{id}, single: id}}
	got, _ := scanTestEncoded(t, raw, 3, 1, dictionarySnapshot(dict, 3), filter)
	if len(got) != 1 || got[0].Tags[0] != "tag-7" || got[0].Values[0] != 7 {
		t.Fatalf("unexpected direct scan: %+v", got)
	}
}

func TestAggregateGroupsAllMetricOps(t *testing.T) {
	now := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	db, err := Open(t.TempDir()+"/aggregate.db", Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := db.Series(1, SeriesConfig{
		Arity:      1,
		SummaryOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast},
		RowOps:     [4]MetricOp{OpSum, OpMin, OpMax, OpLast},
	})
	if err != nil {
		t.Fatal(err)
	}
	base := now.Add(-3 * time.Minute)
	rows := []struct {
		at    time.Time
		value [4]int64
	}{
		{base, [4]int64{1, 9, 2, 3}},
		{base.Add(time.Minute), [4]int64{2, 4, 8, 7}},
		{base.Add(2 * time.Minute), [4]int64{3, 1, 4, 9}},
	}
	for _, item := range rows {
		if err := s.Append(item.at, Summary{Values: item.value}, Row{Tags: []string{"group"}, Values: item.value}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := s.Aggregate(context.Background(), AggregateQuery{
		From: base, To: base.Add(3 * time.Minute), GroupDims: []int{0},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if len(result.Groups) != 1 || len(result.Groups[0]) != 1 {
		t.Fatalf("unexpected groups: %+v", result.Groups)
	}
	got := result.Groups[0][0].Summary.Values
	want := [4]int64{6, 1, 8, 9}
	if got != want {
		t.Fatalf("aggregate mismatch: got %v want %v", got, want)
	}
}

func TestAggregateMixedSmallAndStreamBlocks(t *testing.T) {
	now := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	db, err := Open(t.TempDir()+"/mixed.db", Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := db.Series(1, SeriesConfig{Arity: 1, SummaryOps: [4]MetricOp{OpSum}, RowOps: [4]MetricOp{OpSum}})
	if err != nil {
		t.Fatal(err)
	}
	base := now.Add(-2 * time.Minute)
	if err := s.Append(base, Summary{}, Row{Tags: []string{"small"}, Values: [4]int64{3}}); err != nil {
		t.Fatal(err)
	}
	rows := make([]Row, 300)
	for i := range rows {
		rows[i] = Row{Tags: []string{"large-" + strconv.Itoa(i)}, Values: [4]int64{1}}
	}
	if err := s.Append(base.Add(time.Minute), Summary{}, rows...); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	result, err := s.Aggregate(context.Background(), AggregateQuery{From: base, To: base.Add(2 * time.Minute), GroupDims: []int{0}})
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if len(result.Groups) != 1 || len(result.Groups[0]) != 301 {
		t.Fatalf("unexpected mixed groups: %d", len(result.Groups))
	}
}

func TestColumnarFilterPushdownAndAdaptiveSelection(t *testing.T) {
	dict := newDict()
	rows := make([]idRow, 600)
	constTag, _ := dict.intern(5, 2, "const-net")
	otherConst, _ := dict.intern(5, 2, "other-net")
	evenTag, _ := dict.intern(5, 3, "even")
	oddTag, _ := dict.intern(5, 3, "odd")
	for i := range rows {
		d0, _ := dict.intern(5, 0, "dom-"+strconv.Itoa(i/2))
		d1, _ := dict.intern(5, 1, "app-"+strconv.Itoa(i%10))
		d3 := evenTag
		if i%2 == 1 {
			d3 = oddTag
		}
		rows[i] = idRow{tags: [8]uint32{d0, d1, constTag, d3}, values: [4]int64{int64(i), 2, 0, 1}, initMask: 0x0f}
	}
	raw, err := encodeBlockLevel(sealedBlock{rows: rows}, CompressionFastest)
	if err != nil {
		t.Fatal(err)
	}
	snap := dictionarySnapshot(dict, 5)

	// 1. modeConst mismatch early exit.
	got, _ := scanTestEncoded(t, raw, 5, 4, snap, []compiledFilter{{dim: 2, ids: []uint32{otherConst}, single: otherConst}})
	if len(got) != 0 {
		t.Fatalf("expected 0 rows on const mismatch, got %d", len(got))
	}

	// 2. dim 0 binary search with duplicates + dim 1 filter + const match.
	idDom150 := dict.lookup(5, 0, "dom-150")
	idApp1 := dict.lookup(5, 1, "app-1")
	got, sum := scanTestEncoded(t, raw, 5, 4, snap, []compiledFilter{
		{dim: 2, ids: []uint32{constTag}, single: constTag},
		{dim: 1, ids: []uint32{idApp1}, single: idApp1},
		{dim: 0, ids: []uint32{idDom150}, single: idDom150},
	})
	if len(got) != 1 || got[0].Tags[0] != "dom-150" || got[0].Tags[1] != "app-1" || sum.Values[0] != 301 {
		t.Fatalf("unexpected binary search + multi-filter result: rows=%+v sum=%+v", got, sum)
	}

	// 3. Out-of-width single ID on modeU8 dim 1.
	got, _ = scanTestEncoded(t, raw, 5, 4, snap, []compiledFilter{{dim: 1, ids: []uint32{999}, single: 999}})
	if len(got) != 0 {
		t.Fatalf("expected 0 rows for out-of-width ID, got %d", len(got))
	}

	// 4. Missing key on dim 0 binary search.
	got, _ = scanTestEncoded(t, raw, 5, 4, snap, []compiledFilter{{dim: 0, ids: []uint32{65000}, single: 65000}})
	if len(got) != 0 {
		t.Fatalf("expected 0 rows for missing dim 0 ID, got %d", len(got))
	}

	// 5. High selectivity (50% -> selBits) & 100% hit range collapse.
	view, err := parseColumnarBlock(raw)
	if err != nil {
		t.Fatal(err)
	}
	slot := newQuerySlot()
	defer slot.close()
	sel50, err := filterStreamView(view, []compiledFilter{{dim: 3, ids: []uint32{evenTag}, single: evenTag}}, true, slot)
	if err != nil {
		t.Fatal(err)
	}
	if sel50.mode != selBits || sel50.count() != 300 {
		t.Fatalf("expected selBits with 300 rows, got mode=%d count=%d", sel50.mode, sel50.count())
	}
	sel50.release()

	sel100, err := filterStreamView(view, []compiledFilter{{dim: 3, ids: []uint32{evenTag, oddTag}, small: [8]uint32{evenTag, oddTag}, smallLen: 2}}, true, slot)
	if err != nil {
		t.Fatal(err)
	}
	if sel100.mode != selRange || sel100.count() != 600 {
		t.Fatalf("expected 100%% hit range collapse, got mode=%d count=%d", sel100.mode, sel100.count())
	}
	sel100.release()
}

func TestSoAScanBatchAndColCacheSelectivityFallback(t *testing.T) {
	// 1. Selectivity-driven prepareOutput (<25% stays non-fullBlock; >=25% upgrades to fullBlock).
	var view columnarBlockView
	view.count = 10000
	view.descs[0] = columnStreamDesc{mode: modeU32, width: 4}
	view.descs[1] = columnStreamDesc{mode: modeU32, width: 4}
	for m := range 4 {
		view.descs[8+m] = columnStreamDesc{mode: modeU32, width: 4}
	}
	view.descs[12] = columnStreamDesc{mode: modeConst, value: 0x0f}
	colMask := uint16(1<<0) | uint16(1<<1) | (uint16(0x1f) << 8)
	slot := newQuerySlot()
	defer slot.close()

	lowCache := newBlockColCache(view, colMask, uint16(1<<0), 512<<10, slot)
	lowCache.prepareOutput(colMask, 2000, false)
	if lowCache.fullBlock || lowCache.availPackBytes() < 256*lowCache.packRowBytes(colMask, false) {
		t.Fatalf("low selectivity cache state invalid: full=%v avail=%d", lowCache.fullBlock, lowCache.availPackBytes())
	}
	highCache := newBlockColCache(view, colMask, uint16(1<<0), 512<<10, slot)
	highCache.prepareOutput(colMask, 2500, false)
	if !highCache.fullBlock {
		t.Fatalf("expected >=25%% selectivity to upgrade to fullBlock")
	}

	// 2. End-to-end SoA ScanBatch (stream + small + live, full & partial projections) + constrained fallback parity.
	ctx := context.Background()
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	dbPath := filepath.Join(t.TempDir(), "soa-cache.db")
	ops := [4]MetricOp{OpSum, OpMin, OpMax, OpLast}
	wdb, err := Open(dbPath, Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	ws, _ := wdb.Series(1, SeriesConfig{Arity: 4, SummaryOps: ops, RowOps: ops})
	addSlot := func(ts time.Time, count int, prefix string) {
		rows := make([]Row, count)
		var sum Summary
		for i := range count {
			v := [4]int64{int64(i % 200), int64((i%500)*100 + 7), int64(70000 + i*13), -int64(1000000 + i*997)}
			mask := uint8(0x0f)
			if i%11 == 0 {
				mask = 0x05
			}
			rows[i] = Row{
				Tags:   []string{fmt.Sprintf("u-%02d", i%10), fmt.Sprintf("%s-h-%03d", prefix, i%320), "const-reg", fmt.Sprintf("k-%05d", i)},
				Values: v, Mask: mask,
			}
			mergeSummary(&sum, Summary{Values: v, Mask: mask}, ops, uint64(i+1), uint64(i))
		}
		if err := ws.Append(ts, sum, rows...); err != nil {
			t.Fatal(err)
		}
	}
	addSlot(now.Add(-5*time.Minute), 4000, "str")
	addSlot(now.Add(-4*time.Minute), 40, "sml")
	if err := wdb.Compact(); err != nil {
		t.Fatal(err)
	}
	addSlot(now.Add(-1*time.Minute), 30, "liv")
	_ = wdb.Close()

	dbBig, err := Open(dbPath, Options{ReadOnly: true, Now: func() time.Time { return now }, MemoryBudget: MemoryBudget{QueryBytes: 16 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	defer dbBig.Close()
	dbTight, err := Open(dbPath, Options{ReadOnly: true, Now: func() time.Time { return now }, MemoryProfile: MemoryConstrained, MemoryBudget: MemoryBudget{QueryBytes: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	defer dbTight.Close()
	sBig, _ := dbBig.Series(1, SeriesConfig{Arity: 4, SummaryOps: ops, RowOps: ops})
	sTight, _ := dbTight.Series(1, SeriesConfig{Arity: 4, SummaryOps: ops, RowOps: ops})

	for _, f := range [][]Filter{
		nil,
		{{Dim: 0, Op: TagEq, Values: []string{"u-03"}}},
		{{Dim: 1, Op: TagPrefix, Values: []string{"str-h-01"}}},
	} {
		aBig, err := sBig.Aggregate(ctx, AggregateQuery{AllTime: true, Filters: f, GroupDims: []int{0}})
		if err != nil {
			t.Fatal(err)
		}
		aTight, err := sTight.Aggregate(ctx, AggregateQuery{AllTime: true, Filters: f, GroupDims: []int{0}})
		if err != nil {
			aBig.Release()
			t.Fatal(err)
		}
		if aBig.Summary != aTight.Summary || !slices.Equal(aBig.Groups[0], aTight.Groups[0]) {
			t.Fatalf("Aggregate fallback mismatch for %+v", f)
		}
		aBig.Release()
		aTight.Release()

		runScan := func(ser *Series) (int, int64) {
			var cnt int
			var sumV3 int64
			err := ser.Scan(ctx, ScanQuery{
				AllTime: true, Filters: f, BatchSize: 128,
				ProjectDims: (1 << 0) | (1 << 2), ProjectMetrics: (1 << 1) | (1 << 3),
			}, func(b ScanBatch) error {
				for i := range b.Len() {
					if b.TagID(i, 1) != 0 || b.Tag(i, 1) != "" || b.TagID(i, 3) != 0 || b.Values(i)[0] != 0 || b.Values(i)[2] != 0 {
						return fmt.Errorf("unprojected field leaked")
					}
					cnt++
					sumV3 += b.Values(i)[3]
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			return cnt, sumV3
		}
		c1, v1 := runScan(sBig)
		c2, v2 := runScan(sTight)
		if c1 != c2 || v1 != v2 || c1 == 0 {
			t.Fatalf("Scan parity mismatch for %+v: big=(%d,%d) tight=(%d,%d)", f, c1, v1, c2, v2)
		}
	}
}

func TestColumnarStreamsRawFallback(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 30, 0, 0, time.UTC)
	ts0 := now.Add(-10 * time.Minute)
	ts1 := now.Add(-5 * time.Minute)
	path := filepath.Join(t.TempDir(), "streams-raw.db")
	opts := Options{Now: func() time.Time { return now }}
	cfg := SeriesConfig{
		Arity:      2,
		SummaryOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast},
		RowOps:     [4]MetricOp{OpSum, OpMin, OpMax, OpLast},
	}

	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed && db != nil {
			_ = db.Close()
		}
	}()
	s := mustSeries(t, db, 1, cfg)

	// Pre-intern 255 distinct tags per dim at ts0 (<256 rows -> small block)
	// so ts1 uses 1-byte modeU8 IDs 1..255 with no dictionary growth.
	warmRows := make([]Row, 255)
	for i := range warmRows {
		warmRows[i] = Row{
			Tags: []string{"d0-" + strconv.Itoa(100+i), "d1-" + strconv.Itoa(100+i)},
			Mask: 0x0f,
		}
	}
	if err = s.AppendSync(ts0, Summary{Mask: 0x0f}, warmRows...); err != nil {
		t.Fatal(err)
	}

	const rowCount = 523
	const targetIdx = 271
	rows := make([]Row, rowCount)
	var seen [255][255]bool
	rng := uint64(0x9e3779b97f4a7c15)
	nextRand := func() uint64 {
		rng ^= rng << 13
		rng ^= rng >> 7
		rng ^= rng << 17
		return rng
	}

	var sumV0, minV1, maxV2, lastV3 int64
	for i := range rows {
		var a, b int
		for {
			a = int(nextRand() % 255)
			b = int(nextRand() % 255)
			if !seen[a][b] {
				seen[a][b] = true
				break
			}
		}
		v0 := int64(1 + (i % 97) + int(nextRand()%19))
		v1 := int64(1000 - i)
		v2 := int64(2000 + i)
		v3 := int64(nextRand())
		rows[i] = Row{
			Tags:   []string{"d0-" + strconv.Itoa(100+a), "d1-" + strconv.Itoa(100+b)},
			Values: [4]int64{v0, v1, v2, v3},
			Mask:   0x0f,
		}
		sumV0 += v0
		if i == 0 || v1 < minV1 {
			minV1 = v1
		}
		if i == 0 || v2 > maxV2 {
			maxV2 = v2
		}
		lastV3 = v3
	}
	if err = s.AppendSync(ts1, Summary{Values: [4]int64{sumV0, minV1, maxV2, lastV3}, Mask: 0x0f}, rows...); err != nil {
		t.Fatal(err)
	}
	if err = db.Compact(); err != nil {
		t.Fatal(err)
	}
	db = reopenTestDB(t, db, path, opts)
	s = mustSeries(t, db, 1, cfg)

	targetTag0 := rows[targetIdx].Tags[0]
	targetTag1 := rows[targetIdx].Tags[1]
	targetRow := rows[targetIdx]
	var sumForTag0, lastV3Tag0 int64
	for _, r := range rows {
		if r.Tags[0] == targetTag0 {
			sumForTag0 += r.Values[0]
			lastV3Tag0 = r.Values[3]
		}
	}

	var ts1Raw []byte
	var mapped []byte
	if db.mapped != nil {
		mapped = db.mapped.bytes()
	}
	for i := range db.sealed {
		m := &db.sealed[i]
		if m.series == s.id && m.res == resMinuteHR && m.start == ts1.Unix() {
			ts1Raw, err = sectionBlock(db.data, db.head.sections[secTSBlocks], m.offset, m.length, mapped)
			if err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if len(ts1Raw) == 0 {
		t.Fatal("missing compacted ts1 block")
	}
	if !bytes.HasPrefix(ts1Raw, blockStreamsMagic[:]) {
		t.Fatalf("ts1 block missing STR2 magic: %x", ts1Raw[:min(8, len(ts1Raw))])
	}
	view, err := parseColumnarBlock(ts1Raw)
	if err != nil {
		t.Fatal(err)
	}
	if view.descs[0].flags != streamFlagUnsorted|streamFlagRaw {
		t.Fatalf("dim0 expected unsorted+raw flags=%#x", view.descs[0].flags)
	}
	if view.descs[1].flags&streamFlagRaw == 0 {
		t.Fatalf("dim1 expected raw flag=%#x", view.descs[1].flags)
	}
	if view.descs[11].flags&streamFlagRaw == 0 {
		t.Fatalf("values[3] expected raw flag=%#x", view.descs[11].flags)
	}
	for _, idx := range []int{0, 1, 11} {
		if view.descs[idx].length != view.descs[idx].rawLen {
			t.Fatalf("raw stream %d length=%d want rawLen=%d", idx, view.descs[idx].length, view.descs[idx].rawLen)
		}
	}

	decoded, err := decodeBlock(ts1Raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.rows) != rowCount {
		t.Fatalf("decoded row count=%d want=%d", len(decoded.rows), rowCount)
	}
	if decoded.rows[targetIdx].values != targetRow.Values {
		t.Fatalf("decoded target row values=%v want=%v", decoded.rows[targetIdx].values, targetRow.Values)
	}

	// Corrupt rawLen on values[3] descriptor in a copied STR2 block and verify rejection.
	bad := append([]byte(nil), ts1Raw...)
	var rawLenLE [4]byte
	binary.LittleEndian.PutUint32(rawLenLE[:], view.descs[11].rawLen)
	hdrEnd := min(len(bad), len(blockStreamsMagic)+int(view.descs[0].offset))
	pos := bytes.LastIndex(bad[:hdrEnd], rawLenLE[:])
	if pos < 0 {
		t.Fatalf("failed to locate values[3] rawLen %d in header", view.descs[11].rawLen)
	}
	bad[pos] ^= 1
	if _, err = decodeBlock(bad); err == nil {
		t.Fatal("expected decodeBlock to reject corrupted rawLen on raw stream")
	}

	ctx := context.Background()
	qFrom, qTo := ts1, ts1.Add(time.Minute)

	checkQueries := func(cur *Series, label string) {
		if got := queryAggSum(t, cur, AggregateQuery{From: qFrom, To: qTo, PreferFine: true}); got != sumV0 {
			t.Fatalf("%s unfiltered Aggregate sum=%d want=%d", label, got, sumV0)
		}
		aggTag0, err := cur.Aggregate(ctx, AggregateQuery{
			From:       qFrom,
			To:         qTo,
			Filters:    []Filter{{Dim: 0, Op: TagEq, Values: []string{targetTag0}}},
			GroupDims:  []int{0},
			PreferFine: true,
		})
		if err != nil {
			t.Fatalf("%s dim0 filtered Aggregate: %v", label, err)
		}
		gotAgg0, gotAgg3 := aggTag0.Summary.Values[0], aggTag0.Summary.Values[3]
		aggTag0.Release()
		if gotAgg0 != sumForTag0 || gotAgg3 != lastV3Tag0 {
			t.Fatalf("%s dim0 Aggregate got=(%d,%d) want=(%d,%d)", label, gotAgg0, gotAgg3, sumForTag0, lastV3Tag0)
		}

		exactFilters := []Filter{
			{Dim: 0, Op: TagEq, Values: []string{targetTag0}},
			{Dim: 1, Op: TagEq, Values: []string{targetTag1}},
		}
		aggPoint, err := cur.Aggregate(ctx, AggregateQuery{
			From: qFrom, To: qTo, Filters: exactFilters, PreferFine: true,
		})
		if err != nil {
			t.Fatalf("%s point Aggregate: %v", label, err)
		}
		gotPoint := aggPoint.Summary
		aggPoint.Release()
		if gotPoint.Values != targetRow.Values {
			t.Fatalf("%s point Aggregate values=%v want=%v", label, gotPoint.Values, targetRow.Values)
		}

		if err = readCheckErr(ctx, cur, ReadRequest{
			From: qFrom, To: qTo, Filters: exactFilters, Limit: 10, PreferFine: true,
		}, func(p *PageResult) error {
			if len(p.Rows) != 1 || p.Rows[0].Values != targetRow.Values || !slices.Equal(p.Rows[0].Tags, targetRow.Tags) {
				return fmt.Errorf("%s filtered Read mismatch: %+v", label, p.Rows)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err = readCheckErr(ctx, cur, ReadRequest{
			From: qFrom, To: qTo, Offset: 200, Limit: 100, PreferFine: true,
		}, func(p *PageResult) error {
			got := p.Rows[targetIdx-200]
			if got.Values != targetRow.Values || !slices.Equal(got.Tags, targetRow.Tags) {
				return fmt.Errorf("%s light slice Read mismatch: got=%+v want=%+v", label, got, targetRow)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err = readCheckErr(ctx, cur, ReadRequest{
			From: qFrom, To: qTo, Limit: 600, PreferFine: true,
		}, func(p *PageResult) error {
			if len(p.Rows) != rowCount {
				return fmt.Errorf("%s full Read count=%d want=%d", label, len(p.Rows), rowCount)
			}
			if p.Rows[targetIdx].Values != targetRow.Values || !slices.Equal(p.Rows[targetIdx].Tags, targetRow.Tags) {
				return fmt.Errorf("%s full Read row %d mismatch: got=%+v want=%+v", label, targetIdx, p.Rows[targetIdx], targetRow)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		cnt, scanSum, err := scanCountSumErr(ctx, cur, ScanQuery{
			From: qFrom, To: qTo, BatchSize: 64, PreferFine: true,
		})
		if err != nil || cnt != rowCount || scanSum != sumV0 {
			t.Fatalf("%s full Scan: cnt=%d want=%d sum=%d want=%d err=%v", label, cnt, rowCount, scanSum, sumV0, err)
		}
		var exactCnt int
		var exactRow Row
		if err = cur.Scan(ctx, ScanQuery{
			From: qFrom, To: qTo, Filters: exactFilters, ProjectDims: 0x03, ProjectMetrics: 0x0f, BatchSize: 64, PreferFine: true,
		}, func(b ScanBatch) error {
			for i := range b.Len() {
				exactCnt++
				exactRow = b.CloneRow(i)
			}
			return nil
		}); err != nil {
			t.Fatalf("%s filtered Scan: %v", label, err)
		}
		if exactCnt != 1 || exactRow.Values != targetRow.Values || !slices.Equal(exactRow.Tags, targetRow.Tags) {
			t.Fatalf("%s filtered Scan mismatch: cnt=%d row=%+v want=%+v", label, exactCnt, exactRow, targetRow)
		}
	}

	checkQueries(s, "default")

	closed = true
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	tightDB, err := Open(path, Options{
		ReadOnly:      true,
		Now:           opts.Now,
		MemoryProfile: MemoryConstrained,
		MemoryBudget:  MemoryBudget{QueryBytes: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tightDB.Close() }()
	checkQueries(mustSeries(t, tightDB, 1, cfg), "tight")
}
