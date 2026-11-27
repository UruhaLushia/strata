package strata

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func benchSeriesConfig() SeriesConfig {
	return SeriesConfig{
		Arity:      7,
		SummaryOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast},
		RowOps:     [4]MetricOp{OpSum, OpMin, OpMax, OpLast},
	}
}

func requireGatewayStress(tb testing.TB) {
	tb.Helper()
	if os.Getenv("STRATA_STRESS_90D") != "1" {
		tb.Skip("set STRATA_STRESS_90D=1 to run the 90-day million-row gateway suite")
	}
}

type gateway90Fixture struct {
	path         string
	now          time.Time
	rawRows      int
	compactions  int
	compactTotal time.Duration
	lastCompact  time.Duration
	sealed1m     int
	sealed1h     int
	sealed1d     int
	totalSealed  int
}

func buildGateway90DayFixture(t testing.TB) gateway90Fixture {
	return buildGateway90DayFixtureMode(t, false)
}

func buildGateway90DayFixtureMode(t testing.TB, extreme bool) gateway90Fixture {
	t.Helper()
	base := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	start := base.Add(-90 * 24 * time.Hour)
	clock := base
	path := filepath.Join(t.TempDir(), "strata.db")
	db, err := Open(path, Options{
		Now:         func() time.Time { return clock },
		Retention1m: 14 * 24 * time.Hour,
		Retention1h: 90 * 24 * time.Hour,
		Retention1d: 365 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	s, err := db.Series(77, benchSeriesConfig())
	if err != nil {
		t.Fatal(err)
	}
	stats := gateway90Fixture{path: path, now: base}
	global := 0
	appendRows := func(ts time.Time, count int) {
		rows := gatewayRowsMode(global, count, extreme)
		if err := s.Append(ts, Summary{Values: [4]int64{int64(count), int64(count), int64(count), int64(count)}}, rows...); err != nil {
			t.Fatal(err)
		}
		stats.rawRows += count
		global += count
	}
	compactAt := func(now time.Time) {
		clock = now
		started := time.Now()
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(started)
		stats.compactions++
		stats.compactTotal += elapsed
		stats.lastCompact = elapsed
	}
	for day := range 76 {
		for hour := range 24 {
			appendRows(start.Add(time.Duration(day*24+hour)*time.Hour), 250)
		}
		if (day+1)%14 == 0 {
			compactAt(start.Add(time.Duration(day+1) * 24 * time.Hour))
		}
	}
	for day := 76; day < 90; day++ {
		for minute := range 24 * 60 {
			appendRows(start.Add(time.Duration(day*24*60+minute)*time.Minute), 30)
		}
	}
	clock = base
	appendRows(base, 30)
	regions := make([]*Region, 3)
	for i := range regions {
		regions[i], err = db.Region("gateway-" + strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
	}
	batch := db.NewBatch()
	for i := range 2000 {
		region := regions[i%len(regions)]
		batch.In(region).Put("gateway/key/"+strconv.Itoa(i), []byte("gateway-value-"+strconv.Itoa(i)))
	}
	if err := batch.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	compactAt(base)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	check, err := Open(path, Options{ReadOnly: true, Now: func() time.Time { return base }})
	if err != nil {
		t.Fatal(err)
	}
	for _, block := range check.sealed {
		switch block.res {
		case resMinuteHR:
			stats.sealed1m++
		case resHour:
			stats.sealed1h++
		case resDay:
			stats.sealed1d++
		}
	}
	stats.totalSealed = len(check.sealed)
	if err := check.Close(); err != nil {
		t.Fatal(err)
	}
	if stats.rawRows < 1_000_000 {
		t.Fatalf("gateway fixture has only %d rows", stats.rawRows)
	}
	return stats
}

func gatewayRowsMode(offset, count int, extreme bool) []Row {
	rows := make([]Row, count)
	for i := range rows {
		n := offset + i
		network := "udp"
		if n&1 == 0 {
			network = "tcp"
		}
		rows[i] = Row{
			Tags: []string{
				gatewayDomain(n), "app-" + strconv.Itoa(n%120), "node-" + strconv.Itoa(n%50),
				"category-" + strconv.Itoa(n%20), "rule-" + strconv.Itoa(n%80),
				gatewayDestMode(n, extreme), network,
			},
			Values: [4]int64{1, 1, 1, 1},
		}
	}
	return rows
}

func gatewayDomain(n int) string {
	if n%100000 == 0 {
		return "domain-4999"
	}
	if n%10 < 7 {
		return "domain-" + strconv.Itoa(n%50)
	}
	return "domain-" + strconv.Itoa(50+(n*7919+n/17)%4950)
}

func gatewayDestMode(n int, extreme bool) string {
	if extreme {
		return fmt.Sprintf("2001:db8:%x::%x", n/65536, n%65536)
	}
	id := n % 2000
	if id < 1000 {
		return fmt.Sprintf("198.51.%d.%d", id/250, id%250+1)
	}
	id -= 1000
	return fmt.Sprintf("2001:db8:%x::%x", id/256, id%256)
}

func openGateway90Fixture(t testing.TB, fx gateway90Fixture, mmap bool) (*DB, *Series) {
	t.Helper()
	db, err := Open(fx.path, Options{ReadOnly: true, UseMmap: mmap, Now: func() time.Time { return fx.now }})
	if err != nil {
		t.Fatal(err)
	}
	s, err := db.Series(77, benchSeriesConfig())
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db, s
}

type gatewayQueryCase struct {
	name string
	run  func(ctx context.Context) (int, error)
}

func gatewayQueries(s *Series, now time.Time) []gatewayQueryCase {
	from90d := now.Add(-90 * 24 * time.Hour)
	return []gatewayQueryCase{
		{"ReadLast15mPage200", func(ctx context.Context) (int, error) {
			p, err := s.Read(ctx, ReadRequest{From: now.Add(-15 * time.Minute), To: now, Limit: 200})
			if err != nil {
				return 0, err
			}
			n := len(p.Rows)
			p.Release()
			return n, nil
		}},
		{"ReadToday24hFilteredPage200", func(ctx context.Context) (int, error) {
			p, err := s.Read(ctx, ReadRequest{From: now.Add(-24 * time.Hour), To: now, Limit: 200, Filters: []Filter{{Dim: 0, Op: TagPrefix, Values: []string{"domain-"}}}})
			if err != nil {
				return 0, err
			}
			n := len(p.Rows)
			p.Release()
			return n, nil
		}},
		{"Read90dDeepOffset5000", func(ctx context.Context) (int, error) {
			p, err := s.Read(ctx, ReadRequest{From: from90d, To: now, Offset: 5000, Limit: 200})
			if err != nil {
				return 0, err
			}
			n := len(p.Rows)
			p.Release()
			return n, nil
		}},
		{"Scan90dTimeFirstIDs", func(ctx context.Context) (int, error) {
			rows := 0
			err := s.Scan(ctx, ScanQuery{From: from90d, To: now}, func(b ScanBatch) error {
				rows += b.Len()
				return nil
			})
			return rows, err
		}},
		{"Scan90dSparseMeta", func(ctx context.Context) (int, error) {
			rows := 0
			err := s.Scan(ctx, ScanQuery{From: from90d, To: now, Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"domain-4999"}}}}, func(b ScanBatch) error {
				rows += b.Len()
				return nil
			})
			return rows, err
		}},
		{"Scan90dMediumMeta", func(ctx context.Context) (int, error) {
			rows := 0
			err := s.Scan(ctx, ScanQuery{From: from90d, To: now, Filters: []Filter{
				{Dim: 1, Op: TagEq, Values: []string{"app-7"}}, {Dim: 2, Op: TagEq, Values: []string{"node-17"}},
			}}, func(b ScanBatch) error {
				rows += b.Len()
				return nil
			})
			return rows, err
		}},
		{"Scan90dDenseMeta", func(ctx context.Context) (int, error) {
			rows := 0
			err := s.Scan(ctx, ScanQuery{From: from90d, To: now, Filters: []Filter{{Dim: 6, Op: TagEq, Values: []string{"tcp"}}}}, func(b ScanBatch) error {
				rows += b.Len()
				return nil
			})
			return rows, err
		}},
	}
}

func logIncrementalCompactTelemetry(t *testing.T, fixtureName string, fx gateway90Fixture, extreme bool) {
	t.Helper()
	for _, tc := range []struct {
		name    string
		profile MemoryProfile
	}{
		{"default", MemoryBalanced},
		{"constrained", MemoryConstrained},
	} {
		dst := filepath.Join(t.TempDir(), "inc.db")
		srcF, err := os.Open(fx.path)
		if err != nil {
			t.Fatal(err)
		}
		dstF, err := os.Create(dst)
		if err != nil {
			_ = srcF.Close()
			t.Fatal(err)
		}
		_, err = io.Copy(dstF, srcF)
		_ = srcF.Close()
		_ = dstF.Close()
		if err != nil {
			t.Fatal(err)
		}
		clock := fx.now.Add(20 * time.Minute)
		db, err := Open(dst, Options{
			Now:           func() time.Time { return clock },
			MemoryProfile: tc.profile,
			Retention1m:   14 * 24 * time.Hour,
			Retention1h:   90 * 24 * time.Hour,
			Retention1d:   365 * 24 * time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
		s, err := db.Series(77, benchSeriesConfig())
		if err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		for m := range 18 {
			rows := gatewayRowsMode(fx.rawRows+m*30, 30, extreme)
			ts := fx.now.Add(time.Duration(m+1) * time.Minute)
			if err := s.Append(ts, Summary{Values: [4]int64{30, 30, 30, 30}}, rows...); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
		}
		var msBefore, msAfter runtime.MemStats
		runtime.ReadMemStats(&msBefore)
		started := time.Now()
		if err := db.Compact(); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		elapsed := time.Since(started)
		runtime.ReadMemStats(&msAfter)
		afterMem, afterOS := sampleCleanMem()
		t.Logf("fixture=%s phase=inc_compact profile=%s elapsed=%s total_alloc_delta=%d live_after=%d SecTSInvert=%d HeapAlloc=%d HeapInuse=%d RssAnon=%d VmHWM=%d",
			fixtureName, tc.name, elapsed, msAfter.TotalAlloc-msBefore.TotalAlloc, len(db.live),
			db.head.sections[secTSInvert].length, afterMem.HeapAlloc, afterMem.HeapInuse, afterOS.RssAnonBytes, afterOS.VmHWMBytes)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGateway90DayMillionReport(t *testing.T) {
	requireGatewayStress(t)
	ctx := context.Background()
	for _, fixture := range []struct {
		name    string
		extreme bool
	}{
		{"RealisticGateway", false},
		{"ExtremeHighCard", true},
	} {
		fx := buildGateway90DayFixtureMode(t, fixture.extreme)
		t.Logf("fixture=%s raw_rows=%d sealed_1m=%d sealed_1h=%d sealed_1d=%d total_sealed=%d compactions=%d compact_total=%s last_compact=%s",
			fixture.name, fx.rawRows, fx.sealed1m, fx.sealed1h, fx.sealed1d, fx.totalSealed, fx.compactions, fx.compactTotal, fx.lastCompact)
		for _, mmap := range []bool{false, true} {
			db, s := openGateway90Fixture(t, fx, mmap)
			client := NewClient(db, ClientOptions{})
			mem, osMem := sampleCleanMem()
			info, err := os.Stat(db.path)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("fixture=%s phase=open mmap=%t file=%d SecTSBlocks=%d SecTSIndex=%d SecTSDict=%d SecTSInvert=%d HeapAlloc=%d HeapInuse=%d RSS=%d RssAnon=%d RssFile=%d PSS=%d VmHWM=%d",
				fixture.name, mmap, info.Size(), db.head.sections[secTSBlocks].length, db.head.sections[secTSIndex].length, db.head.sections[secTSDict].length, db.head.sections[secTSInvert].length,
				mem.HeapAlloc, mem.HeapInuse, osMem.RSSBytes, osMem.RssAnonBytes, osMem.RssFileBytes, osMem.PSSBytes, osMem.VmHWMBytes)
			for _, query := range gatewayQueries(s, fx.now) {
				started := time.Now()
				rows, err := query.run(ctx)
				if err != nil {
					t.Fatal(err)
				}
				qMem, qOS := sampleCleanMem()
				t.Logf("fixture=%s mmap=%t query=%s elapsed=%s rows=%d HeapAlloc=%d RssAnon=%d VmHWM=%d",
					fixture.name, mmap, query.name, time.Since(started), rows, qMem.HeapAlloc, qOS.RssAnonBytes, qOS.VmHWMBytes)
			}
			client.Reclaim()
			rMem, rOS := sampleCleanMem()
			t.Logf("fixture=%s phase=after_reclaim mmap=%t HeapAlloc=%d HeapInuse=%d RssAnon=%d RssFile=%d PSS=%d VmHWM=%d",
				fixture.name, mmap, rMem.HeapAlloc, rMem.HeapInuse, rOS.RssAnonBytes, rOS.RssFileBytes, rOS.PSSBytes, rOS.VmHWMBytes)
			_ = client.Close()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
		logIncrementalCompactTelemetry(t, fixture.name, fx, fixture.extreme)
	}
}

func TestGateway90ReclaimedFirstQuery(t *testing.T) {
	requireGatewayStress(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		extreme bool
	}{
		{"RealisticGateway", false},
		{"ExtremeHighCard", true},
	} {
		fx := buildGateway90DayFixtureMode(t, tc.extreme)
		from90d := fx.now.Add(-90 * 24 * time.Hour)
		for _, mmap := range []bool{false, true} {
			db, _ := openGateway90Fixture(t, fx, mmap)
			client := NewClient(db, ClientOptions{})
			cases := []struct {
				name string
				run  func() error
			}{
				{"SummaryOnly", func() error {
					return client.WithAggregate(ctx, 77, AggregateQuery{From: from90d, To: fx.now}, nil)
				}},
				{"ReadLast15m", func() error {
					return client.WithPage(ctx, 77, ReadRequest{From: fx.now.Add(-15 * time.Minute), To: fx.now, Limit: 200}, nil)
				}},
				{"AggregateDomain", func() error {
					return client.WithAggregate(ctx, 77, AggregateQuery{From: from90d, To: fx.now, GroupDims: []int{0}}, nil)
				}},
			}
			if tc.extreme {
				cases = append(cases, struct {
					name string
					run  func() error
				}{"ExtremeDestIPTopK", func() error {
					return client.WithAggregate(ctx, 77, AggregateQuery{From: from90d, To: fx.now, GroupDims: []int{5}, TopK: 50}, nil)
				}})
			}
			for _, qc := range cases {
				client.Reclaim()
				_, beforeOS := sampleCleanMem()
				var beforeRT, afterRT runtime.MemStats
				runtime.ReadMemStats(&beforeRT)
				t0 := time.Now()
				if err := qc.run(); err != nil {
					t.Fatal(err)
				}
				dur := time.Since(t0)
				runtime.ReadMemStats(&afterRT)
				afterOS := sampleOSMem()
				t.Logf("fixture=%s phase=reclaimed_first mmap=%t query=%s elapsed=%s alloc_bytes=%d allocs=%d delta_anon=%d delta_file=%d HeapAlloc=%d RssAnon=%d RssFile=%d VmHWM=%d",
					tc.name, mmap, qc.name, dur, afterRT.TotalAlloc-beforeRT.TotalAlloc, afterRT.Mallocs-beforeRT.Mallocs,
					int64(afterOS.RssAnonBytes)-int64(beforeOS.RssAnonBytes), int64(afterOS.RssFileBytes)-int64(beforeOS.RssFileBytes),
					afterRT.HeapAlloc, afterOS.RssAnonBytes, afterOS.RssFileBytes, afterOS.VmHWMBytes)
			}
			_ = client.Close()
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func BenchmarkGateway90Day(b *testing.B) {
	requireGatewayStress(b)
	ctx := context.Background()
	fx := buildGateway90DayFixture(b)
	db, s := openGateway90Fixture(b, fx, false)
	b.Cleanup(func() { _ = db.Close() })
	for _, query := range gatewayQueries(s, fx.now) {
		b.Run(query.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := query.run(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGateway90DayExtremeHighCard(b *testing.B) {
	requireGatewayStress(b)
	ctx := context.Background()
	fx := buildGateway90DayFixtureMode(b, true)
	db, s := openGateway90Fixture(b, fx, false)
	b.Cleanup(func() { _ = db.Close() })
	for _, query := range gatewayQueries(s, fx.now) {
		b.Run(query.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := query.run(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkGateway90AggregateDomain(b *testing.B) {
	requireGatewayStress(b)
	ctx := context.Background()
	fx := buildGateway90DayFixture(b)
	db, s := openGateway90Fixture(b, fx, false)
	defer db.Close()
	q := AggregateQuery{From: fx.now.Add(-90 * 24 * time.Hour), To: fx.now, GroupDims: []int{0}}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		res, err := s.Aggregate(ctx, q)
		if err != nil {
			b.Fatal(err)
		}
		res.Release()
	}
}

func BenchmarkGateway90AggregateTopK(b *testing.B) {
	requireGatewayStress(b)
	ctx := context.Background()
	fx := buildGateway90DayFixture(b)
	db, s := openGateway90Fixture(b, fx, false)
	defer db.Close()
	q := AggregateQuery{From: fx.now.Add(-90 * 24 * time.Hour), To: fx.now, GroupDims: []int{0}, TopK: 20}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		res, err := s.Aggregate(ctx, q)
		if err != nil {
			b.Fatal(err)
		}
		res.Release()
	}
}

func BenchmarkGateway90AggregateFiltered(b *testing.B) {
	requireGatewayStress(b)
	ctx := context.Background()
	fx := buildGateway90DayFixture(b)
	db, s := openGateway90Fixture(b, fx, false)
	defer db.Close()
	q := AggregateQuery{
		From:      fx.now.Add(-90 * 24 * time.Hour),
		To:        fx.now,
		Filters:   []Filter{{Dim: 0, Op: TagEq, Values: []string{"domain-4999"}}},
		GroupDims: []int{0, 2},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		res, err := s.Aggregate(ctx, q)
		if err != nil {
			b.Fatal(err)
		}
		res.Release()
	}
}

func BenchmarkGateway90SummaryOnly(b *testing.B) {
	requireGatewayStress(b)
	ctx := context.Background()
	fx := buildGateway90DayFixture(b)
	db, s := openGateway90Fixture(b, fx, false)
	defer db.Close()
	q := AggregateQuery{From: fx.now.Add(-90 * 24 * time.Hour), To: fx.now}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		res, err := s.Aggregate(ctx, q)
		if err != nil {
			b.Fatal(err)
		}
		res.Release()
	}
}

func BenchmarkGateway90ExtremeDestIPTopK(b *testing.B) {
	requireGatewayStress(b)
	ctx := context.Background()
	fx := buildGateway90DayFixtureMode(b, true)
	db, s := openGateway90Fixture(b, fx, false)
	defer db.Close()
	q := AggregateQuery{From: fx.now.Add(-90 * 24 * time.Hour), To: fx.now, GroupDims: []int{5}, TopK: 50}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		res, err := s.Aggregate(ctx, q)
		if err != nil {
			b.Fatal(err)
		}
		res.Release()
	}
}

func BenchmarkGateway90OutboundLookup(b *testing.B) {
	requireGatewayStress(b)
	const outboundDim = 2
	for _, mode := range []struct {
		name    string
		extreme bool
		dim     int
		miss    string
	}{
		{"RealisticDomain", false, 0, "unknown.example"},
		{"ExtremeDestIP", true, 5, "2001:db8:ffff::1"},
	} {
		b.Run(mode.name, func(b *testing.B) {
			fx := buildGateway90DayFixtureMode(b, mode.extreme)
			db, s := openGateway90Fixture(b, fx, false)
			defer db.Close()
			dim := db.dict.dims[dictDimKey{series: s.id, dim: uint8(mode.dim)}]
			if len(db.live) == 0 || dim == nil || (dim.ext != nil) != mode.extreme {
				b.Fatal("fixture must include live rows and the expected dictionary backend")
			}
			// Pick a row five minutes before the end, in the sealed minute tier.
			rowID := fx.rawRows - 6*30
			hit := gatewayDomain(rowID)
			if mode.extreme {
				hit = gatewayDestMode(rowID, true)
			}
			for _, win := range []struct {
				name string
				span time.Duration
			}{{"15m", 15 * time.Minute}, {"1h", time.Hour}, {"24h", 24 * time.Hour}, {"7d", 7 * 24 * time.Hour}} {
				for _, lookup := range []struct {
					name, value string
					hit         bool
				}{{"Hit", hit, true}, {"Miss", mode.miss, false}} {
					b.Run(win.name+"/"+lookup.name, func(b *testing.B) {
						ctx := b.Context()
						to := fx.now.Add(time.Minute)
						q := AggregateQuery{
							From: to.Add(-win.span), To: to,
							Filters: tagFilter(mode.dim, TagEq, lookup.value), GroupDims: []int{outboundDim},
						}
						b.ReportAllocs()
						for b.Loop() {
							res, err := s.Aggregate(ctx, q)
							if err != nil {
								b.Fatal(err)
							}
							res.Release()
						}
						res, err := s.Aggregate(ctx, q)
						if err != nil {
							b.Fatal(err)
						}
						defer res.Release()
						if (len(res.Groups[0]) != 0) != lookup.hit {
							b.Fatalf("lookup %q produced %d outbound groups", lookup.value, len(res.Groups[0]))
						}
					})
				}
			}
		})
	}
}
