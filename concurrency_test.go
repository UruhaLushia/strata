package strata

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func dictCacheStats(db *DB) (used, maxBytes int) {
	if db == nil {
		return 0, 0
	}
	db.mu.RLock()
	c := db.dict.pageCache
	db.mu.RUnlock()
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	used, maxBytes = c.usedBytes, c.maxBytes
	c.mu.Unlock()
	return used, maxBytes
}

func TestCompactSealedSeqLessThanFenceAndCrash(t *testing.T) {
	now := time.Date(2026, 2, 10, 12, 34, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "strata.db")
	db, err := Open(path, Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	s, err := db.Series(1, SeriesConfig{Arity: 1, SummaryOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast}})
	if err != nil {
		t.Fatal(err)
	}
	old := now.Add(-2 * time.Hour)
	hot := now.Add(-time.Minute)
	if err := s.AppendSync(old, Summary{Values: [4]int64{10, 10, 10, 10}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(hot, Summary{Values: [4]int64{20, 20, 20, 20}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	fence := db.appendSeq
	var wg sync.WaitGroup
	wg.Go(func() {
		_ = s.AppendSync(now.Add(-30*time.Second), Summary{Values: [4]int64{30, 30, 30, 30}})
	})
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if db.head.sealedWalSeq >= fence {
		t.Fatalf("expected unfinished fence, sealed=%d fence=%d", db.head.sealedWalSeq, fence)
	}
	ctx := context.Background()
	for range 3 {
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
	}
	res, err := s.Aggregate(ctx, AggregateQuery{From: old, To: now})
	if err != nil || res.Summary.Values[0] != 60 {
		t.Fatalf("repeated compaction changed aggregate: err=%v result=%+v", err, res)
	}
	res.Release()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(path + ".lock")
}

func TestReaderRefreshAcrossRename(t *testing.T) {
	for _, useMmap := range []bool{false, true} {
		t.Run(map[bool]string{false: "readat", true: "mmap"}[useMmap], func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Minute)
			path := filepath.Join(t.TempDir(), "strata.db")
			writer, err := Open(path, Options{Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			series, _ := writer.Series(3, SeriesConfig{Arity: 1})
			reader, err := Open(path, Options{ReadOnly: true, UseMmap: useMmap})
			if err != nil {
				t.Fatal(err)
			}
			if err := series.AppendSync(now.Add(-2*time.Hour), Summary{Values: [4]int64{1, 1, 1, 1}}); err != nil {
				t.Fatal(err)
			}
			readerData := reader.data
			if err := reader.Refresh(); err != nil {
				t.Fatal(err)
			}
			if reader.data != readerData {
				t.Fatal("incremental refresh reopened unchanged generation")
			}
			if err := writer.Compact(); err != nil {
				t.Fatal(err)
			}
			if err := reader.Refresh(); err != nil {
				t.Fatal(err)
			}
			rs, _ := reader.Series(3, SeriesConfig{Arity: 1})
			ctx := context.Background()
			res, err := rs.Aggregate(ctx, AggregateQuery{From: now.Add(-3 * time.Hour), To: now})
			if err != nil || len(res.Segments) == 0 {
				t.Fatalf("refresh result err=%v res=%+v", err, res)
			}
			res.Release()
			if err := series.AppendSync(now.Add(-time.Minute), Summary{Values: [4]int64{2, 2, 2, 2}}); err != nil {
				t.Fatal(err)
			}
			if err := reader.Refresh(); err != nil {
				t.Fatal(err)
			}
			res, err = rs.Aggregate(ctx, AggregateQuery{From: now.Add(-3 * time.Hour), To: now})
			if err != nil || res.Summary.Values[0] < 3 {
				t.Fatalf("tail refresh err=%v result=%+v", err, res)
			}
			res.Release()
			_ = reader.Close()
			_ = writer.Close()
		})
	}
}

func TestCompactFailureKeepsOldBlocksReadable(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	dbPath := filepath.Join(t.TempDir(), "fail.db")
	ops := [4]MetricOp{OpSum, OpMin, OpMax, OpLast}

	db, err := Open(dbPath, Options{Now: func() time.Time { return now }, MemoryProfile: MemoryBalanced})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s, _ := db.Series(1, SeriesConfig{Arity: 2, SummaryOps: ops, RowOps: ops})
	appendBatch := func(ts time.Time, startID, count int) {
		rows := make([]Row, count)
		var sum Summary
		for i := range count {
			v := [4]int64{int64(startID + i + 1), int64(10 + i), int64(100 + i), int64(1000 + startID + i)}
			rows[i] = Row{Tags: []string{fmt.Sprintf("g-%02d", i%10), fmt.Sprintf("k-%04d", startID+i)}, Values: v, Mask: 0x0f}
			mergeSummary(&sum, Summary{Values: v, Mask: 0x0f}, ops, uint64(i+1), uint64(i))
		}
		if err := s.Append(ts, sum, rows...); err != nil {
			t.Fatal(err)
		}
	}
	appendBatch(now.Add(-5*time.Minute), 0, 300)
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	appendBatch(now.Add(-2*time.Minute), 300, 260)
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}

	type snap struct {
		summary Summary
		scanCnt int
		scanV0  int64
		groups  []GroupRow
	}
	capture := func(ser *Series) snap {
		var out snap
		sumAgg, err := ser.Aggregate(ctx, AggregateQuery{AllTime: true})
		if err != nil {
			t.Fatal(err)
		}
		out.summary = sumAgg.Summary
		sumAgg.Release()
		if err := ser.Scan(ctx, ScanQuery{AllTime: true, BatchSize: 128}, func(b ScanBatch) error {
			for i := range b.Len() {
				out.scanCnt++
				out.scanV0 += b.Values(i)[0]
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		grpAgg, err := ser.Aggregate(ctx, AggregateQuery{AllTime: true, GroupDims: []int{0}})
		if err != nil {
			t.Fatal(err)
		}
		out.groups = slices.Clone(grpAgg.Groups[0])
		grpAgg.Release()
		return out
	}
	assertSame := func(label string, got, want snap) {
		if got.summary != want.summary || got.scanCnt != want.scanCnt || got.scanV0 != want.scanV0 || !slices.Equal(got.groups, want.groups) {
			t.Fatalf("%s diverged: got=(%+v,%d,%d) want=(%+v,%d,%d)", label, got.summary, got.scanCnt, got.scanV0, want.summary, want.scanCnt, want.scanV0)
		}
	}

	want := capture(s)

	// Cross-platform failure: non-empty directory at db.path+".tmp" blocks Compact temp creation.
	tmpDir := dbPath + ".tmp"
	if err := os.Mkdir(tmpDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "blocker"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()
	if err := db.Compact(); err == nil {
		t.Fatal("expected Compact failure when .tmp is a non-empty directory")
	}
	assertSame("after-tmp-open-failure", capture(s), want)
	_ = os.RemoveAll(tmpDir)

	// Mid-Compact renameAtomic failure on Unix.
	if runtime.GOOS != "windows" {
		db.compactHook = func() { _ = os.Remove(dbPath + ".tmp") }
		err := db.Compact()
		db.compactHook = nil
		if err == nil {
			t.Fatal("expected Compact failure when .tmp removed before renameAtomic")
		}
		assertSame("after-rename-failure", capture(s), want)
	}

	// Recovery Compact succeeds and persists cleanly.
	if err := db.Compact(); err != nil {
		t.Fatalf("recovery Compact failed: %v", err)
	}
	assertSame("after-recovery", capture(s), want)
}

func TestConcurrentReadCompactReclaimRace(t *testing.T) {
	ctx := context.Background()
	ops := [4]MetricOp{OpSum, OpMin, OpMax, OpLast}
	baseNow := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)

	for _, profile := range []MemoryProfile{MemoryConstrained, MemoryBalanced} {
		for _, mmap := range []bool{false, true} {
			t.Run(fmt.Sprintf("profile=%d-mmap=%t", profile, mmap), func(t *testing.T) {
				var clockSec atomic.Int64
				clockSec.Store(baseNow.Unix())
				db, err := Open(filepath.Join(t.TempDir(), "race.db"), Options{
					Now:           func() time.Time { return time.Unix(clockSec.Load(), 0).UTC() },
					MemoryProfile: profile,
					UseMmap:       mmap,
				})
				if err != nil {
					t.Fatal(err)
				}
				client := NewClient(db, ClientOptions{})
				s, _ := db.Series(1, SeriesConfig{Arity: 2, SummaryOps: ops, RowOps: ops})

				for slot := range 3 {
					ts := baseNow.Add(time.Duration(slot-5) * time.Minute)
					rows := make([]Row, 80)
					for i := range rows {
						rows[i] = Row{
							Tags:   []string{fmt.Sprintf("dom-%02d", i%20), fmt.Sprintf("ip-%04d", slot*80+i)},
							Values: [4]int64{int64(i + 1), 1, int64(i + 10), int64(slot*1000 + i)},
							Mask:   0x0f,
						}
					}
					if err := s.Append(ts, Summary{Values: [4]int64{80, 1, 90, 1000}, Mask: 0x0f}, rows...); err != nil {
						t.Fatal(err)
					}
				}
				if err := db.Compact(); err != nil {
					t.Fatal(err)
				}

				deadline := time.Now().Add(40 * time.Millisecond)
				var stop atomic.Bool
				errCh := make(chan error, 32)
				recordErr := func(err error) {
					if err != nil {
						select {
						case errCh <- err:
						default:
						}
					}
				}

				var wg sync.WaitGroup
				wg.Go(func() {
					step := 0
					for time.Now().Before(deadline) {
						step++
						ts := time.Unix(clockSec.Add(60), 0).UTC()
						rows := make([]Row, 30)
						for i := range rows {
							rows[i] = Row{
								Tags:   []string{fmt.Sprintf("dom-%02d", (step+i)%20), fmt.Sprintf("ip-%05d", step*30+i)},
								Values: [4]int64{int64(i + 1), 1, int64(i + 5), int64(step*100 + i)},
								Mask:   0x0f,
							}
						}
						if err := s.Append(ts, Summary{Values: [4]int64{30, 1, 35, int64(step * 100)}, Mask: 0x0f}, rows...); err != nil {
							recordErr(err)
							return
						}
						if step%3 == 0 {
							if err := db.Compact(); err != nil {
								recordErr(err)
								return
							}
						}
					}
					stop.Store(true)
				})

				for rID := range 3 {
					wg.Add(1)
					go func(id int) {
						defer wg.Done()
						iter := 0
						for !stop.Load() {
							iter++
							switch (iter + id) % 4 {
							case 0:
								p, err := s.Read(ctx, ReadRequest{AllTime: true, Limit: 50})
								if err != nil {
									recordErr(err)
									return
								}
								p.Release()
							case 1:
								if err := s.Scan(ctx, ScanQuery{AllTime: true, BatchSize: 64, Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"dom-03"}}}}, func(ScanBatch) error { return nil }); err != nil {
									recordErr(err)
									return
								}
							case 2:
								agg, err := s.Aggregate(ctx, AggregateQuery{AllTime: true, GroupDims: []int{0}})
								if err != nil {
									recordErr(err)
									return
								}
								agg.Release()
							case 3:
								agg, err := s.Aggregate(ctx, AggregateQuery{AllTime: true, GroupDims: []int{1}, TopK: 15})
								if err != nil {
									recordErr(err)
									return
								}
								agg.Release()
							}
							if used, maxB := dictCacheStats(db); maxB > 0 && used > maxB {
								recordErr(fmt.Errorf("dictPageCache overflow: used=%d max=%d", used, maxB))
								return
							}
						}
					}(rID)
				}

				wg.Go(func() {
					for !stop.Load() {
						client.Reclaim()
						time.Sleep(10 * time.Millisecond)
					}
				})

				wg.Wait()
				close(errCh)
				for err := range errCh {
					t.Fatal(err)
				}
				client.Reclaim()
				if used, _ := dictCacheStats(db); used != 0 {
					t.Fatalf("dictPageCache used=%d after Reclaim, want 0", used)
				}
				_ = client.Close()
				_ = db.Close()
			})
		}
	}
}

func TestInferredSeriesConfigRefreshRace(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	dbPath := filepath.Join(t.TempDir(), "inferred-refresh.db")
	ops := [4]MetricOp{OpSum, OpMin, OpMax, OpLast}
	cfg := SeriesConfig{Arity: 2, SummaryOps: ops, RowOps: ops}

	wdb, err := Open(dbPath, Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer wdb.Close()
	ws, _ := wdb.Series(1, cfg)
	if err := ws.Append(now.Add(-5*time.Minute), Summary{Values: [4]int64{10, 2, 8, 5}, Mask: 0x0f},
		Row{Tags: []string{"g-1", "k-1"}, Values: [4]int64{10, 2, 8, 5}, Mask: 0x0f},
	); err != nil {
		t.Fatal(err)
	}
	if err := wdb.Sync(); err != nil {
		t.Fatal(err)
	}

	roClient, err := OpenClient(dbPath, ClientOptions{DB: Options{ReadOnly: true, Now: func() time.Time { return now }}})
	if err != nil {
		t.Fatal(err)
	}
	defer roClient.Close()
	p, err := roClient.Read(ctx, 1, ReadRequest{AllTime: true, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	p.Release()
	roDB := roClient.DB()
	roDB.mu.RLock()
	inferredBefore := roDB.series[1] != nil && roDB.series[1].configInferred
	roDB.mu.RUnlock()
	if !inferredBefore {
		t.Fatal("expected read-only client to cache configInferred=true series before Compact")
	}

	if err := wdb.Compact(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	errCh := make(chan error, 16)
	recErr := func(err error) {
		if err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}
	wg.Go(func() {
		<-start
		for range 20 {
			recErr(roClient.Refresh())
		}
	})
	for g := range 3 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			for range 20 {
				if id == 0 {
					if _, err := roClient.Series(1, cfg); err != nil {
						recErr(err)
						return
					}
				}
				if err := roClient.WithPage(ctx, 1, ReadRequest{AllTime: true, Limit: 10}, nil); err != nil {
					recErr(err)
					return
				}
				if err := roClient.Scan(ctx, 1, ScanQuery{AllTime: true}, func(ScanBatch) error { return nil }); err != nil {
					recErr(err)
					return
				}
				if err := roClient.WithAggregate(ctx, 1, AggregateQuery{AllTime: true, GroupDims: []int{0}}, nil); err != nil {
					recErr(err)
					return
				}
				if err := roClient.WithAggregate(ctx, 1, AggregateQuery{AllTime: true}, nil); err != nil {
					recErr(err)
					return
				}
			}
		}(g)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestQueryFilterCompileDoesNotBlockAppendAndCompact(t *testing.T) {
	ctx := context.Background()
	ops := [4]MetricOp{OpSum, OpMin, OpMax, OpLast}
	baseNow := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

	for _, useMmap := range []bool{false, true} {
		t.Run(map[bool]string{false: "readat", true: "mmap"}[useMmap], func(t *testing.T) {
			var clockSec atomic.Int64
			clockSec.Store(baseNow.Unix())

			db, err := Open(filepath.Join(t.TempDir(), "filter-lockfree.db"), Options{
				Now:     func() time.Time { return time.Unix(clockSec.Load(), 0).UTC() },
				UseMmap: useMmap,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			s, err := db.Series(9, SeriesConfig{Arity: 3, SummaryOps: ops, RowOps: ops})
			if err != nil {
				t.Fatal(err)
			}

			const extCount = 17000
			seedTS := baseNow.Add(-2 * time.Hour)
			rows := make([]Row, 0, extCount+1)
			for i := range extCount {
				rows = append(rows, Row{
					Tags: []string{
						fmt.Sprintf("svc-%05d.prod.local", i),
						fmt.Sprintf("10.%d.%d.%d", (i>>16)&0xff, (i>>8)&0xff, i&0xff),
						fmt.Sprintf("app-%03d", i%150),
					},
					Values: [4]int64{1, 1, 1, 1},
					Mask:   0x0f,
				})
			}
			rows = append(rows, Row{
				Tags:   []string{"anchor-beacon.edge.net", "198.51.100.42", "app-anchor"},
				Values: [4]int64{7, 7, 7, 7},
				Mask:   0x0f,
			})
			if err := s.Append(seedTS, Summary{Values: [4]int64{extCount + 7, 1, 7, 7}, Mask: 0x0f}, rows...); err != nil {
				t.Fatal(err)
			}
			if err := db.Compact(); err != nil {
				t.Fatal(err)
			}

			db.mu.RLock()
			dictCodec := db.head.sections[secTSDict].codec
			dim0 := db.dict.dims[dictDimKey{series: 9, dim: 0}]
			dim1 := db.dict.dims[dictDimKey{series: 9, dim: 1}]
			dim2 := db.dict.dims[dictDimKey{series: 9, dim: 2}]
			hasExt0 := dim0 != nil && dim0.ext != nil
			hasExt1 := dim1 != nil && dim1.ext != nil
			inline2OK := dim2 != nil && dim2.ext == nil && dim2.count == 151
			mutID := db.dict.lookup(9, 0, "svc-00000.prod.local")
			mutInlineID := db.dict.lookup(9, 2, "app-149")
			db.mu.RUnlock()

			if dictCodec != 2 || !hasExt0 || !hasExt1 || !inline2OK || mutID == 0 || mutInlineID == 0 {
				t.Fatalf("unexpected dict layout: codec=%d ext0=%t ext1=%t inline2=%t mutID=%d mutInlineID=%d",
					dictCodec, hasExt0, hasExt1, inline2OK, mutID, mutInlineID)
			}

			// Trigger lazy sorted index on inline dim 2 before testing extra override against sorted.
			if agg, err := s.Aggregate(ctx, AggregateQuery{
				AllTime: true,
				Filters: []Filter{{Dim: 2, Op: TagEq, Values: []string{"app-anchor"}}},
			}); err != nil {
				t.Fatal(err)
			} else {
				got := agg.Summary.Values[0]
				agg.Release()
				if got != 7 {
					t.Fatalf("pre-override inline TagEq got=%d want=7", got)
				}
			}

			// Verify 3-tier override priority (extra > tail > ext / extra > sorted) across lock-free filter ops.
			db.mu.Lock()
			db.dict.apply(dictEntry{id: mutID, series: 9, dim: 0, value: "override-init.alt.org"})
			db.dict.apply(dictEntry{id: mutInlineID, series: 9, dim: 2, value: "app-override"})
			db.mu.Unlock()
			for _, tc := range []struct {
				name   string
				filter Filter
				want   int64
			}{
				{"ext-overridden-old-eq", Filter{Dim: 0, Op: TagEq, Values: []string{"svc-00000.prod.local"}}, 0},
				{"ext-override-new-eq", Filter{Dim: 0, Op: TagEq, Values: []string{"override-init.alt.org"}}, 1},
				{"ext-override-new-prefix", Filter{Dim: 0, Op: TagPrefix, Values: []string{"override-init"}}, 1},
				{"ext-override-new-suffix", Filter{Dim: 0, Op: TagSuffix, Values: []string{".alt.org"}}, 1},
				{"ext-override-new-contains", Filter{Dim: 0, Op: TagContains, Values: []string{"override-init"}}, 1},
				{"inline-overridden-old-eq", Filter{Dim: 2, Op: TagEq, Values: []string{"app-149"}}, 0},
				{"inline-overridden-old-prefix", Filter{Dim: 2, Op: TagPrefix, Values: []string{"app-149"}}, 0},
				{"inline-override-new-eq", Filter{Dim: 2, Op: TagEq, Values: []string{"app-override"}}, 113},
				{"inline-override-new-prefix", Filter{Dim: 2, Op: TagPrefix, Values: []string{"app-over"}}, 113},
				{"inline-override-new-contains", Filter{Dim: 2, Op: TagContains, Values: []string{"override"}}, 113},
			} {
				agg, err := s.Aggregate(ctx, AggregateQuery{AllTime: true, Filters: []Filter{tc.filter}})
				if err != nil {
					t.Fatalf("%s: %v", tc.name, err)
				}
				got := agg.Summary.Values[0]
				agg.Release()
				if got != tc.want {
					t.Fatalf("%s: got sum=%d want=%d", tc.name, got, tc.want)
				}
			}

			var stopReaders atomic.Bool
			var filterReads atomic.Int64
			readersActive := make(chan struct{})
			var signalActive sync.Once
			errCh := make(chan error, 32)
			recErr := func(err error) {
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					signalActive.Do(func() { close(readersActive) })
				}
			}

			var readerWG sync.WaitGroup
			for rID := range 4 {
				readerWG.Add(1)
				go func(id int) {
					defer readerWG.Done()
					defer signalActive.Do(func() { close(readersActive) })
					iter := 0
					for !stopReaders.Load() {
						iter++
						switch (iter + id) % 6 {
						case 0:
							agg, err := s.Aggregate(ctx, AggregateQuery{
								AllTime: true,
								Filters: []Filter{{Dim: 0, Op: TagContains, Values: []string{"anchor-beacon"}}},
							})
							if err != nil {
								recErr(err)
								return
							}
							got := agg.Summary.Values[0]
							agg.Release()
							if got != 7 {
								recErr(fmt.Errorf("TagContains anchor mismatch: got=%d want=7", got))
								return
							}
						case 1:
							page, err := s.Read(ctx, ReadRequest{
								AllTime: true,
								Limit:   10,
								Filters: []Filter{{Dim: 0, Op: TagSuffix, Values: []string{".edge.net"}}},
							})
							if err != nil {
								recErr(err)
								return
							}
							ok := len(page.Rows) == 1 && page.Rows[0].Tags[0] == "anchor-beacon.edge.net"
							page.Release()
							if !ok {
								recErr(fmt.Errorf("TagSuffix anchor row mismatch"))
								return
							}
						case 2:
							var matched int
							var sumV0 int64
							err := s.Scan(ctx, ScanQuery{
								AllTime:   true,
								BatchSize: 32,
								Filters:   []Filter{{Dim: 1, Op: TagCIDR, Values: []string{"198.51.100.0/24"}}},
							}, func(b ScanBatch) error {
								for i := range b.Len() {
									matched++
									sumV0 += b.Values(i)[0]
								}
								return nil
							})
							if err != nil {
								recErr(err)
								return
							}
							if matched != 1 || sumV0 != 7 {
								recErr(fmt.Errorf("TagCIDR anchor mismatch: matched=%d sumV0=%d", matched, sumV0))
								return
							}
						case 3:
							agg, err := s.Aggregate(ctx, AggregateQuery{
								AllTime: true,
								Filters: []Filter{
									{Dim: 0, Op: TagPrefix, Values: []string{"svc-0010"}},
									{Dim: 2, Op: TagEq, Values: []string{"app-anchor"}, Negate: true},
								},
							})
							if err != nil {
								recErr(err)
								return
							}
							got := agg.Summary.Values[0]
							agg.Release()
							if got != 10 {
								recErr(fmt.Errorf("TagPrefix+Negate mismatch: got=%d want=10", got))
								return
							}
						case 4:
							page, err := s.Read(ctx, ReadRequest{
								AllTime: true,
								Limit:   10,
								Filters: []Filter{{Dim: 2, Op: TagEq, Values: []string{"app-anchor"}}},
							})
							if err != nil {
								recErr(err)
								return
							}
							ok := len(page.Rows) == 1 && page.Rows[0].Values[0] == 7
							page.Release()
							if !ok {
								recErr(fmt.Errorf("inline TagEq anchor mismatch"))
								return
							}
						case 5:
							agg, err := s.Aggregate(ctx, AggregateQuery{
								AllTime:   true,
								GroupDims: []int{2},
								Filters:   []Filter{{Dim: 2, Op: TagPrefix, Values: []string{"app-00"}}},
							})
							if err != nil {
								recErr(err)
								return
							}
							gotSum := agg.Summary.Values[0]
							gotGroups := len(agg.Groups[0])
							agg.Release()
							if gotSum != 1140 || gotGroups != 10 {
								recErr(fmt.Errorf("inline TagPrefix group mismatch: sum=%d groups=%d", gotSum, gotGroups))
								return
							}
						}
						if filterReads.Add(1) >= 2 {
							signalActive.Do(func() { close(readersActive) })
						}
					}
				}(rID)
			}

			<-readersActive
			readsBeforePhase1 := filterReads.Load()

			// Phase 1: sustained Appends (tail inserts + inline mergeTailIntoSorted + extra updates)
			// must make steady progress concurrently with heavy high-cardinality filter scans.
			const phase1Appends = 80
			var maxAppend time.Duration
			for step := range phase1Appends {
				ts := time.Unix(clockSec.Add(60), 0).UTC()
				row := Row{
					Tags: []string{
						fmt.Sprintf("tail-p1-%03d.dyn.local", step),
						fmt.Sprintf("10.2.0.%d", step%250),
						fmt.Sprintf("app-dyn-%03d", step),
					},
					Values: [4]int64{3, 3, 3, 3},
					Mask:   0x0f,
				}
				start := time.Now()
				err := s.Append(ts, Summary{Values: [4]int64{3, 3, 3, 3}, Mask: 0x0f}, row)
				elapsed := time.Since(start)
				if err != nil {
					recErr(err)
					break
				}
				if elapsed > maxAppend {
					maxAppend = elapsed
				}
				if step%10 == 0 {
					db.mu.Lock()
					db.dict.apply(dictEntry{id: mutID, series: 9, dim: 0, value: fmt.Sprintf("override-step-%03d.alt.org", step)})
					db.mu.Unlock()
				}
			}
			if maxAppend > 2*time.Second {
				recErr(fmt.Errorf("Append blocked too long during concurrent filter scans: max=%v", maxAppend))
			}

			// Phase 2: 3-way concurrency with FilterQueries + Append + Compact.
			const phase2Appends = 40
			var compactWG sync.WaitGroup
			compactWG.Go(func() {
				for range 2 {
					if err := db.Compact(); err != nil {
						recErr(err)
						return
					}
				}
			})
			for step := range phase2Appends {
				ts := time.Unix(clockSec.Add(60), 0).UTC()
				row := Row{
					Tags: []string{
						fmt.Sprintf("tail-p2-%03d.dyn.local", step),
						fmt.Sprintf("10.2.1.%d", step%250),
						fmt.Sprintf("app-dyn-%03d", phase1Appends+step),
					},
					Values: [4]int64{3, 3, 3, 3},
					Mask:   0x0f,
				}
				if err := s.Append(ts, Summary{Values: [4]int64{3, 3, 3, 3}, Mask: 0x0f}, row); err != nil {
					recErr(err)
					break
				}
			}
			compactWG.Wait()
			stopReaders.Store(true)
			readerWG.Wait()

			close(errCh)
			for err := range errCh {
				t.Fatal(err)
			}
			if filterReads.Load() <= readsBeforePhase1 {
				t.Fatalf("expected concurrent filter reads during writes/compacts: before=%d after=%d",
					readsBeforePhase1, filterReads.Load())
			}

			// Post-stress deterministic accuracy checks across all 5 filter operators.
			for _, tc := range []struct {
				name   string
				filter Filter
				want   int64
			}{
				{"tail-p1-prefix", Filter{Dim: 0, Op: TagPrefix, Values: []string{"tail-p1-"}}, phase1Appends * 3},
				{"tail-p2-prefix", Filter{Dim: 0, Op: TagPrefix, Values: []string{"tail-p2-"}}, phase2Appends * 3},
				{"dyn-inline-prefix", Filter{Dim: 2, Op: TagPrefix, Values: []string{"app-dyn-"}}, (phase1Appends + phase2Appends) * 3},
				{"dyn-cidr", Filter{Dim: 1, Op: TagCIDR, Values: []string{"10.2.0.0/23"}}, (phase1Appends + phase2Appends) * 3},
				{"anchor-eq", Filter{Dim: 0, Op: TagEq, Values: []string{"anchor-beacon.edge.net"}}, 7},
			} {
				agg, err := s.Aggregate(ctx, AggregateQuery{AllTime: true, Filters: []Filter{tc.filter}})
				if err != nil {
					t.Fatalf("%s: %v", tc.name, err)
				}
				got := agg.Summary.Values[0]
				agg.Release()
				if got != tc.want {
					t.Fatalf("%s: got sum=%d want=%d", tc.name, got, tc.want)
				}
			}

			// Concurrent queries + Close() under -race, followed by deterministic closed-DB checks.
			cursorFilter := []Filter{{Dim: 0, Op: TagPrefix, Values: []string{"svc-0010"}}}
			cursorPage, err := s.Read(ctx, ReadRequest{AllTime: true, Limit: 1, Filters: cursorFilter})
			if err != nil || !cursorPage.HasMore || cursorPage.NextCursor.IsZero() {
				t.Fatalf("failed to obtain cursor before close: err=%v hasMore=%t", err, cursorPage != nil && cursorPage.HasMore)
			}
			savedCursor := cursorPage.NextCursor
			cursorPage.Release()

			var closing atomic.Bool
			var closeStarted atomic.Int32
			var closedSeen atomic.Int32
			var stopClose atomic.Bool
			closeReady := make(chan struct{})
			var sigClose sync.Once
			closeErrCh := make(chan error, 16)
			recCloseErr := func(err error) {
				select {
				case closeErrCh <- err:
				default:
				}
				stopClose.Store(true)
				sigClose.Do(func() { close(closeReady) })
			}

			var closeWG sync.WaitGroup
			for g := range 3 {
				closeWG.Add(1)
				go func(id int) {
					defer closeWG.Done()
					defer sigClose.Do(func() { close(closeReady) })
					iter := 0
					for !stopClose.Load() {
						iter++
						var qErr error
						switch (iter + id) % 4 {
						case 0:
							agg, err := s.Aggregate(ctx, AggregateQuery{
								AllTime: true,
								Filters: []Filter{{Dim: 0, Op: TagContains, Values: []string{"anchor-beacon"}}},
							})
							if err == nil {
								agg.Release()
							}
							qErr = err
						case 1:
							page, err := s.Read(ctx, ReadRequest{
								AllTime: true,
								Limit:   10,
								Filters: []Filter{{Dim: 0, Op: TagSuffix, Values: []string{".edge.net"}}},
							})
							if err == nil {
								page.Release()
							}
							qErr = err
						case 2:
							qErr = s.Scan(ctx, ScanQuery{
								AllTime:   true,
								BatchSize: 32,
								Filters:   []Filter{{Dim: 1, Op: TagCIDR, Values: []string{"198.51.100.0/24"}}},
							}, func(ScanBatch) error { return nil })
						case 3:
							agg, err := s.Aggregate(ctx, AggregateQuery{AllTime: true})
							if err == nil {
								agg.Release()
							}
							qErr = err
						}
						if qErr != nil {
							if !closing.Load() {
								recCloseErr(fmt.Errorf("unexpected error before Close: %w", qErr))
								return
							}
							if closedSeen.Add(1) >= 6 {
								stopClose.Store(true)
								return
							}
							continue
						}
						if closeStarted.Add(1) >= 2 {
							sigClose.Do(func() { close(closeReady) })
						}
					}
				}(g)
			}

			<-closeReady
			closing.Store(true)
			if err := db.Close(); err != nil {
				stopClose.Store(true)
				closeWG.Wait()
				t.Fatal(err)
			}
			closeWG.Wait()
			close(closeErrCh)
			for err := range closeErrCh {
				t.Fatal(err)
			}

			if agg, err := s.Aggregate(ctx, AggregateQuery{
				AllTime: true,
				Filters: []Filter{{Dim: 0, Op: TagContains, Values: []string{"anchor-beacon"}}},
			}); err == nil {
				agg.Release()
				t.Fatal("expected error on filtered Aggregate after Close")
			}
			if agg, err := s.Aggregate(ctx, AggregateQuery{AllTime: true}); err == nil {
				agg.Release()
				t.Fatal("expected error on SummaryOnly Aggregate after Close")
			}
			if page, err := s.Read(ctx, ReadRequest{
				AllTime: true,
				Limit:   10,
				Filters: []Filter{{Dim: 0, Op: TagSuffix, Values: []string{".edge.net"}}},
			}); err == nil {
				page.Release()
				t.Fatal("expected error on filtered Read after Close")
			}
			if page, err := s.Read(ctx, ReadRequest{Cursor: savedCursor, Limit: 1, Filters: cursorFilter}); err == nil {
				page.Release()
				t.Fatal("expected error on cursor Read after Close")
			}
			if err := s.Scan(ctx, ScanQuery{
				AllTime: true,
				Filters: []Filter{{Dim: 1, Op: TagCIDR, Values: []string{"198.51.100.0/24"}}},
			}, func(ScanBatch) error { return nil }); err == nil {
				t.Fatal("expected error on filtered Scan after Close")
			}
		})
	}
}
