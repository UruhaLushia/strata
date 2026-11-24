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
