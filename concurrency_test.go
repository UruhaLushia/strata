package strata

import (
	"context"
	"errors"
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

const (
	backgroundTestInterval = 5 * time.Millisecond
	backgroundTestTimeout  = 2 * time.Second
)

func TestConcurrentStartClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "strata.db")
	opts := Options{FlushInterval: time.Hour, SyncInterval: time.Hour}
	for range 16 {
		db, err := Open(path, opts)
		if err != nil {
			t.Fatal(err)
		}
		ctx := t.Context()
		db.Start(ctx)
		done := backgroundLoopDone(db)
		db.Start(ctx)
		if done == nil || backgroundLoopDone(db) != done {
			t.Fatal("Start did not keep exactly one loop")
		}

		var wg sync.WaitGroup
		start := make(chan struct{})
		for range 8 {
			wg.Go(func() {
				<-start
				for range 16 {
					db.Start(ctx)
					runtime.Gosched()
				}
			})
		}
		for range 2 {
			wg.Go(func() {
				<-start
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		close(start)
		wg.Wait()

		select {
		case <-done:
		default:
			t.Fatal("Close returned before the loop stopped")
		}
		db.Start(ctx)
		assertNoBackgroundLoop(t, db)
	}
}

func TestStartSkipsInactiveDB(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	opts := Options{FlushInterval: time.Hour, SyncInterval: time.Hour}
	path := filepath.Join(t.TempDir(), "strata.db")
	closed, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := Open(path, Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, tc := range []struct {
		name string
		db   *DB
		ctx  context.Context
	}{
		{"read-only", reader, t.Context()},
		{"closed", closed, t.Context()},
		{"canceled", &DB{opts: opts, writer: true}, canceled},
		{"disabled", &DB{writer: true}, t.Context()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.db.Start(tc.ctx)
			assertNoBackgroundLoop(t, tc.db)
		})
	}
}

func TestStartRestartsAfterCancel(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(t.Context())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()
	db, s, now, release := startBlockedAutoCompact(t, ctx1)
	done := backgroundLoopDone(db)
	started := make(chan struct{})
	go func() {
		cancel1()
		db.Start(ctx2)
		cancel2()
		db.Start(context.Background())
		close(started)
	}()
	waitForBackgroundSignal(t, started, "Start waited for the canceled loop to exit")
	release()
	waitForBackgroundContext(t, db, done, nil)
	assertBackgroundFlush(t, db, s, now)
}

func TestStartReentersFromNow(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(t.Context())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()
	now := time.Now()
	db, s := testDB(t, &now, Options{FlushInterval: backgroundTestInterval, SyncInterval: time.Hour})
	defer closeBackgroundTestDB(t, db)

	returned := make(chan struct{})
	db.opts.Now = func() time.Time {
		select {
		case <-returned:
		default:
			cancel1()
			db.Start(ctx2)
			close(returned)
		}
		return now
	}
	db.Start(ctx1)
	done := backgroundLoopDone(db)
	waitForBackgroundSignal(t, returned, "Start did not return in the Now callback")
	waitForBackgroundContext(t, db, done, ctx2.Done())
	assertBackgroundFlush(t, db, s, now)

	closeBackgroundTestDB(t, db)
	select {
	case <-done:
	default:
		t.Fatal("Close returned before the loop stopped")
	}
	assertNoBackgroundLoop(t, db)
}

func TestConcurrentCloseWaitsForBackgroundLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	db, _, _, release := startBlockedAutoCompact(t, ctx)
	done := backgroundLoopDone(db)
	started := make(chan struct{})
	go func() {
		cancel()
		db.Start(context.Background())
		close(started)
	}()
	waitForBackgroundSignal(t, started, "Start did not register a handoff during compaction")

	db.loopMu.Lock()
	stop := db.loopStop
	db.loopMu.Unlock()
	closed := make(chan error, 2)
	closeDB := func() {
		err := db.Close()
		if err == nil {
			select {
			case <-done:
			default:
				err = errors.New("Close returned before the loop stopped")
			}
		}
		closed <- err
	}
	go closeDB()
	waitForBackgroundSignal(t, stop, "the first Close did not signal the loop to stop")

	second := make(chan struct{})
	go func() {
		close(second)
		closeDB()
	}()
	waitForBackgroundSignal(t, second, "the second Close did not start")
	select {
	case err := <-closed:
		t.Fatalf("Close returned while background compaction was blocked: %v", err)
	case <-time.After(backgroundTestInterval):
	}
	release()
	for range 2 {
		select {
		case err := <-closed:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(backgroundTestTimeout):
			t.Fatal("Close did not return after compaction was released")
		}
	}
	assertNoBackgroundLoop(t, db)
}

func startBlockedAutoCompact(t *testing.T, ctx context.Context) (*DB, *Series, time.Time, func()) {
	t.Helper()
	now := time.Now()
	db, s := testDB(t, &now, Options{FlushInterval: backgroundTestInterval, SyncInterval: time.Hour, WALCompactBytes: 1})
	entered, unblock := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	t.Cleanup(func() {
		release()
		closeBackgroundTestDB(t, db)
	})
	db.compactHook = func() {
		select {
		case <-unblock:
			return
		default:
		}
		close(entered)
		<-unblock
	}
	if err := s.Append(now.Add(-time.Hour), Summary{Values: [4]int64{1}}); err != nil {
		t.Fatal(err)
	}
	db.Start(ctx)
	waitForBackgroundSignal(t, entered, "background compaction did not reach the hook")
	return db, s, now, release
}

func TestBackgroundLoopStopsOnCompactWriteDisable(t *testing.T) {
	now := time.Now()
	db, _ := testDB(t, &now, Options{FlushInterval: time.Hour, SyncInterval: time.Hour})
	defer closeBackgroundTestDB(t, db)
	ctx := t.Context()

	// A required unknown section is written successfully and rejected when reloaded.
	db.optional = []optionalSection{{desc: sectionDesc{kind: 7}, data: []byte{1}, slot: 7}}
	db.Start(ctx)
	done := backgroundLoopDone(db)
	if done == nil {
		t.Fatal("Start did not register a loop")
	}
	if err := db.Compact(); !errors.Is(err, ErrIncompatFormat) {
		t.Fatalf("Compact reload error = %v, want %v", err, ErrIncompatFormat)
	}
	if err := db.Flush(); err != ErrReadOnly {
		t.Fatalf("Flush after write disable = %v, want %v", err, ErrReadOnly)
	}
	waitForBackgroundSignal(t, done, "loop did not stop before the next tick")
	db.Start(ctx)
	assertNoBackgroundLoop(t, db)
}

type startContext struct {
	context.Context
	done func() <-chan struct{}
}

func (c startContext) Done() <-chan struct{} { return c.done() }

func TestStartContextDoneCanReenter(t *testing.T) {
	now := time.Now()
	db, _ := testDB(t, &now, Options{FlushInterval: time.Hour, SyncInterval: time.Hour})
	defer db.Close()

	var done <-chan struct{}
	db.Start(startContext{Context: context.Background(), done: func() <-chan struct{} {
		if !db.loopMu.TryLock() {
			t.Fatal("Start called Context.Done with loopMu held")
		}
		db.loopMu.Unlock()
		if !db.mu.TryLock() {
			t.Fatal("Start called Context.Done with mu held")
		}
		db.mu.Unlock()
		var nilCtx context.Context
		db.Start(nilCtx)
		done = backgroundLoopDone(db)
		return nil
	}})
	if done == nil || backgroundLoopDone(db) != done {
		t.Fatal("reentrant Start did not keep exactly one loop")
	}
}

func backgroundLoopDone(db *DB) <-chan struct{} {
	db.loopMu.Lock()
	defer db.loopMu.Unlock()
	return db.loopDone
}

func waitForBackgroundSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(backgroundTestTimeout):
		t.Fatal(message)
	}
}

func waitForBackgroundCondition(t *testing.T, message string, ready func() bool) {
	t.Helper()
	tick := time.NewTicker(backgroundTestInterval)
	defer tick.Stop()
	timeout := time.After(backgroundTestTimeout)
	for !ready() {
		select {
		case <-tick.C:
		case <-timeout:
			t.Fatal(message)
		}
	}
}

func waitForBackgroundContext(t *testing.T, db *DB, done, ctxDone <-chan struct{}) {
	t.Helper()
	waitForBackgroundCondition(t, "the loop did not hand off to the requested context", func() bool {
		db.loopMu.Lock()
		defer db.loopMu.Unlock()
		return done != nil && db.loopDone == done && db.loopCtxDone == ctxDone && !db.loopHasNext
	})
}

func assertBackgroundFlush(t *testing.T, db *DB, s *Series, now time.Time) {
	t.Helper()
	db.mu.RLock()
	before := db.walPos
	db.mu.RUnlock()
	if err := s.Append(now, Summary{Values: [4]int64{1}}); err != nil {
		t.Fatal(err)
	}
	waitForBackgroundCondition(t, "restarted loop did not flush pending data to WAL", func() bool {
		db.mu.RLock()
		defer db.mu.RUnlock()
		return db.walPos > before && len(db.pending) == 0
	})
}

func closeBackgroundTestDB(t *testing.T, db *DB) {
	t.Helper()
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(backgroundTestTimeout):
		t.Error("Close did not return")
	}
}

func assertNoBackgroundLoop(t *testing.T, db *DB) {
	t.Helper()
	db.loopMu.Lock()
	defer db.loopMu.Unlock()
	if db.loopDone != nil || db.loopStop != nil || db.loopCtxDone != nil || db.loopHasNext || db.loopNextCtxDone != nil {
		t.Fatal("background loop is still registered")
	}
}

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
				errSink := newTestErrSink(32)

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
							errSink.record(err)
							return
						}
						if step%3 == 0 {
							if err := db.Compact(); err != nil {
								errSink.record(err)
								return
							}
						}
					}
					stop.Store(true)
				})

				for id := range 3 {
					wg.Go(func() {
						iter := 0
						for !stop.Load() {
							iter++
							var qErr error
							switch (iter + id) % 4 {
							case 0:
								qErr = readCheckErr(ctx, s, ReadRequest{AllTime: true, Limit: 50}, nil)
							case 1:
								qErr = s.Scan(ctx, ScanQuery{AllTime: true, BatchSize: 64, Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"dom-03"}}}}, func(ScanBatch) error { return nil })
							case 2:
								_, qErr = aggSumErr(ctx, s, AggregateQuery{AllTime: true, GroupDims: []int{0}})
							case 3:
								_, qErr = aggSumErr(ctx, s, AggregateQuery{AllTime: true, GroupDims: []int{1}, TopK: 15})
							}
							if qErr != nil {
								errSink.record(qErr)
								return
							}
							if used, maxB := dictCacheStats(db); maxB > 0 && used > maxB {
								errSink.record(fmt.Errorf("dictPageCache overflow: used=%d max=%d", used, maxB))
								return
							}
						}
					})
				}

				wg.Go(func() {
					for !stop.Load() {
						client.Reclaim()
						time.Sleep(10 * time.Millisecond)
					}
				})

				wg.Wait()
				errSink.check(t)
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

	if err := runConcurrentWorkers(4, func(id int) error {
		if id == 3 {
			for range 20 {
				if err := roClient.Refresh(); err != nil {
					return err
				}
			}
			return nil
		}
		for range 20 {
			if id == 0 {
				if _, err := roClient.Series(1, cfg); err != nil {
					return err
				}
			}
			for _, fn := range []func() error{
				func() error { return roClient.WithPage(ctx, 1, ReadRequest{AllTime: true, Limit: 10}, nil) },
				func() error {
					return roClient.Scan(ctx, 1, ScanQuery{AllTime: true}, func(ScanBatch) error { return nil })
				},
				func() error {
					return roClient.WithAggregate(ctx, 1, AggregateQuery{AllTime: true, GroupDims: []int{0}}, nil)
				},
				func() error { return roClient.WithAggregate(ctx, 1, AggregateQuery{AllTime: true}, nil) },
			} {
				if err := fn(); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
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
			seedHighCardMinute(t, s, seedTS, extCount, func(i int) []string {
				return []string{
					fmt.Sprintf("svc-%05d.prod.local", i),
					fmt.Sprintf("10.%d.%d.%d", (i>>16)&0xff, (i>>8)&0xff, i&0xff),
					fmt.Sprintf("app-%03d", i%150),
				}
			})
			if err := s.Append(seedTS, Summary{Values: [4]int64{7, 7, 7, 7}, Mask: 0x0f}, Row{
				Tags:   []string{"anchor-beacon.edge.net", "198.51.100.42", "app-anchor"},
				Values: [4]int64{7, 7, 7, 7},
				Mask:   0x0f,
			}); err != nil {
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

			if dictCodec != 1 || !hasExt0 || !hasExt1 || !inline2OK || mutID == 0 || mutInlineID == 0 {
				t.Fatalf("unexpected dict layout: codec=%d ext0=%t ext1=%t inline2=%t mutID=%d mutInlineID=%d",
					dictCodec, hasExt0, hasExt1, inline2OK, mutID, mutInlineID)
			}

			// Trigger lazy sorted index on inline dim 2 before testing extra override against sorted.
			if got := queryAggSum(t, s, AggregateQuery{
				AllTime: true,
				Filters: []Filter{{Dim: 2, Op: TagEq, Values: []string{"app-anchor"}}},
			}); got != 7 {
				t.Fatalf("pre-override inline TagEq got=%d want=7", got)
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
				if got := queryAggSum(t, s, AggregateQuery{AllTime: true, Filters: []Filter{tc.filter}}); got != tc.want {
					t.Fatalf("%s: got sum=%d want=%d", tc.name, got, tc.want)
				}
			}

			var stopReaders atomic.Bool
			var filterReads atomic.Int64
			readersActive := make(chan struct{})
			var signalActive sync.Once
			errSink := newTestErrSink(32)
			recErr := func(err error) {
				if err != nil {
					errSink.record(err)
					signalActive.Do(func() { close(readersActive) })
				}
			}

			var readerWG sync.WaitGroup
			for id := range 4 {
				readerWG.Go(func() {
					defer signalActive.Do(func() { close(readersActive) })
					iter := 0
					for !stopReaders.Load() {
						iter++
						switch (iter + id) % 6 {
						case 0:
							if got, err := aggSumErr(ctx, s, AggregateQuery{
								AllTime: true,
								Filters: []Filter{{Dim: 0, Op: TagContains, Values: []string{"anchor-beacon"}}},
							}); err != nil {
								recErr(err)
								return
							} else if got != 7 {
								recErr(fmt.Errorf("TagContains anchor mismatch: got=%d want=7", got))
								return
							}
						case 1:
							if err := readCheckErr(ctx, s, ReadRequest{
								AllTime: true, Limit: 10,
								Filters: []Filter{{Dim: 0, Op: TagSuffix, Values: []string{".edge.net"}}},
							}, func(page *PageResult) error {
								if len(page.Rows) != 1 || page.Rows[0].Tags[0] != "anchor-beacon.edge.net" {
									return fmt.Errorf("TagSuffix anchor row mismatch")
								}
								return nil
							}); err != nil {
								recErr(err)
								return
							}
						case 2:
							if matched, sumV0, err := scanCountSumErr(ctx, s, ScanQuery{
								AllTime: true, BatchSize: 32,
								Filters: []Filter{{Dim: 1, Op: TagCIDR, Values: []string{"198.51.100.0/24"}}},
							}); err != nil {
								recErr(err)
								return
							} else if matched != 1 || sumV0 != 7 {
								recErr(fmt.Errorf("TagCIDR anchor mismatch: matched=%d sumV0=%d", matched, sumV0))
								return
							}
						case 3:
							if got, err := aggSumErr(ctx, s, AggregateQuery{
								AllTime: true,
								Filters: []Filter{
									{Dim: 0, Op: TagPrefix, Values: []string{"svc-0010"}},
									{Dim: 2, Op: TagEq, Values: []string{"app-anchor"}, Negate: true},
								},
							}); err != nil {
								recErr(err)
								return
							} else if got != 10 {
								recErr(fmt.Errorf("TagPrefix+Negate mismatch: got=%d want=10", got))
								return
							}
						case 4:
							if err := readCheckErr(ctx, s, ReadRequest{
								AllTime: true, Limit: 10,
								Filters: []Filter{{Dim: 2, Op: TagEq, Values: []string{"app-anchor"}}},
							}, func(page *PageResult) error {
								if len(page.Rows) != 1 || page.Rows[0].Values[0] != 7 {
									return fmt.Errorf("inline TagEq anchor mismatch")
								}
								return nil
							}); err != nil {
								recErr(err)
								return
							}
						case 5:
							agg, err := s.Aggregate(ctx, AggregateQuery{
								AllTime: true, GroupDims: []int{2},
								Filters: []Filter{{Dim: 2, Op: TagPrefix, Values: []string{"app-00"}}},
							})
							if err != nil {
								recErr(err)
								return
							}
							gotSum, gotGroups := agg.Summary.Values[0], len(agg.Groups[0])
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
				})
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
			errSink.check(t)
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
				if got := queryAggSum(t, s, AggregateQuery{AllTime: true, Filters: []Filter{tc.filter}}); got != tc.want {
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
			var closeStarted, closedSeen atomic.Int32
			var stopClose atomic.Bool
			closeReady := make(chan struct{})
			var sigClose sync.Once
			closeErrSink := newTestErrSink(16)
			recCloseErr := func(err error) {
				closeErrSink.record(err)
				stopClose.Store(true)
				sigClose.Do(func() { close(closeReady) })
			}

			var closeWG sync.WaitGroup
			for id := range 3 {
				closeWG.Go(func() {
					defer sigClose.Do(func() { close(closeReady) })
					iter := 0
					for !stopClose.Load() {
						iter++
						var qErr error
						switch (iter + id) % 4 {
						case 0:
							_, qErr = aggSumErr(ctx, s, AggregateQuery{AllTime: true, Filters: []Filter{{Dim: 0, Op: TagContains, Values: []string{"anchor-beacon"}}}})
						case 1:
							qErr = readCheckErr(ctx, s, ReadRequest{AllTime: true, Limit: 10, Filters: []Filter{{Dim: 0, Op: TagSuffix, Values: []string{".edge.net"}}}}, nil)
						case 2:
							qErr = s.Scan(ctx, ScanQuery{AllTime: true, BatchSize: 32, Filters: []Filter{{Dim: 1, Op: TagCIDR, Values: []string{"198.51.100.0/24"}}}}, func(ScanBatch) error { return nil })
						case 3:
							_, qErr = aggSumErr(ctx, s, AggregateQuery{AllTime: true})
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
				})
			}

			<-closeReady
			closing.Store(true)
			if err := db.Close(); err != nil {
				stopClose.Store(true)
				closeWG.Wait()
				t.Fatal(err)
			}
			closeWG.Wait()
			closeErrSink.check(t)

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

func TestResetAllConcurrentQueryAppendCompact(t *testing.T) {
	ctx := context.Background()
	ops := [4]MetricOp{OpSum, OpMin, OpMax, OpLast}
	baseNow := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

	var clockSec atomic.Int64
	clockSec.Store(baseNow.Unix())

	db, err := Open(filepath.Join(t.TempDir(), "reset-all-race.db"), Options{
		Now:     func() time.Time { return time.Unix(clockSec.Load(), 0).UTC() },
		UseMmap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s1, err := db.Series(1, SeriesConfig{Arity: 2, SummaryOps: ops, RowOps: ops})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := db.Series(2, SeriesConfig{Arity: 1, SummaryOps: ops, RowOps: ops})
	if err != nil {
		t.Fatal(err)
	}

	const extCount = 17000
	seedTS := baseNow.Add(-2 * time.Hour)
	seedHighCardMinute(t, s1, seedTS, extCount, func(i int) []string {
		return []string{fmt.Sprintf("old-%05d.net", i), fmt.Sprintf("g-%02d", i%10)}
	})
	seedHighCardMinute(t, s2, seedTS, 200, func(i int) []string {
		return []string{fmt.Sprintf("keep-%03d", i)}
	})
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	initStats, err := s1.DictStats()
	if err != nil || len(initStats) != 2 || initStats[0].ExternalBytes == 0 {
		t.Fatalf("expected external dict on s1 before concurrent ResetAll: err=%v stats=%+v", err, initStats)
	}

	var stopReaders atomic.Bool
	var reads atomic.Int64
	readersActive := make(chan struct{})
	var sigActive sync.Once
	errSink := newTestErrSink(32)
	recErr := func(err error) {
		if err != nil {
			errSink.record(err)
			sigActive.Do(func() { close(readersActive) })
		}
	}

	var readerWG sync.WaitGroup
	for id := range 3 {
		readerWG.Go(func() {
			defer sigActive.Do(func() { close(readersActive) })
			iter := 0
			for !stopReaders.Load() {
				iter++
				switch (iter + id) % 5 {
				case 0:
					if _, err := aggSumErr(ctx, s1, AggregateQuery{
						AllTime: true,
						Filters: []Filter{{Dim: 0, Op: TagContains, Values: []string{"old-0010"}}},
					}); err != nil {
						recErr(err)
						return
					}
				case 1:
					if err := readCheckErr(ctx, s1, ReadRequest{
						AllTime: true, Limit: 10,
						Filters: []Filter{{Dim: 0, Op: TagSuffix, Values: []string{".net"}}},
					}, nil); err != nil {
						recErr(err)
						return
					}
				case 2:
					if err := s1.Scan(ctx, ScanQuery{
						AllTime: true, BatchSize: 64,
						Filters: []Filter{{Dim: 0, Op: TagPrefix, Values: []string{"live-"}}},
					}, func(ScanBatch) error { return nil }); err != nil {
						recErr(err)
						return
					}
				case 3:
					if _, err := s1.DictStats(); err != nil {
						recErr(err)
						return
					}
				case 4:
					if got, err := aggSumErr(ctx, s2, AggregateQuery{
						AllTime: true,
						Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"keep-042"}}},
					}); err != nil {
						recErr(err)
						return
					} else if got != 1 {
						recErr(fmt.Errorf("s2 affected by concurrent s1 ResetAll: got=%d want=1", got))
						return
					}
				}
				if reads.Add(1) >= 2 {
					sigActive.Do(func() { close(readersActive) })
				}
			}
		})
	}

	<-readersActive

	var mutWG sync.WaitGroup
	mutWG.Go(func() {
		for step := range 60 {
			ts := time.Unix(clockSec.Add(60), 0).UTC()
			row := Row{
				Tags:   []string{fmt.Sprintf("live-%03d.net", step), fmt.Sprintf("g-%02d", step%10)},
				Values: [4]int64{2, 2, 2, 2},
				Mask:   0x0f,
			}
			if err := s1.Append(ts, Summary{Values: [4]int64{2, 2, 2, 2}, Mask: 0x0f}, row); err != nil {
				recErr(err)
				return
			}
		}
	})
	mutWG.Go(func() {
		for range 3 {
			runtime.Gosched()
			if err := s1.ResetAll(); err != nil {
				recErr(err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	})
	mutWG.Go(func() {
		for range 2 {
			if err := db.Compact(); err != nil {
				recErr(err)
				return
			}
		}
	})

	mutWG.Wait()
	stopReaders.Store(true)
	readerWG.Wait()
	errSink.check(t)

	// Final deterministic marker + full seal Compact verifies dict reclamation and consistency.
	finalTS := time.Unix(clockSec.Add(60), 0).UTC()
	if err := s1.AppendSync(finalTS, Summary{Values: [4]int64{77, 77, 77, 77}, Mask: 0x0f},
		Row{Tags: []string{"final-marker.net", "final-g"}, Values: [4]int64{77, 77, 77, 77}, Mask: 0x0f},
	); err != nil {
		t.Fatal(err)
	}
	clockSec.Add(10 * 60)
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	if got1 := queryAggSum(t, s1, AggregateQuery{
		AllTime: true,
		Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"final-marker.net"}}},
	}); got1 != 77 {
		t.Fatalf("final marker sum got=%d want=77", got1)
	}

	finalStats1, err := s1.DictStats()
	if err != nil {
		t.Fatal(err)
	}
	if len(finalStats1) != 2 || finalStats1[0].ExternalBytes != 0 || finalStats1[0].ForwardPages != 0 ||
		finalStats1[0].ReverseBlocks != 0 || finalStats1[0].MaxID == 0 || finalStats1[0].MaxID >= 1000 ||
		finalStats1[0].LiveCount != finalStats1[0].MaxID || finalStats1[0].DeadCount != 0 {
		t.Fatalf("unexpected final s1 DictStats after concurrent ResetAll+Compact: %+v", finalStats1)
	}

	if got2 := queryAggSum(t, s2, AggregateQuery{
		AllTime: true,
		Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"keep-042"}}},
	}); got2 != 1 {
		t.Fatalf("s2 final sum got=%d want=1", got2)
	}
	finalStats2, err := s2.DictStats()
	if err != nil || len(finalStats2) != 1 || finalStats2[0].MaxID != 200 || finalStats2[0].LiveCount != 200 {
		t.Fatalf("unexpected final s2 DictStats: err=%v stats=%+v", err, finalStats2)
	}
}

func TestDictGCPhaseBConcurrentResurrectionAndResetAll(t *testing.T) {
	ops := [4]MetricOp{OpSum, OpMin, OpMax, OpLast}
	ctx := context.Background()

	t.Run("PhaseBResurrectionExternalFoldedPage", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "resurrect_ext.db")
		clock := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
		opts := Options{
			Now:         func() time.Time { return clock },
			Retention1m: 2 * time.Hour,
			Retention1h: 4 * time.Hour,
			Retention1d: 6 * time.Hour,
		}
		cfg := SeriesConfig{Arity: 2, SummaryOps: ops, RowOps: ops}
		db, err := Open(path, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if db != nil {
				_ = db.Close()
			}
		}()
		s := mustSeries(t, db, 1, cfg)

		const persistN = 4
		const deadN = 17000
		seedTS := clock.Add(-10 * time.Minute)
		keepRows := make([]Row, persistN)
		for i := range persistN {
			keepRows[i] = Row{
				Tags:   []string{fmt.Sprintf("2001:db8:keep::%d", i+1), "keep"},
				Values: [4]int64{10, 10, 10, 10}, Mask: 0x0f,
			}
		}
		if err := s.Append(seedTS, Summary{Values: [4]int64{persistN * 10, 10, 10, 10}, Mask: 0x0f}, keepRows...); err != nil {
			t.Fatal(err)
		}
		seedHighCardMinute(t, s, seedTS, deadN, func(i int) []string {
			return []string{fmt.Sprintf("2001:db8:old::%05d", i+1), "old"}
		})
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
		stInit, err := s.DictStats()
		if err != nil || stInit[0].ExternalBytes == 0 || stInit[0].MaxID != persistN+deadN {
			t.Fatalf("expected external dim after seed: err=%v stats=%+v", err, stInit)
		}

		// Expire all 17,000 old tags and keep only 4 persistent tags.
		clock = clock.Add(12 * time.Hour)
		if err := s.Append(clock.Add(-10*time.Minute), Summary{Values: [4]int64{persistN * 10, 10, 10, 10}, Mask: 0x0f}, keepRows...); err != nil {
			t.Fatal(err)
		}

		// During Phase B (after pruned SecTSDict with folded pages is written), concurrently
		// write dead tags that hit the old in-memory dict (newID=false, no dictEntry emitted).
		var hookErr error
		db.compactHook = func() {
			if err := s.AppendSync(clock.Add(-30*time.Second), Summary{Values: [4]int64{66, 11, 33, 33}, Mask: 0x0f},
				Row{Tags: []string{"2001:db8:old::00042", "res"}, Values: [4]int64{11, 11, 11, 11}, Mask: 0x0f},
				Row{Tags: []string{"2001:db8:old::05000", "res"}, Values: [4]int64{22, 22, 22, 22}, Mask: 0x0f},
				Row{Tags: []string{"2001:db8:brandnew::1", "new"}, Values: [4]int64{33, 33, 33, 33}, Mask: 0x0f},
			); err != nil {
				hookErr = err
				return
			}
			// Second delta frame reusing the same resurrected ID tests dedup across frames.
			if err := s.AppendSync(clock.Add(-20*time.Second), Summary{Values: [4]int64{14, 14, 14, 14}, Mask: 0x0f},
				Row{Tags: []string{"2001:db8:old::00042", "res"}, Values: [4]int64{14, 14, 14, 14}, Mask: 0x0f},
			); err != nil {
				hookErr = err
			}
		}
		err = db.Compact()
		db.compactHook = nil
		if err != nil {
			t.Fatalf("Compact with Phase B resurrection: %v", err)
		}
		if hookErr != nil {
			t.Fatalf("compactHook AppendSync failed: %v", hookErr)
		}

		verify := func(label string, curS *Series) {
			t.Helper()
			for _, tc := range []struct {
				tag  string
				want int64
			}{
				{"2001:db8:old::00042", 25},
				{"2001:db8:old::05000", 22},
				{"2001:db8:brandnew::1", 33},
				{"2001:db8:keep::1", 10},
				{"2001:db8:old::00043", 0},
			} {
				if got := queryAggSum(t, curS, AggregateQuery{AllTime: true, Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{tc.tag}}}}); got != tc.want {
					t.Fatalf("%s tag=%s sum=%d want=%d", label, tc.tag, got, tc.want)
				}
			}
			if err := readCheckErr(ctx, curS, ReadRequest{AllTime: true, Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"2001:db8:old::05000"}}}, Limit: 10}, func(page *PageResult) error {
				if len(page.Rows) != 1 || page.Rows[0].Tags[0] != "2001:db8:old::05000" || page.Rows[0].Values[0] != 22 {
					return fmt.Errorf("unexpected Read #5000: %+v", page.Rows)
				}
				return nil
			}); err != nil {
				t.Fatalf("%s %v", label, err)
			}
			if _, scanSum, err := scanCountSumErr(ctx, curS, ScanQuery{AllTime: true, Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"2001:db8:old::00042"}}}}); err != nil || scanSum != 25 {
				t.Fatalf("%s Scan #42: err=%v sum=%d want=25", label, err, scanSum)
			}
			st, err := curS.DictStats()
			if err != nil {
				t.Fatal(err)
			}
			wantMax := uint32(persistN + deadN + 1)
			if st[0].MaxID != wantMax || st[0].LiveCount != 7 || st[0].DeadCount != wantMax-7 {
				t.Fatalf("%s DictStats dim0=%+v want max=%d live=7 dead=%d", label, st[0], wantMax, wantMax-7)
			}
		}

		verify("post-compact", s)
		db = reopenTestDB(t, db, path, opts)
		s = mustSeries(t, db, 1, cfg)
		verify("reopened", s)

		// Follow-up Compact seals the resurrected tail WAL and unfolds the previously folded page cleanly.
		clock = clock.Add(10 * time.Minute)
		if err := db.Compact(); err != nil {
			t.Fatalf("follow-up Compact: %v", err)
		}
		db = reopenTestDB(t, db, path, opts)
		s = mustSeries(t, db, 1, cfg)
		verify("post-unfold-reopen", s)
	})

	t.Run("PhaseBConcurrentResetAllIsolation", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "resurrect_resetall.db")
		clock := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
		opts := Options{
			Now:         func() time.Time { return clock },
			Retention1m: 2 * time.Hour,
			Retention1h: 4 * time.Hour,
			Retention1d: 6 * time.Hour,
		}
		cfg := SeriesConfig{Arity: 1, SummaryOps: ops, RowOps: ops}
		db, err := Open(path, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if db != nil {
				_ = db.Close()
			}
		}()
		s1 := mustSeries(t, db, 1, cfg)
		s2 := mustSeries(t, db, 2, cfg)

		const n = 1500
		seedTS := clock.Add(-10 * time.Minute)
		seedHighCardMinute(t, s1, seedTS, n, func(i int) []string {
			return []string{fmt.Sprintf("s1-old-%04d", i+1)}
		})
		seedHighCardMinute(t, s2, seedTS, n, func(i int) []string {
			return []string{fmt.Sprintf("s2-old-%04d", i+1)}
		})
		if err := s2.Append(seedTS, Summary{Values: [4]int64{7, 7, 7, 7}, Mask: 0x0f},
			Row{Tags: []string{"s2-keep"}, Values: [4]int64{7, 7, 7, 7}, Mask: 0x0f},
		); err != nil {
			t.Fatal(err)
		}
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}

		// Expire all s1 data (triggers full omission liveCnt==0) and all s2-old-* (triggers pruning).
		clock = clock.Add(12 * time.Hour)
		if err := s2.Append(clock.Add(-10*time.Minute), Summary{Values: [4]int64{7, 7, 7, 7}, Mask: 0x0f},
			Row{Tags: []string{"s2-keep"}, Values: [4]int64{7, 7, 7, 7}, Mask: 0x0f},
		); err != nil {
			t.Fatal(err)
		}

		var hookErr error
		db.compactHook = func() {
			if err := s1.AppendSync(clock.Add(-40*time.Second), Summary{Values: [4]int64{9, 9, 9, 9}, Mask: 0x0f},
				Row{Tags: []string{"s1-old-0010"}, Values: [4]int64{9, 9, 9, 9}, Mask: 0x0f},
			); err != nil {
				hookErr = err
				return
			}
			if err := s1.ResetAll(); err != nil {
				hookErr = err
				return
			}
			if err := s1.AppendSync(clock.Add(-30*time.Second), Summary{Values: [4]int64{300, 100, 200, 200}, Mask: 0x0f},
				Row{Tags: []string{"s1-fresh-a"}, Values: [4]int64{100, 100, 100, 100}, Mask: 0x0f},
				Row{Tags: []string{"s1-fresh-b"}, Values: [4]int64{200, 200, 200, 200}, Mask: 0x0f},
			); err != nil {
				hookErr = err
				return
			}
			if err := s2.AppendSync(clock.Add(-20*time.Second), Summary{Values: [4]int64{55, 55, 55, 55}, Mask: 0x0f},
				Row{Tags: []string{"s2-old-0010"}, Values: [4]int64{55, 55, 55, 55}, Mask: 0x0f},
			); err != nil {
				hookErr = err
			}
		}
		err = db.Compact()
		db.compactHook = nil
		if err != nil {
			t.Fatalf("Compact with concurrent ResetAll: %v", err)
		}
		if hookErr != nil {
			t.Fatalf("compactHook failed: %v", hookErr)
		}

		db = reopenTestDB(t, db, path, opts)
		s1 = mustSeries(t, db, 1, cfg)
		s2 = mustSeries(t, db, 2, cfg)

		for _, tc := range []struct {
			s    *Series
			tag  string
			want int64
		}{
			{s1, "s1-fresh-a", 100},
			{s1, "s1-fresh-b", 200},
			{s1, "s1-old-0010", 0},
			{s1, "s1-old-0001", 0},
			{s2, "s2-keep", 7},
			{s2, "s2-old-0010", 55},
		} {
			if got := queryAggSum(t, tc.s, AggregateQuery{AllTime: true, Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{tc.tag}}}}); got != tc.want {
				t.Fatalf("%s sum=%d want=%d", tc.tag, got, tc.want)
			}
		}
		st1, err := s1.DictStats()
		if err != nil || st1[0].MaxID != 2 || st1[0].LiveCount != 2 || st1[0].DeadCount != 0 {
			t.Fatalf("unexpected s1 DictStats after concurrent ResetAll: err=%v stats=%+v", err, st1)
		}
		if err := readCheckErr(ctx, s2, ReadRequest{AllTime: true, Filters: []Filter{{Dim: 0, Op: TagEq, Values: []string{"s2-old-0010"}}}, Limit: 10}, func(page *PageResult) error {
			if len(page.Rows) != 1 || page.Rows[0].Tags[0] != "s2-old-0010" || page.Rows[0].Values[0] != 55 {
				return fmt.Errorf("unexpected s2 Read resurrected rows: %+v", page.Rows)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}
