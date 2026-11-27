package strata

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	backgroundTestInterval = 5 * time.Millisecond
	backgroundTestTimeout  = 2 * time.Second
)

func awaitSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(backgroundTestTimeout):
		t.Fatal(message)
	}
}

func awaitCondition(t *testing.T, message string, ready func() bool) {
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

func awaitResult(t *testing.T, result <-chan error, message string) {
	t.Helper()
	select {
	case err := <-result:
		requireOK(t, err)
	case <-time.After(backgroundTestTimeout):
		t.Fatal(message)
	}
}

func backgroundLoopDone(db *DB) <-chan struct{} {
	db.loopMu.Lock()
	defer db.loopMu.Unlock()
	return db.loopDone
}

func assertNoBackgroundLoop(t *testing.T, db *DB) {
	t.Helper()
	db.loopMu.Lock()
	defer db.loopMu.Unlock()
	if db.loopDone != nil || db.loopStop != nil || db.loopCtxDone != nil || db.loopHasNext || db.loopNextCtxDone != nil {
		t.Fatal("background loop still registered")
	}
}

type startContext struct {
	context.Context
	done func() <-chan struct{}
}

func (c startContext) Done() <-chan struct{} { return c.done() }

func TestBackgroundLoopLifecycle(t *testing.T) {
	t.Run("ConcurrentStartClose", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "strata.db")
		for range 16 {
			db, err := Open(path, Options{})
			requireOK(t, err)
			ctx := t.Context()
			db.Start(ctx)
			done := backgroundLoopDone(db)
			db.Start(ctx)
			if done == nil || backgroundLoopDone(db) != done {
				t.Fatal("repeated Start did not retain the same loop")
			}
			requireOK(t, runConcurrentWorkers(8, func(id int) error {
				if id < 2 {
					return db.Close()
				}
				for range 16 {
					db.Start(t.Context())
					runtime.Gosched()
				}
				return nil
			}))
			select {
			case <-done:
			default:
				t.Fatal("Close returned before the loop stopped")
			}
			db.Start(t.Context())
			assertNoBackgroundLoop(t, db)
		}
	})
	t.Run("Inactive", func(t *testing.T) {
		now := time.Now()
		closed, _ := testDB(t, &now, Options{})
		requireOK(t, closed.Close())
		reader, err := Open(closed.path, Options{ReadOnly: true})
		requireOK(t, err)
		defer reader.Close()
		ctx := t.Context()
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		for _, tc := range []struct {
			db  *DB
			ctx context.Context
		}{
			{reader, ctx}, {closed, ctx},
			{&DB{writer: true, opts: Options{FlushInterval: time.Hour}}, canceled},
			{&DB{writer: true}, ctx},
		} {
			tc.db.Start(tc.ctx)
			assertNoBackgroundLoop(t, tc.db)
		}
	})
	t.Run("CompactWriteFuse", func(t *testing.T) {
		now := time.Now()
		db, _ := testDB(t, &now, Options{FlushInterval: time.Hour, SyncInterval: time.Hour})
		defer db.Close()
		db.optional = []optionalSection{{desc: sectionDesc{kind: 7}, data: []byte{1}, slot: 7}}
		db.Start(t.Context())
		done := backgroundLoopDone(db)
		if err := db.Compact(); !errors.Is(err, ErrIncompatFormat) {
			t.Fatalf("Compact reload error=%v", err)
		}
		if err := db.Flush(); err != ErrReadOnly {
			t.Fatalf("Flush after write fuse=%v", err)
		}
		awaitSignal(t, done, "write fuse did not stop the loop")
		db.Start(t.Context())
		assertNoBackgroundLoop(t, db)
	})
	t.Run("ContextDoneReentry", func(t *testing.T) {
		now := time.Now()
		db, _ := testDB(t, &now, Options{})
		defer db.Close()
		var done <-chan struct{}
		db.Start(startContext{Context: context.Background(), done: func() <-chan struct{} {
			if !db.loopMu.TryLock() {
				t.Fatal("Context.Done ran under loopMu")
			}
			db.loopMu.Unlock()
			if !db.mu.TryLock() {
				t.Fatal("Context.Done ran under mu")
			}
			db.mu.Unlock()
			var nilCtx context.Context
			db.Start(nilCtx)
			done = backgroundLoopDone(db)
			return nil
		}})
		if done == nil || backgroundLoopDone(db) != done {
			t.Fatal("reentrant Start did not retain one loop")
		}
	})
}

func TestBackgroundLoopHandoff(t *testing.T) {
	verify := func(t *testing.T, db *DB, s *Series, now time.Time, done, ctxDone <-chan struct{}) {
		t.Helper()
		awaitCondition(t, "background context did not change on the same loop", func() bool {
			db.loopMu.Lock()
			defer db.loopMu.Unlock()
			return done != nil && db.loopDone == done && !db.loopHasNext && db.loopCtxDone == ctxDone
		})
		db.mu.RLock()
		before := db.walPos
		db.mu.RUnlock()
		requireOK(t, s.Append(now, Summary{Values: testValues(1)}))
		awaitCondition(t, "restarted loop did not flush", func() bool {
			db.mu.RLock()
			defer db.mu.RUnlock()
			return db.walPos > before && len(db.pending) == 0
		})
	}
	t.Run("ReplaceCanceledHandoff", func(t *testing.T) {
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
		awaitSignal(t, started, "Start waited for the canceled loop")
		release()
		verify(t, db, s, now, done, nil)
	})
	t.Run("NowCallback", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		now := time.Now()
		db, s := testDB(t, &now, Options{FlushInterval: backgroundTestInterval})
		defer func() {
			closed := make(chan error, 1)
			go func() { closed <- db.Close() }()
			awaitResult(t, closed, "Close blocked after Now callback")
			assertNoBackgroundLoop(t, db)
		}()
		returned := make(chan struct{})
		db.opts.Now = func() time.Time {
			select {
			case <-returned:
			default:
				cancel()
				db.Start(t.Context())
				close(returned)
			}
			return now
		}
		db.Start(ctx)
		done := backgroundLoopDone(db)
		awaitSignal(t, returned, "Start blocked inside Now")
		verify(t, db, s, now, done, t.Context().Done())
	})
	t.Run("BothClosesWait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		db, _, _, release := startBlockedAutoCompact(t, ctx)
		done := backgroundLoopDone(db)
		started := make(chan struct{})
		go func() { cancel(); db.Start(context.Background()); close(started) }()
		awaitSignal(t, started, "Start blocked while registering handoff")
		db.loopMu.Lock()
		stop := db.loopStop
		db.loopMu.Unlock()
		results := make(chan error, 2)
		closeDB := func() {
			err := db.Close()
			if err == nil {
				select {
				case <-done:
				default:
					err = errors.New("Close returned before the loop stopped")
				}
			}
			results <- err
		}
		go closeDB()
		go closeDB()
		awaitSignal(t, stop, "Close did not signal the loop")
		// The stop signal is shared. Observe both callers waiting inside Close.
		stack := make([]byte, 16<<10)
		awaitCondition(t, "both Close calls did not wait for compaction", func() bool {
			select {
			case err := <-results:
				t.Fatalf("Close returned during blocked compaction: %v", err)
			default:
			}
			n := runtime.Stack(stack, true)
			if n == len(stack) {
				stack = make([]byte, 2*len(stack))
				return false
			}
			waiting := 0
			for goroutine := range strings.SplitSeq(string(stack[:n]), "\n\n") {
				if strings.Contains(goroutine, "[chan receive]") && strings.Contains(goroutine, ".(*DB).Close(") &&
					strings.Contains(goroutine, ".TestBackgroundLoopHandoff.") {
					waiting++
				}
			}
			return waiting == 2
		})
		release()
		for range 2 {
			awaitResult(t, results, "Close blocked after compaction")
		}
		assertNoBackgroundLoop(t, db)
	})
}

func startBlockedAutoCompact(t *testing.T, ctx context.Context) (*DB, *Series, time.Time, func()) {
	t.Helper()
	now := time.Now()
	db, s := testDB(t, &now, Options{FlushInterval: backgroundTestInterval, WALCompactBytes: 1})
	entered, unblock := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	t.Cleanup(func() {
		release()
		closed := make(chan error, 1)
		go func() { closed <- db.Close() }()
		awaitResult(t, closed, "Close blocked after compaction")
	})
	db.compactHook = sync.OnceFunc(func() { close(entered); <-unblock })
	appendValue(t, s, now.Add(-time.Hour), 1)
	db.Start(ctx)
	awaitSignal(t, entered, "automatic compact did not reach its hook")
	return db, s, now, release
}

func dictCacheStats(db *DB) (used, capacity int) {
	db.mu.RLock()
	cache := db.dict.pageCache
	db.mu.RUnlock()
	if cache == nil {
		return 0, 0
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	return cache.usedBytes, cache.maxBytes
}

func TestCompactSealedSeqLessThanFenceAndCrash(t *testing.T) {
	now := time.Date(2026, 2, 10, 12, 34, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{})
	defer db.Close()
	old := now.Add(-2 * time.Hour)
	appendValue(t, s, old, 10)
	appendValue(t, s, now.Add(-time.Minute), 20)
	fence := db.appendSeq
	var tailErr error
	db.compactHook = func() {
		tailErr = s.AppendSync(now.Add(-30*time.Second), Summary{Values: testValues(30)}, testRow(30, "a", "b"))
	}
	requireOK(t, db.Compact())
	db.compactHook = nil
	requireOK(t, tailErr)
	if db.head.sealedWalSeq >= fence {
		t.Fatalf("sealed sequence=%d should precede fence=%d", db.head.sealedWalSeq, fence)
	}
	// Recover directly from the compacted file and its WAL, before graceful Close.
	raw, err := os.ReadFile(db.path)
	requireOK(t, err)
	path := filepath.Join(t.TempDir(), "crash.db")
	requireOK(t, os.WriteFile(path, raw, 0o600))
	recovered, err := Open(path, db.opts)
	requireOK(t, err)
	defer recovered.Close()
	rs := mustSeries(t, recovered, s.id, s.cfg)
	assertTSSum(t, rs, old, now, 60)
	requireOK(t, recovered.Compact())
	assertTSSum(t, rs, old, now, 60)
}

func TestReaderRefreshAcrossRename(t *testing.T) {
	for _, mmap := range []bool{false, true} {
		t.Run(fmt.Sprintf("mmap=%t", mmap), func(t *testing.T) {
			now := time.Now().Truncate(time.Minute)
			writer, s := testDB(t, &now, Options{})
			defer writer.Close()
			reader, err := Open(writer.path, Options{ReadOnly: true, UseMmap: mmap})
			requireOK(t, err)
			defer reader.Close()
			old, data := now.Add(-2*time.Hour), reader.data
			appendValue(t, s, old, 1)
			requireOK(t, reader.Refresh())
			if reader.data != data {
				t.Fatal("incremental Refresh reopened the same generation")
			}
			rs := mustSeries(t, reader, s.id, s.cfg)
			requireOK(t, writer.Compact())
			requireOK(t, reader.Refresh())
			if reader.data == data || reader.head.gen != writer.head.gen {
				t.Fatal("Refresh did not replace the previous file view")
			}
			assertTSSum(t, rs, old, now, 1)
			appendValue(t, s, now.Add(-time.Minute), 2)
			requireOK(t, reader.Refresh())
			assertTSSum(t, rs, old, now, 3)
		})
	}
}

func TestCompactFailureKeepsOldBlocksReadable(t *testing.T) {
	now := time.Now().Truncate(time.Minute)
	db, s := testDB(t, &now, Options{})
	defer func() { _ = db.Close() }()
	seed := func(ts time.Time, n, offset int) {
		t.Helper()
		seedHighCardMinute(t, s, ts, n, func(i int) []string {
			return []string{fmt.Sprintf("g-%02d", i%10), fmt.Sprintf("k-%04d", offset+i)}
		})
	}
	seed(now.Add(-5*time.Minute), 300, 0)
	requireOK(t, db.Compact())
	seed(now.Add(-time.Minute), 260, 300)
	requireOK(t, db.Sync())
	check := func() {
		t.Helper()
		agg, err := s.Aggregate(t.Context(), AggregateQuery{AllTime: true, GroupDims: []int{0}})
		requireOK(t, err)
		defer agg.Release()
		if agg.Summary.Values[0] != 560 || len(agg.Groups) != 1 || len(agg.Groups[0]) != 10 {
			t.Fatalf("old blocks changed after Compact: %+v", agg)
		}
		for _, group := range agg.Groups[0] {
			if group.Summary.Values[0] != 56 {
				t.Fatalf("group changed after Compact: %+v", group)
			}
		}
		n, sum, err := scanCountSumErr(t.Context(), s, ScanQuery{AllTime: true})
		if err != nil || n != 560 || sum != 560 {
			t.Fatalf("Scan after Compact: rows=%d sum=%d err=%v", n, sum, err)
		}
	}
	tmp := db.path + ".tmp"
	requireOK(t, os.Mkdir(tmp, 0o755))
	requireOK(t, os.WriteFile(filepath.Join(tmp, "blocker"), []byte("x"), 0o600))
	if err := db.Compact(); err == nil {
		t.Fatal("Compact wrote through a blocked temporary path")
	}
	check()
	requireOK(t, os.RemoveAll(tmp))
	if runtime.GOOS != "windows" {
		db.compactHook = func() { _ = os.Remove(tmp) }
		err := db.Compact()
		db.compactHook = nil
		if err == nil {
			t.Fatal("Compact renamed a missing temporary file")
		}
		check()
	}
	requireOK(t, db.Compact())
	check()
	db = reopenTestDB(t, db, db.path, db.opts)
	s = mustSeries(t, db, s.id, s.cfg)
	check()
}

func TestConcurrentReadCompactReclaimRace(t *testing.T) {
	for _, profile := range []MemoryProfile{MemoryConstrained, MemoryBalanced} {
		for _, mmap := range []bool{false, true} {
			t.Run(fmt.Sprintf("profile=%d-mmap=%t", profile, mmap), func(t *testing.T) {
				now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
				db, s := testDB(t, &now, Options{UseMmap: mmap, MemoryProfile: profile})
				defer db.Close()
				client := NewClient(db, ClientOptions{})
				defer client.Close()
				seedHighCardMinute(t, s, now.Add(-5*time.Minute), 300, func(i int) []string {
					return []string{"dom", fmt.Sprintf("ip-%d", i)}
				})
				appendValue(t, s, now.Add(-4*time.Minute), 1)
				requireOK(t, db.Compact())
				ctx := t.Context()
				cacheErrors := newTestErrSink(12)
				requireOK(t, runConcurrentWorkers(3, func(id int) error {
					for i := range 12 {
						switch id {
						case 0:
							if err := s.Append(now.Add(time.Duration(i)*time.Minute), Summary{Values: testValues(1)}, testRow(1, "dom", "live")); err != nil {
								return err
							}
							if i%3 == 0 {
								if err := db.Compact(); err != nil {
									return err
								}
							}
						case 1:
							if err := readCheckErr(ctx, s, ReadRequest{AllTime: true, Limit: 50}, nil); err != nil {
								return err
							}
							if _, _, err := scanCountSumErr(ctx, s, ScanQuery{AllTime: true, Filters: tagFilter(0, TagEq, "dom")}); err != nil {
								return err
							}
							if _, err := aggSumErr(ctx, s, AggregateQuery{AllTime: true, GroupDims: []int{1}, TopK: 15}); err != nil {
								return err
							}
							if used, budget := dictCacheStats(db); budget > 0 && used > budget {
								cacheErrors.record(fmt.Errorf("dictionary cache used=%d budget=%d", used, budget))
							}
						case 2:
							client.Reclaim()
							runtime.Gosched()
						}
					}
					return nil
				}))
				cacheErrors.check(t)
				client.Reclaim()
				if used, _ := dictCacheStats(db); used != 0 {
					t.Fatalf("Reclaim left %d cache bytes", used)
				}
			})
		}
	}
}

func TestInferredSeriesConfigRefreshRace(t *testing.T) {
	now := time.Now().Truncate(time.Minute)
	writer, s := testDB(t, &now, Options{})
	defer writer.Close()
	appendValue(t, s, now.Add(-5*time.Minute), 10)
	client, err := OpenClient(writer.path, ClientOptions{DB: Options{ReadOnly: true}})
	requireOK(t, err)
	defer client.Close()
	ctx := t.Context()
	req := ReadRequest{AllTime: true, Limit: 10}
	requireOK(t, client.WithPage(ctx, s.id, req, nil))
	if cached := client.DB().series[s.id]; cached == nil || !cached.configInferred {
		t.Fatal("reader must start with an inferred config")
	}
	requireOK(t, writer.Compact())
	id, cfg := s.id, s.cfg
	requireOK(t, runConcurrentWorkers(4, func(worker int) error {
		for range 16 {
			var err error
			switch worker {
			case 0:
				err = client.Refresh()
			case 1:
				if _, err = client.Series(id, cfg); err == nil {
					err = client.WithPage(ctx, id, req, nil)
				}
			case 2:
				err = client.WithAggregate(ctx, id, AggregateQuery{AllTime: true, GroupDims: []int{0}}, nil)
				if err == nil {
					err = client.WithAggregate(ctx, id, AggregateQuery{AllTime: true}, nil)
				}
			case 3:
				err = client.Scan(ctx, id, ScanQuery{AllTime: true}, func(ScanBatch) error { return nil })
			}
			if err != nil {
				return err
			}
		}
		return nil
	}))
}

func TestQueryFilterCompileDoesNotBlockAppendAndCompact(t *testing.T) {
	for _, mmap := range []bool{false, true} {
		t.Run(fmt.Sprintf("mmap=%t", mmap), func(t *testing.T) {
			now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
			db, _ := testDB(t, &now, Options{UseMmap: mmap})
			defer db.Close()
			s := mustSeries(t, db, 9, defaultSeriesConfig(3))
			ts := now.Add(-2 * time.Hour)
			seedHighCardMinute(t, s, ts, 17000, func(i int) []string {
				return []string{fmt.Sprintf("svc-%05d", i), fmt.Sprintf("10.0.%d.%d", i>>8, i&255), fmt.Sprintf("app-%03d", i%150)}
			})
			requireOK(t, s.Append(ts, Summary{Values: testValues(7)}, testRow(7, "anchor.edge.net", "198.51.100.42", "app-anchor")))
			requireOK(t, db.Compact())
			stats := dictStats(t, s)
			if stats[0].ExternalBytes == 0 || stats[1].ExternalBytes == 0 || stats[2].ExternalBytes != 0 {
				t.Fatalf("seed needs external and inline dimensions: %+v", stats)
			}
			assertQuerySum(t, s, AggregateQuery{AllTime: true, Filters: tagFilter(2, TagEq, "app-anchor")}, 7)
			for _, tc := range []struct {
				dim      uint8
				old, new string
				want     int64
			}{
				{0, "svc-00000", "override-alt", 1}, {2, "app-149", "override-app", 113},
			} {
				id, err := db.dict.lookup(s.id, tc.dim, tc.old)
				requireOK(t, err)
				if id == 0 {
					t.Fatalf("missing override source %q", tc.old)
				}
				requireOK(t, db.dict.apply(dictEntry{id: id, series: s.id, dim: tc.dim, value: tc.new}))
				assertQuerySum(t, s, AggregateQuery{AllTime: true, Filters: tagFilter(int(tc.dim), TagEq, tc.old)}, 0)
				assertQuerySum(t, s, AggregateQuery{AllTime: true, Filters: tagFilter(int(tc.dim), TagEq, tc.new)}, tc.want)
				assertQuerySum(t, s, AggregateQuery{AllTime: true, Filters: tagFilter(int(tc.dim), TagPrefix, tc.old)}, 0)
				assertQuerySum(t, s, AggregateQuery{AllTime: true, Filters: tagFilter(int(tc.dim), TagContains, tc.new)}, tc.want)
			}

			ctx := t.Context()
			contains := AggregateQuery{AllTime: true, Filters: tagFilter(0, TagContains, "anchor")}
			suffix := ReadRequest{AllTime: true, Filters: tagFilter(0, TagSuffix, ".edge.net"), Limit: 10}
			cidr := ScanQuery{AllTime: true, Filters: tagFilter(1, TagCIDR, "198.51.100.0/24")}
			prefix := tagFilter(0, TagPrefix, "svc-0010")
			negatedPrefix := append(slices.Clone(prefix), Filter{Dim: 2, Op: TagEq, Values: []string{"app-anchor"}, Negate: true})
			entered, unblock := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(unblock) })
			defer release()
			db.compactHook = func() { close(entered); <-unblock }
			compacted := make(chan error, 1)
			go func() { compacted <- db.Compact() }()
			awaitSignal(t, entered, "Compact did not reach the write boundary")
			work := make(chan error, 1)
			go func() {
				work <- runConcurrentWorkers(3, func(id int) error {
					for i := range 12 {
						switch id {
						case 0:
							if err := s.Append(now.Add(time.Duration(i)*time.Minute), Summary{Values: testValues(3)},
								testRow(3, fmt.Sprintf("tail-%d", i), "10.2.0.1", "app-tail")); err != nil {
								return err
							}
						case 1:
							if got, err := aggSumErr(ctx, s, contains); err != nil || got != 7 {
								return fmt.Errorf("contains: sum=%d err=%v", got, err)
							}
							if err := readCheckErr(ctx, s, suffix, func(p *PageResult) error {
								if len(p.Rows) != 1 || p.Rows[0].Tags[0] != "anchor.edge.net" {
									return fmt.Errorf("suffix rows: %+v", p.Rows)
								}
								return nil
							}); err != nil {
								return err
							}
						case 2:
							n, sum, err := scanCountSumErr(ctx, s, cidr)
							if err != nil || n != 1 || sum != 7 {
								return fmt.Errorf("CIDR: rows=%d sum=%d err=%v", n, sum, err)
							}
							if got, err := aggSumErr(ctx, s, AggregateQuery{AllTime: true, Filters: negatedPrefix}); err != nil || got != 10 {
								return fmt.Errorf("prefix with negation: sum=%d err=%v", got, err)
							}
						}
					}
					return nil
				})
			}()
			awaitResult(t, work, "filtered queries or Append blocked during Compact")
			release()
			awaitResult(t, compacted, "Compact did not finish")
			db.compactHook = nil
			assertQuerySum(t, s, AggregateQuery{AllTime: true, Filters: tagFilter(0, TagPrefix, "tail-")}, 36)

			p, err := s.Read(ctx, ReadRequest{AllTime: true, Filters: prefix, Limit: 1})
			requireOK(t, err)
			cursor := p.NextCursor
			p.Release()
			if cursor.IsZero() {
				t.Fatal("missing cursor before Close")
			}
			scanEntered, scanUnblock := make(chan struct{}), make(chan struct{})
			releaseScan := sync.OnceFunc(func() { close(scanUnblock) })
			defer releaseScan()
			firstBatch := sync.OnceFunc(func() { close(scanEntered); <-scanUnblock })
			scanned := make(chan error, 1)
			go func() {
				n := 0
				err := s.Scan(ctx, ScanQuery{AllTime: true, Filters: prefix, BatchSize: 1}, func(b ScanBatch) error {
					n += b.Len()
					firstBatch()
					return nil
				})
				if err == nil && n != 10 {
					err = fmt.Errorf("in-flight Scan returned %d rows after Close", n)
				}
				scanned <- err
			}()
			awaitSignal(t, scanEntered, "filtered Scan did not reach first batch")
			closed := make(chan error, 1)
			go func() { closed <- db.Close() }()
			awaitResult(t, closed, "Close waited on an in-flight Scan")
			releaseScan()
			awaitResult(t, scanned, "in-flight Scan did not finish")
			for _, q := range []AggregateQuery{contains, {AllTime: true}} {
				if _, err := aggSumErr(ctx, s, q); err == nil {
					t.Fatal("Aggregate succeeded after Close")
				}
			}
			for _, q := range []ReadRequest{suffix, {Cursor: cursor, Filters: prefix, Limit: 1}} {
				if err := readCheckErr(ctx, s, q, nil); err == nil {
					t.Fatal("Read succeeded after Close")
				}
			}
			if _, _, err := scanCountSumErr(ctx, s, cidr); err == nil {
				t.Fatal("Scan succeeded after Close")
			}
		})
	}
}

func TestResetAllConcurrentQueryAppendCompact(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	var clock atomic.Int64
	clock.Store(now.Unix())
	db, err := Open(filepath.Join(t.TempDir(), "reset-race.db"), Options{
		UseMmap: true, Now: func() time.Time { return time.Unix(clock.Load(), 0) },
	})
	requireOK(t, err)
	defer db.Close()
	s := mustSeries(t, db, 1, defaultSeriesConfig(2))
	other := mustSeries(t, db, 2, defaultSeriesConfig(1))
	seedHighCardMinute(t, s, now.Add(-2*time.Hour), 17000, func(i int) []string {
		return []string{fmt.Sprintf("old-%05d.net", i), "old"}
	})
	requireOK(t, other.Append(now.Add(-2*time.Hour), Summary{Values: testValues(1)}, testRow(1, "keep")))
	requireOK(t, db.Compact())
	if dictStats(t, s)[0].ExternalBytes == 0 {
		t.Fatal("reset race requires an external dictionary")
	}
	ctx := t.Context()
	keep := AggregateQuery{AllTime: true, Filters: tagFilter(0, TagEq, "keep")}
	requireOK(t, runConcurrentWorkers(5, func(id int) error {
		for i := range 12 {
			switch id {
			case 0:
				ts := time.Unix(clock.Add(60), 0)
				if err := s.Append(ts, Summary{Values: testValues(2)}, testRow(2, fmt.Sprintf("live-%d", i), "live")); err != nil {
					return err
				}
			case 1:
				if i%4 == 0 {
					if err := s.ResetAll(); err != nil {
						return err
					}
				}
			case 2:
				if i%6 == 0 {
					if err := db.Compact(); err != nil {
						return err
					}
				}
			case 3:
				if _, err := aggSumErr(ctx, s, AggregateQuery{AllTime: true, Filters: tagFilter(0, TagContains, "old-0010")}); err != nil {
					return err
				}
				if err := readCheckErr(ctx, s, ReadRequest{AllTime: true, Limit: 10, Filters: tagFilter(0, TagSuffix, ".net")}, nil); err != nil {
					return err
				}
				if _, _, err := scanCountSumErr(ctx, s, ScanQuery{AllTime: true, Filters: tagFilter(0, TagPrefix, "live-")}); err != nil {
					return err
				}
				if _, err := s.DictStats(); err != nil {
					return err
				}
			case 4:
				if got, err := aggSumErr(ctx, other, keep); err != nil || got != 1 {
					return fmt.Errorf("neighbor series during ResetAll: sum=%d err=%v", got, err)
				}
			}
			runtime.Gosched()
		}
		return nil
	}))
	requireOK(t, s.AppendSync(time.Unix(clock.Add(60), 0), Summary{Values: testValues(77)}, testRow(77, "marker", "live")))
	clock.Add(10 * 60)
	requireOK(t, db.Compact())
	st := dictStats(t, s)[0]
	if st.ExternalBytes != 0 || st.MaxID == 0 || st.MaxID > 13 || st.LiveCount != st.MaxID || st.DeadCount != 0 {
		t.Fatalf("external dictionary survived reset: %+v", st)
	}
	assertQuerySum(t, s, AggregateQuery{AllTime: true, Filters: tagFilter(0, TagEq, "marker")}, 77)
	assertQuerySum(t, other, keep, 1)
}

func TestDictGCPhaseBConcurrentResurrectionAndResetAll(t *testing.T) {
	opts := Options{Retention1m: 2 * time.Hour, Retention1h: 4 * time.Hour, Retention1d: 6 * time.Hour}
	compactWithWrites := func(t *testing.T, db *DB, writes func() error) {
		t.Helper()
		var writeErr error
		db.compactHook = func() { writeErr = writes() }
		err := db.Compact()
		db.compactHook = nil
		requireOK(t, err)
		requireOK(t, writeErr)
	}
	t.Run("FoldedExternalPage", func(t *testing.T) {
		now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
		db, s := testDB(t, &now, opts)
		defer func() { _ = db.Close() }()
		const staleCount = 17000
		oldTag := func(i int) string { return fmt.Sprintf("old-%05d", i) }
		seedHighCardMinute(t, s, now.Add(-10*time.Minute), staleCount, func(i int) []string {
			return []string{oldTag(i), "old"}
		})
		appendValue(t, s, now.Add(-10*time.Minute), 10)
		requireOK(t, db.Compact())
		if st := dictStats(t, s)[0]; st.ExternalBytes == 0 || st.MaxID != staleCount+1 {
			t.Fatalf("seed did not create an external dictionary: %+v", st)
		}

		now = now.Add(12 * time.Hour)
		appendValue(t, s, now.Add(-10*time.Minute), 10)
		compactWithWrites(t, db, func() error {
			err := s.AppendSync(now.Add(-30*time.Second), Summary{Values: [4]int64{66, 11, 33, 33}},
				testRow(11, oldTag(42), "old"), testRow(22, oldTag(5000), "old"), testRow(33, "brandnew", "new"))
			if err != nil {
				return err
			}
			// A separate WAL frame reuses a resurrected ID.
			return s.AppendSync(now.Add(-20*time.Second), Summary{Values: testValues(14)}, testRow(14, oldTag(42), "old"))
		})
		for _, followup := range []bool{false, true} {
			if followup {
				now = now.Add(10 * time.Minute)
				requireOK(t, db.Compact())
			}
			db = reopenTestDB(t, db, db.path, db.opts)
			s = mustSeries(t, db, s.id, s.cfg)
			for _, tc := range []struct {
				tag  string
				want int64
			}{{oldTag(42), 25}, {oldTag(5000), 22}, {oldTag(43), 0}, {"brandnew", 33}, {"a", 10}} {
				assertQuerySum(t, s, AggregateQuery{AllTime: true, Filters: tagFilter(0, TagEq, tc.tag)}, tc.want)
			}
			if st := dictStats(t, s)[0]; st.MaxID != staleCount+2 || st.LiveCount != 4 || st.DeadCount != staleCount-2 {
				t.Fatalf("resurrected dictionary stats: %+v", st)
			}
		}
		requireOK(t, readCheckErr(t.Context(), s, ReadRequest{AllTime: true, Filters: tagFilter(0, TagEq, oldTag(5000)), Limit: 10}, func(p *PageResult) error {
			if len(p.Rows) != 1 || p.Rows[0].Tags[0] != oldTag(5000) || p.Rows[0].Values[0] != 22 {
				return fmt.Errorf("resurrected Read rows: %+v", p.Rows)
			}
			return nil
		}))
		n, sum, err := scanCountSumErr(t.Context(), s, ScanQuery{AllTime: true, Filters: tagFilter(0, TagEq, oldTag(42))})
		if err != nil || n != 1 || sum != 25 {
			t.Fatalf("resurrected Scan: rows=%d sum=%d err=%v", n, sum, err)
		}
	})
	t.Run("ConcurrentResetAll", func(t *testing.T) {
		now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
		db, _ := testDB(t, &now, opts)
		defer func() { _ = db.Close() }()
		cfg := defaultSeriesConfig(1)
		reset, keep := mustSeries(t, db, 1, cfg), mustSeries(t, db, 2, cfg)
		ts := now.Add(-10 * time.Minute)
		for _, s := range []*Series{reset, keep} {
			seedHighCardMinute(t, s, ts, 1500, func(i int) []string { return []string{fmt.Sprintf("old-%04d", i+1)} })
		}
		requireOK(t, keep.Append(ts, Summary{Values: testValues(7)}, testRow(7, "keep")))
		requireOK(t, db.Compact())
		now = now.Add(12 * time.Hour)
		requireOK(t, keep.Append(now.Add(-10*time.Minute), Summary{Values: testValues(7)}, testRow(7, "keep")))

		compactWithWrites(t, db, func() error {
			if err := reset.AppendSync(now.Add(-40*time.Second), Summary{Values: testValues(9)}, testRow(9, "old-0010")); err != nil {
				return err
			}
			if err := reset.ResetAll(); err != nil {
				return err
			}
			if err := reset.AppendSync(now.Add(-30*time.Second), Summary{Values: testValues(300)}, testRow(100, "fresh-a"), testRow(200, "fresh-b")); err != nil {
				return err
			}
			return keep.AppendSync(now.Add(-20*time.Second), Summary{Values: testValues(55)}, testRow(55, "old-0010"))
		})
		db = reopenTestDB(t, db, db.path, db.opts)
		reset, keep = mustSeries(t, db, reset.id, cfg), mustSeries(t, db, keep.id, cfg)
		for _, tc := range []struct {
			s   *Series
			tag string
			sum int64
		}{
			{reset, "fresh-a", 100}, {reset, "fresh-b", 200}, {reset, "old-0010", 0},
			{reset, "old-0001", 0}, {keep, "keep", 7}, {keep, "old-0010", 55},
		} {
			assertQuerySum(t, tc.s, AggregateQuery{AllTime: true, Filters: tagFilter(0, TagEq, tc.tag)}, tc.sum)
		}
		if st := dictStats(t, reset)[0]; st.MaxID != 2 || st.LiveCount != 2 || st.DeadCount != 0 {
			t.Fatalf("Phase B carried old-epoch IDs through ResetAll: %+v", st)
		}
		req := ReadRequest{AllTime: true, Filters: tagFilter(0, TagEq, "old-0010"), Limit: 10}
		requireOK(t, readCheckErr(t.Context(), keep, req, func(p *PageResult) error {
			if len(p.Rows) != 1 || p.Rows[0].Tags[0] != "old-0010" || p.Rows[0].Values[0] != 55 {
				return fmt.Errorf("neighbor's resurrected row: %+v", p.Rows)
			}
			return nil
		}))
	})
}
