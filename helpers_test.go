package strata

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newDict() dictionary {
	return newDictWithPolicy(memPolicy{dictPageCacheBytes: 256 << 10, dictHotBytes: 256 << 10})
}

func encodeBlockLevel(b sealedBlock, level CompressionLevel) ([]byte, error) {
	enc := newZstdEncoder(level, 128<<10)
	defer enc.Close()
	return encodeBlockWithEncoder(b, enc)
}

func dictionarySnapshot(d dictionary, series uint16) dictionary {
	return dictionarySnapshotForQuery(d, series, nil)
}

func testDB(t *testing.T, now *time.Time, extra Options) (*DB, *Series) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "strata.db")
	extra.Now = func() time.Time { return *now }
	db, err := Open(path, extra)
	if err != nil {
		t.Fatal(err)
	}
	cfg := SeriesConfig{Arity: 2, SummaryOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast}, RowOps: [4]MetricOp{OpSum, OpMin, OpMax, OpLast}}
	s, err := db.Series(7, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return db, s
}

func appendValue(t *testing.T, s *Series, ts time.Time, value int64) {
	t.Helper()
	if err := s.AppendSync(ts, Summary{Values: [4]int64{value, value, value, value}}, Row{Tags: []string{"a", "b"}, Values: [4]int64{value, value, value, value}}); err != nil {
		t.Fatal(err)
	}
}

func reopenTestDB(t *testing.T, cur *DB, path string, opts Options) *DB {
	t.Helper()
	if cur != nil {
		if err := cur.Close(); err != nil {
			t.Fatal(err)
		}
	}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func mustSeries(t *testing.T, db *DB, id uint16, cfg SeriesConfig) *Series {
	t.Helper()
	s, err := db.Series(id, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func mustRegion(t *testing.T, db *DB, name string) *Region {
	t.Helper()
	r, err := db.Region(name)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func assertKV(t *testing.T, r *Region, key string, want []byte, wantOK bool) {
	t.Helper()
	got, ok, err := r.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if ok != wantOK {
		t.Fatalf("Get(%q) ok=%v want=%v", key, ok, wantOK)
	}
	if wantOK && !bytes.Equal(got, want) {
		t.Fatalf("Get(%q) len=%d wantLen=%d", key, len(got), len(want))
	}
}

func queryAggSum(t *testing.T, s *Series, q AggregateQuery) int64 {
	t.Helper()
	got, err := aggSumErr(context.Background(), s, q)
	if err != nil {
		t.Fatalf("Aggregate(%+v): %v", q, err)
	}
	return got
}

func assertTSSum(t *testing.T, s *Series, from, to time.Time, want int64) {
	t.Helper()
	if got := queryAggSum(t, s, AggregateQuery{From: from, To: to}); got != want {
		t.Fatalf("TS sum=%d want=%d", got, want)
	}
}

func patchSuperblocks(t *testing.T, path string, edit func(f *osFile, head *superblock)) {
	t.Helper()
	f, err := openData(path, false)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
	}()
	head, err := readSuper(f)
	if err != nil {
		t.Fatal(err)
	}
	edit(f, &head)
	if err := writeSupers(f, head); err != nil {
		t.Fatal(err)
	}
	if err := syncData(f); err != nil {
		t.Fatal(err)
	}
	closed = true
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func patchSuperSlot(t *testing.T, path string, edit func(f *os.File, slot []byte)) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	slot := make([]byte, slotSize)
	if _, err = f.ReadAt(slot, 0); err != nil {
		t.Fatal(err)
	}
	edit(f, slot)
	binary.LittleEndian.PutUint32(slot[slotSize-4:], crc(slot[:slotSize-4]))
	if _, err = f.WriteAt(slot, 0); err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt(slot, slotSize); err != nil {
		t.Fatal(err)
	}
}

func seedHighCardMinute(t *testing.T, s *Series, ts time.Time, count int, makeTags func(i int) []string) {
	t.Helper()
	rows := make([]Row, count)
	for i := range count {
		rows[i] = Row{Tags: makeTags(i), Values: [4]int64{1, 1, 1, 1}, Mask: 0x0f}
	}
	if err := s.Append(ts, Summary{Values: [4]int64{int64(count), 1, 1, 1}, Mask: 0x0f}, rows...); err != nil {
		t.Fatal(err)
	}
}

type testErrSink struct {
	ch chan error
}

func newTestErrSink(buf int) *testErrSink {
	return &testErrSink{ch: make(chan error, buf)}
}

func (e *testErrSink) record(err error) {
	if err != nil {
		select {
		case e.ch <- err:
		default:
		}
	}
}

func (e *testErrSink) check(t *testing.T) {
	t.Helper()
	close(e.ch)
	for err := range e.ch {
		t.Fatal(err)
	}
}

func runConcurrentWorkers(n int, fn func(id int) error) error {
	var wg sync.WaitGroup
	errCh := make(chan error, max(n, 1))
	start := make(chan struct{})
	for id := range n {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-start
			if err := fn(workerID); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(id)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return nil
}

func aggSumErr(ctx context.Context, s *Series, q AggregateQuery) (int64, error) {
	agg, err := s.Aggregate(ctx, q)
	if err != nil {
		return 0, err
	}
	got := agg.Summary.Values[0]
	agg.Release()
	return got, nil
}

func readCheckErr(ctx context.Context, s *Series, req ReadRequest, check func(*PageResult) error) error {
	page, err := s.Read(ctx, req)
	if err != nil {
		return err
	}
	defer page.Release()
	if check != nil {
		return check(page)
	}
	return nil
}

func scanCountSumErr(ctx context.Context, s *Series, q ScanQuery) (int, int64, error) {
	var cnt int
	var sum int64
	err := s.Scan(ctx, q, func(b ScanBatch) error {
		for i := range b.Len() {
			cnt++
			sum += b.Values(i)[0]
		}
		return nil
	})
	return cnt, sum, err
}
