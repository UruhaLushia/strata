package eventlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPackageBoundaries(t *testing.T) {
	files, err := filepath.Glob("*.go")
	check(t, err)
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		check(t, err)
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, "\"")
			if strings.Contains(p, "/internal/") || p == "github.com/UruhaLushia/strata" ||
				strings.HasPrefix(p, "github.com/klauspost/compress") {
				t.Errorf("%s imports %s", name, p)
			}
		}
		checkAPI := func(node ast.Node) {
			ast.Inspect(node, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.InterfaceType:
					t.Errorf("%s: anonymous interface in exported API", name)
				case *ast.Ident:
					if n.Name == "any" {
						t.Errorf("%s: any in exported API", name)
					}
				}
				return true
			})
		}
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Name.IsExported() {
					checkAPI(d.Type)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					if typ, ok := spec.(*ast.TypeSpec); ok && typ.Name.IsExported() {
						checkAPI(typ.Type)
					}
				}
			}
		}
	}
}

func TestRoundTripAndScanLimits(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{})
	check(t, err)
	defer l.Close()
	a := Record{-7, 19, []byte("one")}
	check(t, l.Append(a, Record{7, 3, []byte("two")}))
	a.Payload[0] = 'X'
	if got := timestamps(t, l); len(got) != 0 {
		t.Fatalf("unflushed records visible: %v", got)
	}
	check(t, l.Flush())
	if got := timestamps(t, l); !slices.Equal(got, []int64{7, -7}) {
		t.Fatalf("flushed order = %v", got)
	}
	check(t, l.Append(Record{-13, 0, []byte("last")}))
	check(t, l.Sync())
	check(t, l.Close())
	l, err = Open(dir, Options{})
	check(t, err)
	defer l.Close()

	want := []string{"last", "two", "one"}
	result, err := l.Scan(t.Context(), ScanOptions{MaxRecords: 3}, func(r Record) error {
		if len(want) == 0 || string(r.Payload) != want[0] || cap(r.Payload) != len(r.Payload) ||
			r.TimestampNS == -7 && r.StreamID != 19 {
			t.Fatalf("unexpected borrowed record: %+v", r)
		}
		want = want[1:]
		return nil
	})
	check(t, err)
	if len(want) != 0 || result.Records != 3 || result.LimitReached || result.Truncated {
		t.Fatalf("exact record budget: %+v", result)
	}
	firstFrame := int64(minFrameSize + len("last"))
	for _, tc := range []struct {
		opts ScanOptions
		n    int
		size int64
	}{
		{ScanOptions{MaxRecords: 2}, 2, result.BytesRead},
		{ScanOptions{MaxBytes: firstFrame}, 1, firstFrame},
		{ScanOptions{MaxBytes: firstFrame - 1}, 0, 0},
	} {
		r, err := l.Scan(t.Context(), tc.opts, func(Record) error { return nil })
		check(t, err)
		if !r.LimitReached || r.Stopped || r.Truncated || r.Records != tc.n || r.BytesRead != tc.size {
			t.Fatalf("budget %+v: %+v", tc.opts, r)
		}
	}
	r, err := l.Scan(t.Context(), ScanOptions{}, func(Record) error { return ErrStopScan })
	check(t, err)
	if !r.Stopped || r.Records != 1 || r.LimitReached || r.Truncated {
		t.Fatalf("callback stop: %+v", r)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r, err = l.Scan(ctx, ScanOptions{}, func(Record) error { cancel(); return nil })
	if !errors.Is(err, context.Canceled) || r.Records != 1 {
		t.Fatalf("callback cancellation: %+v, %v", r, err)
	}
}

func TestOpenReusesNewestShard(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{})
	check(t, err)
	check(t, l.Close())
	if n := len(shardFiles(t, dir)); n != 0 {
		t.Fatalf("idle Open created %d shards", n)
	}
	for i := range 5 {
		l, err := Open(dir, Options{})
		check(t, err)
		check(t, l.Append(Record{TimestampNS: int64(i)}))
		check(t, l.Flush())
		check(t, l.Close())
	}
	l, err = Open(dir, Options{})
	check(t, err)
	defer l.Close()
	if files := shardFiles(t, dir); !slices.Equal(files, []string{filepath.Join(dir, "shard-0000000000000001.evl")}) {
		t.Fatalf("unexpected shard names: %v", files)
	}
	if got := timestamps(t, l); !slices.Equal(got, []int64{4, 3, 2, 1, 0}) {
		t.Fatalf("reopened history = %v", got)
	}
}

func TestScanWindows(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		frames, rows, payload int
	}{
		{"WindowBoundaries", 104, 20, 64},
		{"OversizeFrame", 1, 1280, 50},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			l, err := Open(dir, Options{MaxBatchBytes: 128 << 10, MaxBatchCount: tc.rows})
			check(t, err)
			rows := slices.Repeat([]Record{{Payload: make([]byte, tc.payload)}}, tc.rows)
			for i := range tc.frames {
				for j := range rows {
					rows[j].TimestampNS = int64(i*tc.rows + j)
				}
				check(t, l.Append(rows...))
				check(t, l.Flush())
			}
			check(t, l.Close())
			l, err = Open(dir, Options{})
			check(t, err)
			defer l.Close()

			frameBytes := int64(frameOverhead + tc.rows*(recordHeaderSize+tc.payload))
			totalBytes := int64(tc.frames) * frameBytes
			for _, budget := range []int64{totalBytes - 1, totalBytes} {
				next := int64(tc.frames * tc.rows)
				r, err := l.Scan(t.Context(), ScanOptions{MaxBytes: budget}, func(row Record) error {
					next--
					if row.TimestampNS != next || len(row.Payload) != tc.payload {
						t.Fatalf("unexpected record: timestamp=%d want=%d, payload=%d bytes", row.TimestampNS, next, len(row.Payload))
					}
					return nil
				})
				check(t, err)
				frames := budget / frameBytes
				if r.Records != int(frames)*tc.rows || r.BytesRead != frames*frameBytes || r.LimitReached != (budget < totalBytes) {
					t.Fatalf("budget %d: %+v", budget, r)
				}
			}
			if len(l.scanSem) != cap(l.scanSem) {
				t.Fatal("Scan did not return its slot")
			}
			for range cap(l.scanSem) {
				slot := <-l.scanSem
				l.scanSem <- slot
				if slot != nil && (cap(slot.buf) > scanWindowBytes || cap(slot.offsets) > 1024) {
					t.Fatal("Scan retained an oversized buffer")
				}
			}
			check(t, l.Close())
			if len(l.scanSem) != 0 {
				t.Fatal("Close did not release scan slots")
			}
		})
	}
}

func TestTailRecoveryAndCorruption(t *testing.T) {
	source := t.TempDir()
	l, err := Open(source, Options{})
	check(t, err)
	path := filepath.Join(source, "shard-0000000000000001.evl")
	start := []int{fileHeaderSize}
	for i := range 3 {
		check(t, l.Append(Record{TimestampNS: int64(i + 1), Payload: []byte("record")}))
		check(t, l.Flush())
		info, err := os.Stat(path)
		check(t, err)
		start = append(start, int(info.Size()))
	}
	check(t, l.Close())
	raw, err := os.ReadFile(path)
	check(t, err)

	wrappedDir := t.TempDir()
	wrapped, err := Open(wrappedDir, Options{})
	check(t, err)
	payload := append(slices.Clone(raw[start[1]:start[2]]), "opaque"...)
	check(t, wrapped.Append(Record{Payload: payload}))
	check(t, wrapped.Close())
	wrapper, err := os.ReadFile(filepath.Join(wrappedDir, "shard-0000000000000001.evl"))
	check(t, err)
	for _, tc := range []struct {
		name string
		edit func([]byte) []byte
		want int
	}{
		{"short frame", func(b []byte) []byte { return b[:len(b)-3] }, 2},
		{"tail CRC", func(b []byte) []byte { b[start[2]+12] ^= 1; return b }, 2},
		{"tail length", func(b []byte) []byte { b[start[2]+4]++; return b }, 2},
		{"middle CRC", func(b []byte) []byte { b[start[1]+12] ^= 1; return b }, -1},
		{"middle length with valid tail", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[start[0]+4:], ^uint32(0))
			return b
		}, -1},
		{"middle length and torn tail", func(b []byte) []byte {
			binary.LittleEndian.PutUint32(b[start[0]+4:], ^uint32(0))
			return b[:len(b)-3]
		}, -1},
		{"torn tail with embedded frame", func(b []byte) []byte {
			return append(b[:start[2]], wrapper[fileHeaderSize:len(wrapper)-3]...)
		}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			file := filepath.Join(dir, filepath.Base(path))
			damaged := tc.edit(slices.Clone(raw))
			check(t, os.WriteFile(file, damaged, 0o600))
			log, err := Open(dir, Options{})
			if tc.want < 0 {
				wantError(t, err, ErrCorrupt)
				got, err := os.ReadFile(file)
				check(t, err)
				if !bytes.Equal(got, damaged) {
					t.Fatal("Open modified a corrupt middle frame")
				}
				return
			}
			check(t, err)
			defer log.Close()
			got, err := os.ReadFile(file)
			check(t, err)
			if !bytes.Equal(got, raw[:start[tc.want]]) || len(timestamps(t, log)) != tc.want {
				t.Fatal("Open did not truncate to the last valid frame")
			}
		})
	}
	old := append(slices.Clone(raw), "EVF"...)
	check(t, os.WriteFile(path, old, 0o600))
	short := filepath.Join(source, "shard-0000000000000002.evl")
	check(t, os.WriteFile(short, []byte("STEV"), 0o600))
	l, err = Open(source, Options{})
	check(t, err)
	put(t, l, 4)
	check(t, l.Close())
	files := shardFiles(t, source)
	if !slices.Equal(files, []string{path, filepath.Join(source, "shard-0000000000000003.evl")}) {
		t.Fatalf("Open kept the short shard or resumed its predecessor: %v", files)
	}
	got, err := os.ReadFile(path)
	check(t, err)
	if !bytes.Equal(got, old) {
		t.Fatal("Open changed the previous shard after removing a short newest shard")
	}
	raw[0] ^= 1
	check(t, os.WriteFile(path, raw, 0o600))
	_, err = Open(source, Options{})
	wantError(t, err, ErrCorrupt)
}

func TestFailedRotationPreservesSyncedHistory(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxRecordBytes: 8, MaxBatchBytes: 64, ShardMaxBytes: 62, MaxShards: 1}
	l, err := Open(dir, opts)
	check(t, err)
	defer l.Close()
	put(t, l, 1)
	check(t, l.Sync())
	old := filepath.Join(dir, "shard-0000000000000001.evl")
	raw, err := os.ReadFile(old)
	check(t, err)
	blocker := filepath.Join(dir, "shard-0000000000000002.evl")
	check(t, os.Mkdir(blocker, 0o700))
	check(t, l.Append(Record{TimestampNS: 2}))
	if err := l.Flush(); err == nil {
		t.Fatal("rotation accepted a pre-existing path")
	}
	got, err := os.ReadFile(old)
	check(t, err)
	if !bytes.Equal(got, raw) || !slices.Equal(timestamps(t, l), []int64{1}) {
		t.Fatal("failed rotation changed previously synced history")
	}
	if err := l.Close(); err == nil {
		t.Fatal("Close did not report the blocked pending frame")
	}
	check(t, os.Remove(blocker))
	l, err = Open(dir, opts)
	check(t, err)
	defer l.Close()
	if got := timestamps(t, l); !slices.Equal(got, []int64{1}) {
		t.Fatalf("reopened history = %v", got)
	}
	put(t, l, 2)
	if got := timestamps(t, l); !slices.Equal(got, []int64{2}) {
		t.Fatalf("history after successful rotation = %v", got)
	}
}

func TestScanDoesNotRepairCorruptShard(t *testing.T) {
	dir := t.TempDir()
	opts := Options{MaxRecordBytes: 8, MaxBatchBytes: 64, ShardMaxBytes: 62}
	l, err := Open(dir, opts)
	check(t, err)
	put(t, l, 1)
	put(t, l, 2)
	check(t, l.Close())
	file := filepath.Join(dir, "shard-0000000000000001.evl")
	raw, err := os.ReadFile(file)
	check(t, err)
	raw[fileHeaderSize+frameHeaderSize+recordHeaderSize] ^= 1
	check(t, os.WriteFile(file, raw, 0o600))
	l, err = Open(dir, opts)
	check(t, err)
	defer l.Close()
	r, err := l.Scan(t.Context(), ScanOptions{}, func(r Record) error {
		if r.TimestampNS != 2 {
			t.Fatalf("Scan delivered a corrupt record: %+v", r)
		}
		return nil
	})
	wantError(t, err, ErrCorrupt)
	got, err := os.ReadFile(file)
	check(t, err)
	if r.Records != 1 || !bytes.Equal(raw, got) {
		t.Fatalf("Scan delivered or repaired a corrupt frame: %+v", r)
	}
}

func TestStorageBudgets(t *testing.T) {
	for _, opts := range []Options{
		{MaxRecordBytes: 8, MaxBatchBytes: 64, ShardMaxBytes: 62, MaxShards: 2},
		{MaxRecordBytes: 8, MaxBatchBytes: 64, ShardMaxBytes: 62, MaxTotalBytes: 124},
	} {
		dir := t.TempDir()
		l, err := Open(dir, opts)
		check(t, err)
		put(t, l, 1)
		put(t, l, 2)
		r, err := l.Scan(t.Context(), ScanOptions{}, func(got Record) error {
			if got.TimestampNS != 2 {
				t.Fatalf("unexpected visible shard: %d", got.TimestampNS)
			}
			put(t, l, 3)
			return nil
		})
		check(t, err)
		if !r.Truncated || r.Records != 1 || r.Stopped || r.LimitReached {
			t.Fatalf("eviction during scan: %+v", r)
		}
		if len(shardFiles(t, dir)) != 2 || !slices.Equal(timestamps(t, l), []int64{3, 2}) {
			t.Fatal("retention failed")
		}
		check(t, l.Close())
	}
}

func TestAdmissionAndDirectoryLock(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    Options
		payload int
	}{
		{"TotalBudget", Options{MaxTotalBytes: 1 << 20}, 4 << 10},
		{"TotalBelowDefaultBatch", Options{MaxTotalBytes: 8 << 10}, 4 << 10},
		{"TotalBelowDefaultRecord", Options{MaxTotalBytes: 1024}, 1024 - fileHeaderSize - minFrameSize},
		{"LargeRecord", Options{MaxRecordBytes: 64 << 10}, 64 << 10},
		{"SmallBatch", Options{MaxBatchBytes: 2 << 10}, (2 << 10) - minFrameSize},
		{"LargeShard", Options{ShardMaxBytes: 64 << 20}, 4 << 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, err := Open(t.TempDir(), tc.opts)
			check(t, err)
			defer l.Close()
			check(t, l.Append(Record{TimestampNS: 1, Payload: make([]byte, tc.payload)}))
			check(t, l.Flush())
			if got := timestamps(t, l); !slices.Equal(got, []int64{1}) {
				t.Fatalf("written history = %v", got)
			}
		})
	}
	dir := t.TempDir()
	for _, opts := range []Options{
		{MaxRecordBytes: 8, MaxBatchBytes: 45},
		{MaxTotalBytes: 32},
		{MaxTotalBytes: 1 << 20, ShardMaxBytes: 2 << 20},
	} {
		_, err := Open(dir, opts)
		wantError(t, err, ErrInvalidOption)
	}
	l, err := Open(dir, Options{MaxRecordBytes: 8, MaxBatchBytes: 64, MaxBatchCount: 2})
	check(t, err)
	defer l.Close()
	_, err = Open(dir, Options{})
	wantError(t, err, ErrLocked)
	a := Record{TimestampNS: 1}
	big := Record{Payload: make([]byte, 9)}
	wantError(t, l.Append(a, big), ErrRecordTooLarge)
	wantError(t, l.Append(a, a, a), ErrRecordTooLarge)
	check(t, l.Append(a))
	wantError(t, l.Append(a, a), ErrFull)
	check(t, l.Flush())
	if got := timestamps(t, l); !slices.Equal(got, []int64{1}) {
		t.Fatalf("partial append was admitted: %v", got)
	}
	check(t, l.Close())
	wantError(t, l.Append(a), ErrClosed)
	wantError(t, l.Flush(), ErrClosed)
	wantError(t, l.Sync(), ErrClosed)
	_, err = l.Scan(t.Context(), ScanOptions{}, func(Record) error { return nil })
	wantError(t, err, ErrClosed)
}

func TestConcurrentAppendFlushScanClose(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{MaxConcurrentScans: 1})
	check(t, err)
	put(t, l, 1)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	scanned := make(chan error, 1)
	go func() {
		_, err := l.Scan(t.Context(), ScanOptions{}, func(Record) error {
			close(entered)
			<-release
			return nil
		})
		scanned <- err
	}()
	await(t, entered)
	closed := make(chan error, 1)
	failures := make(chan error, 6)
	var wg sync.WaitGroup
	for id := range 6 {
		wg.Go(func() {
			for range 32 {
				var err error
				switch id % 3 {
				case 0:
					err = l.Append(Record{TimestampNS: 2})
				case 1:
					err = l.Flush()
				case 2:
					_, err = l.Scan(t.Context(), ScanOptions{}, func(Record) error { return nil })
				}
				if err != nil && err != ErrClosed && err != ErrFull {
					failures <- err
					return
				}
			}
		})
	}
	go func() { closed <- l.Close() }()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	timeout := time.After(2 * time.Second)
	for l.Append() != ErrClosed {
		select {
		case <-tick.C:
		case <-timeout:
			t.Fatal("Close did not start")
		}
	}
	_, err = Open(dir, Options{})
	wantError(t, err, ErrLocked)
	unblock()
	for _, result := range []<-chan error{scanned, closed} {
		select {
		case err := <-result:
			check(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("Scan or Close did not exit")
		}
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
}

func BenchmarkEventlogAppendFlush(b *testing.B) {
	const batchSize, payloadBytes = 256, 64
	l, err := Open(b.TempDir(), Options{})
	check(b, err)
	b.Cleanup(func() { check(b, l.Close()) })
	records := slices.Repeat([]Record{{Payload: make([]byte, payloadBytes)}}, batchSize)
	b.ReportAllocs()
	b.SetBytes(batchSize * payloadBytes)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for b.Loop() {
		if err := l.Append(records...); err != nil {
			b.Fatal(err)
		}
		if err := l.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)
	events := float64(b.N) * batchSize
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/events, "ns/event")
	b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/events, "B/event")
	b.ReportMetric(float64(after.Mallocs-before.Mallocs)/events, "allocs/event")
}

func BenchmarkEventlogScanRecent(b *testing.B) {
	run := func(b *testing.B, batchSize int) {
		const shardBytes, payloadBytes = 4 << 20, 64
		frameBytes := frameOverhead + batchSize*(recordHeaderSize+payloadBytes)
		batches := (shardBytes - fileHeaderSize) / frameBytes
		l, err := Open(b.TempDir(), Options{ShardMaxBytes: shardBytes})
		check(b, err)
		b.Cleanup(func() { check(b, l.Close()) })
		records := slices.Repeat([]Record{{Payload: make([]byte, payloadBytes)}}, batchSize)
		for range batches {
			check(b, l.Append(records...))
			check(b, l.Flush())
		}
		check(b, l.Sync())

		for _, tc := range []struct {
			name  string
			limit int
			want  int
		}{
			{"Recent200", 200, 200},
			{"All", 0, batches * batchSize},
		} {
			b.Run(tc.name, func(b *testing.B) {
				b.ReportAllocs()
				ctx := b.Context()
				opts := ScanOptions{MaxRecords: tc.limit}
				visit := func(Record) error { return nil }
				for b.Loop() {
					result, err := l.Scan(ctx, opts, visit)
					if err != nil || result.Records != tc.want {
						b.Fatalf("scan: got %d records, want %d, error = %v", result.Records, tc.want, err)
					}
				}
			})
		}
	}
	run(b, 256)
	b.Run("SmallFrames", func(b *testing.B) { run(b, 20) })
}

func check(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func wantError(t *testing.T, got, want error) {
	t.Helper()
	if !errors.Is(got, want) {
		t.Fatalf("error = %v, want %v", got, want)
	}
}

func put(t *testing.T, l *Log, ts int64) {
	t.Helper()
	check(t, l.Append(Record{TimestampNS: ts, Payload: []byte("12345678")}))
	check(t, l.Flush())
}

func timestamps(t *testing.T, l *Log) []int64 {
	t.Helper()
	var got []int64
	_, err := l.Scan(t.Context(), ScanOptions{}, func(r Record) error {
		got = append(got, r.TimestampNS)
		return nil
	})
	check(t, err)
	return got
}

func shardFiles(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "shard-*.evl"))
	check(t, err)
	return files
}

func await(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for scan or close")
	}
}
