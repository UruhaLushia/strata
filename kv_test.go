package strata

import (
	"bytes"
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/UruhaLushia/strata/codec"
	"github.com/UruhaLushia/strata/codec/zstd"
)

func TestKVRegionsLazyAllocationAndIsolation(t *testing.T) {
	now := time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC)
	db, series := testDB(t, &now, Options{})
	defer db.Close()
	wal, next := db.walPos, db.kvNext
	lazy := mustRegion(t, db, "lazy")
	checkUnallocated := func() {
		t.Helper()
		if db.walPos != wal || db.kvNext != next || db.kvCatalog["lazy"] != 0 {
			t.Fatalf("lazy region allocated: wal=%d next=%d id=%d", db.walPos-wal, db.kvNext, db.kvCatalog["lazy"])
		}
	}
	checkUnallocated()
	assertKV(t, lazy, "k", nil, false)
	requireOK(t, lazy.Delete("k"))
	requireOK(t, lazy.Clear())
	checkUnallocated()

	failure := errors.New("sync failure")
	db.syncHook = func() error { return failure }
	if err := lazy.Put("k", []byte("value")); !errors.Is(err, failure) {
		t.Fatalf("first Put error=%v", err)
	}
	checkUnallocated()
	if ok, err := lazy.PutIfAbsent("k", nil); ok || !errors.Is(err, failure) {
		t.Fatalf("first PutIfAbsent ok=%v err=%v", ok, err)
	}
	checkUnallocated()
	db.syncHook = nil

	seq := db.allocatedSeq
	requireOK(t, lazy.Put("k", []byte("left")))
	if db.allocatedSeq != seq+1 || db.kvCatalog["lazy"] == 0 {
		t.Fatalf("first Put must allocate in one WAL frame, frames=%d", db.allocatedSeq-seq)
	}
	other := mustRegion(t, db, "other")
	if ok, err := other.PutIfAbsent("k", []byte("right")); err != nil || !ok || db.allocatedSeq != seq+2 || db.kvCatalog["other"] == 0 {
		t.Fatalf("first PutIfAbsent ok=%v err=%v frames=%d", ok, err, db.allocatedSeq-seq-1)
	}
	requireOK(t, lazy.Clear())
	assertKV(t, lazy, "k", nil, false)
	appendValue(t, series, now.Add(-time.Minute), 7)
	requireOK(t, series.ResetAll())
	assertKV(t, other, "k", []byte("right"), true)

	ro := reopenTestDB(t, nil, db.path, Options{ReadOnly: true})
	defer ro.Close()
	missing := mustRegion(t, ro, "never-written")
	for _, err := range []error{missing.Delete("k"), missing.Clear(), ro.NewBatch().Commit()} {
		if !errors.Is(err, ErrReadOnly) {
			t.Fatalf("read-only no-op err=%v", err)
		}
	}
}

func TestKVBatchAtomicityStickyErrorsAndDurability(t *testing.T) {
	now := time.Date(2026, 4, 3, 12, 0, 0, 0, time.UTC)
	db, series := testDB(t, &now, Options{})
	defer func() { _ = db.Close() }()
	client := NewClient(db, ClientOptions{})
	defer client.Close()
	left, right := mustRegion(t, db, "left"), mustRegion(t, db, "right")
	foreign := &Region{db: &DB{}, name: "foreign"}
	value := []byte("v")
	for _, tc := range []struct {
		name  string
		batch *Batch
		want  error
	}{
		{"missing region", db.NewBatch().Put("k", value), ErrInvalidRegion},
		{"foreign region", db.NewBatch().In(foreign).Put("k", value), ErrInvalidRegion},
		{"sticky after valid region", db.NewBatch().In(nil).In(left).Put("k", value), ErrInvalidRegion},
		{"first error wins", left.NewBatch().PutTTL("k", value, 0).In(foreign), ErrInvalidTTL},
	} {
		if err := tc.batch.Commit(); !errors.Is(err, tc.want) {
			t.Fatalf("%s err=%v want=%v", tc.name, err, tc.want)
		}
	}

	wal := db.walPos
	next := db.kvNext
	batch := func() *Batch {
		return client.NewBatch().In(left).Put("k", value).In(right).Put("k", value)
	}
	failure := errors.New("sync failure")
	db.syncHook = func() error { return failure }
	if err := batch().Commit(); !errors.Is(err, failure) {
		t.Fatalf("batch commit err=%v", err)
	}
	db.syncHook = nil
	if db.walPos != wal || db.kvNext != next || db.kvCatalog["left"] != 0 || db.kvCatalog["right"] != 0 {
		t.Fatal("failed batch allocated regions or advanced WAL")
	}
	assertKV(t, left, "k", nil, false)
	assertKV(t, right, "k", nil, false)
	seq := db.allocatedSeq
	requireOK(t, batch().Commit())
	if db.allocatedSeq != seq+1 {
		t.Fatalf("cross-region batch frames=%d", db.allocatedSeq-seq)
	}
	assertKV(t, left, "k", value, true)
	assertKV(t, right, "k", value, true)

	requireOK(t, left.NewBatch().Put("drop", value).Clear().Commit())
	requireOK(t, right.NewBatch().Put("drop", value).Clear().Commit())
	assertKV(t, left, "drop", nil, false)
	assertKV(t, right, "drop", nil, false)
	requireOK(t, left.Put("keep", value))
	appendValue(t, series, now.Add(-2*time.Hour), 1)
	requireOK(t, db.Compact())
	if db.head.sections[secKVSSTIndex].length == 0 || db.head.sections[secTSInvert].length == 0 {
		t.Fatal("expected both KV index and TS inversion sections")
	}

	db = reopenTestDB(t, db, db.path, db.opts)
	left, right = mustRegion(t, db, "left"), mustRegion(t, db, "right")
	assertKV(t, left, "k", nil, false)
	assertKV(t, left, "drop", nil, false)
	assertKV(t, left, "keep", value, true)
	assertKV(t, right, "k", nil, false)
	assertKV(t, right, "drop", nil, false)
}

func TestKVMixedTSCompactAndReaderRefresh(t *testing.T) {
	for _, tc := range []struct {
		name string
		mmap bool
	}{{"readat", false}, {"mmap", true}} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Date(2026, 4, 4, 12, 0, 0, 0, time.UTC)
			writer, series := testDB(t, &now, Options{})
			defer writer.Close()
			reader := reopenTestDB(t, nil, writer.path, Options{ReadOnly: true, UseMmap: tc.mmap})
			defer reader.Close()
			later := mustRegion(t, reader, "later")
			assertKV(t, later, "k", nil, false)

			r := mustRegion(t, writer, "later")
			appendValue(t, series, now.Add(-2*time.Hour), 3)
			requireOK(t, r.Put("k", []byte("v")))
			writer.compactHook = func() {
				appendValue(t, series, now.Add(-time.Minute), 4)
				requireOK(t, r.Put("hot", []byte("tail")))
			}
			requireOK(t, writer.Compact())
			requireOK(t, reader.Refresh())
			assertKV(t, later, "k", []byte("v"), true)
			assertKV(t, later, "hot", []byte("tail"), true)
			assertTSSum(t, mustSeries(t, reader, series.id, series.cfg), now.Add(-3*time.Hour), now, 7)
		})
	}
}

func TestKVTTLAndCASStateMachine(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	db, _ := testDB(t, &now, Options{})
	defer func() { _ = db.Close() }()
	r := mustRegion(t, db, "cas")
	v := []byte("v")
	pia := func() (bool, error) { return r.PutIfAbsent("k", nil) }
	cas := func(old, new []byte) func() (bool, error) {
		return func() (bool, error) { return r.CompareAndSwap("k", old, new) }
	}
	cad := func(old []byte) func() (bool, error) {
		return func() (bool, error) { return r.CompareAndDelete("k", old) }
	}
	for _, step := range []struct {
		name   string
		write  func() (bool, error)
		ok     bool
		value  []byte
		exists bool
	}{
		{"unallocated CAS", cas(nil, v), false, nil, false},
		{"unallocated CAD", cad(nil), false, nil, false},
		{"insert nil", pia, true, nil, true},
		{"duplicate", pia, false, nil, true},
		{"empty equals nil", cas([]byte{}, v), true, v, true},
		{"mismatched CAS", cas(nil, v), false, v, true},
		{"CAS stores nil", cas(v, nil), true, nil, true},
		{"mismatched CAD", cad(v), false, nil, true},
		{"CAD deletes empty", cad([]byte{}), true, nil, false},
		{"CAS on missing key", cas(nil, v), false, nil, false},
		{"CAD on missing key", cad(nil), false, nil, false},
	} {
		wal := db.walPos
		if ok, err := step.write(); err != nil || ok != step.ok || !ok && db.walPos != wal {
			t.Fatalf("%s ok=%v err=%v walDelta=%d", step.name, ok, err, db.walPos-wal)
		}
		assertKV(t, r, "k", step.value, step.exists)
	}

	for _, ttl := range []time.Duration{0, -time.Second} {
		if err := r.PutTTL("tmp", v, ttl); !errors.Is(err, ErrInvalidTTL) {
			t.Fatalf("ttl=%v err=%v", ttl, err)
		}
	}
	requireOK(t, r.PutTTL("tmp", v, time.Minute))
	requireOK(t, r.PutTTL("max", v, time.Duration(math.MaxInt64)))
	if expires := db.kvOverlay[kvKey{region: db.kvCatalog["cas"], key: "max"}].expiresAtUnixNano; expires != math.MaxInt64 {
		t.Fatalf("maximum TTL expiration=%d want=%d", expires, int64(math.MaxInt64))
	}
	assertKV(t, r, "tmp", v, true)
	assertKV(t, r, "max", v, true)
	now = now.Add(2 * time.Minute)
	assertKV(t, r, "tmp", nil, false)
	if ok, err := r.PutIfAbsent("tmp", v); err != nil || !ok {
		t.Fatalf("PutIfAbsent after expiry ok=%v err=%v", ok, err)
	}
	requireOK(t, db.Compact())
	assertKV(t, r, "max", v, true)
	opts := db.opts
	opts.ReadOnly = true
	db = reopenTestDB(t, db, db.path, opts)
	r = mustRegion(t, db, "cas")
	assertKV(t, r, "max", v, true)
	assertKV(t, r, "tmp", v, true)
	for _, write := range []func() (bool, error){
		pia,
		func() (bool, error) { return r.CompareAndSwap("tmp", v, nil) },
		func() (bool, error) { return r.CompareAndDelete("tmp", v) },
	} {
		if ok, err := write(); ok || !errors.Is(err, ErrReadOnly) {
			t.Fatalf("read-only conditional write ok=%v err=%v", ok, err)
		}
	}
}

func TestKVScanScanSeqAndLargeBlobs(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	db, _ := testDB(t, &now, Options{})
	defer db.Close()
	r, other := mustRegion(t, db, "items"), mustRegion(t, db, "other")
	blob := bytes.Repeat([]byte("large-value-"), 4000)
	requireOK(t, db.NewBatch().In(r).
		Put("a/1", []byte("sst")).Put("a/2", nil).Put("a/3", nil).Put("a/4", nil).
		Put("a/5", blob).Put("\xff\xff/1", nil).
		In(other).Put("a/0", []byte("foreign")).Commit())
	requireOK(t, db.Compact())
	blobSection := db.head.sections[secKVLargeBlobs]
	if blobSection.kind != secKVLargeBlobs || blobSection.length == 0 {
		t.Fatalf("missing large blob section: %+v", blobSection)
	}
	requireOK(t, r.NewBatch().Put("a/2", []byte("overlay")).Put("a/2b", nil).
		Delete("a/3").PutTTL("a/4", nil, time.Minute).Commit())
	now = now.Add(2 * time.Minute)

	ctx := t.Context()
	prefix := KVScan{Prefix: "a/"}
	scanKeys := func(region *Region, q KVScan, visit func(KVItem)) []string {
		t.Helper()
		var keys []string
		for item, err := range region.ScanSeq(ctx, q) {
			requireOK(t, err)
			keys = append(keys, item.Key)
			if visit != nil {
				visit(item)
			}
		}
		return keys
	}
	want := []string{"a/1", "a/2", "a/2b", "a/5"}
	overlay := map[string][]byte{"a/1": []byte("sst"), "a/2": []byte("overlay"), "a/5": blob}
	if got := scanKeys(r, prefix, func(it KVItem) {
		if v := overlay[it.Key]; !bytes.Equal(it.Value, v) {
			t.Fatalf("ScanSeq(%q) len=%d want=%d", it.Key, len(it.Value), len(v))
		}
	}); !slices.Equal(got, want) {
		t.Fatalf("merged scan=%v want=%v", got, want)
	}
	for _, tc := range []struct {
		query KVScan
		want  []string
	}{
		{KVScan{Prefix: "a/", Start: "a/2", End: "a/5"}, want[1:3]},
		{KVScan{Prefix: "a/", Start: "a/9", End: "a/5"}, nil},
		{KVScan{Prefix: "\xff\xff"}, []string{"\xff\xff/1"}},
		{KVScan{Prefix: "a/", Limit: 2}, want[:2]},
	} {
		if got := scanKeys(r, tc.query, nil); !slices.Equal(got, tc.want) {
			t.Fatalf("scan %+v=%v want=%v", tc.query, got, tc.want)
		}
	}

	db.head.sections[secKVLargeBlobs] = sectionDesc{}
	keysOnly := KVScan{Prefix: "a/", KeysOnly: true}
	if got := scanKeys(r, keysOnly, func(it KVItem) {
		if it.Value != nil {
			t.Fatalf("KeysOnly loaded %q", it.Key)
		}
	}); !slices.Equal(got, want) {
		t.Fatalf("KeysOnly=%v want=%v", got, want)
	}
	if err := r.Scan(ctx, prefix, nil); err == nil {
		t.Fatal("scan accepted missing blob section")
	}
	db.head.sections[secKVLargeBlobs] = blobSection

	sentinel := errors.New("callback failure")
	for _, stop := range []error{ErrStopScan, sentinel} {
		seen := 0
		err := r.Scan(ctx, prefix, func(KVItem) error { seen++; return stop })
		if seen != 1 || stop == ErrStopScan && err != nil || stop == sentinel && !errors.Is(err, sentinel) {
			t.Fatalf("stop=%v seen=%d err=%v", stop, seen, err)
		}
	}
	for item, err := range r.ScanSeq(ctx, prefix) {
		requireOK(t, err)
		if item.Key != want[0] {
			t.Fatalf("first ScanSeq key=%q want=%q", item.Key, want[0])
		}
		break
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	seen := 0
	if err := r.Scan(cancelCtx, prefix, func(KVItem) error { seen++; cancel(); return nil }); !errors.Is(err, context.Canceled) || seen != 1 {
		t.Fatalf("mid-scan cancellation seen=%d err=%v", seen, err)
	}
	if err := r.Scan(cancelCtx, prefix, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled Scan err=%v", err)
	}

	// The callback writes to the same region while iterating its snapshot.
	if got := scanKeys(r, prefix, func(it KVItem) {
		if it.Key == "a/1" {
			requireOK(t, r.NewBatch().Put("a/0", nil).Delete("a/2").Commit())
		}
	}); !slices.Equal(got, want) {
		t.Fatalf("snapshot keys=%v want=%v", got, want)
	}
	requireOK(t, db.Close())
	for _, mmap := range []bool{false, true} {
		ro := reopenTestDB(t, nil, db.path, Options{Now: db.opts.Now, ReadOnly: true, UseMmap: mmap})
		defer ro.Close()
		items := mustRegion(t, ro, "items")
		assertKV(t, items, "a/5", blob, true)
		if got := scanKeys(items, keysOnly, nil); !slices.Equal(got, []string{"a/0", "a/1", "a/2b", "a/5"}) {
			t.Fatalf("reopen mmap=%v keys=%v", mmap, got)
		}
	}
}

func TestKVSectionPassthrough(t *testing.T) {
	t.Run("EmptyAndCatalogOnly", func(t *testing.T) {
		now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
		db, s := testDB(t, &now, Options{})
		defer db.Close()
		if !canPassthroughKV(db.head, 0, db.codecs.target) {
			t.Fatal("empty KV is not eligible for passthrough")
		}
		r := mustRegion(t, db, "catalog")
		requireOK(t, r.NewBatch().Put("a", nil).Clear().Commit())
		requireOK(t, db.Compact())
		saved := db.head
		if saved.sections[secKVSSTBlocks].length != 0 || saved.sections[secKVLargeBlobs].length != 0 ||
			saved.sections[secKVSSTIndex].length == 0 || !canPassthroughKV(saved, saved.sealedKVRev, db.codecs.target) {
			t.Fatalf("invalid catalog-only sections: %+v", saved.sections[:3])
		}
		appendValue(t, s, now.Add(-time.Hour), 1)
		requireOK(t, db.Compact())
		if db.head.gen != saved.gen+1 || !slices.Equal(db.head.sections[:3], saved.sections[:3]) {
			t.Fatalf("passthrough changed catalog-only sections: %+v", db.head.sections[:3])
		}
		assertKV(t, r, "a", nil, false)
	})

	t.Run("CopyUnchangedThenSealTail", func(t *testing.T) {
		now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
		db, s := testDB(t, &now, Options{})
		defer func() { _ = db.Close() }()
		r, other := mustRegion(t, db, "kv"), mustRegion(t, db, "other")
		// Incompressible data spans multiple 64 KiB copy chunks.
		blob := make([]byte, 80<<10)
		rng := rand.NewChaCha8([32]byte{1})
		_, _ = rng.Read(blob)
		requireOK(t, r.NewBatch().Put("big", blob).Put("delete", nil).In(other).Put("clear", nil).Commit())
		requireOK(t, db.Compact())
		saved := db.head
		if saved.sections[secKVLargeBlobs].length <= 64<<10 || !canPassthroughKV(saved, saved.sealedKVRev, db.codecs.target) {
			t.Fatalf("invalid passthrough fixture: %+v", saved.sections[:3])
		}

		appendValue(t, s, now.Add(-2*time.Hour), 42)
		db.compactHook = func() {
			requireOK(t, r.NewBatch().Put("hot", nil).Delete("delete").In(other).Clear().Commit())
		}
		requireOK(t, db.Compact())
		db.compactHook = nil
		if db.head.gen != saved.gen+1 || db.head.sealedKVRev != saved.sealedKVRev ||
			!slices.Equal(db.head.sections[:3], saved.sections[:3]) {
			t.Fatalf("passthrough changed sealed KV: %+v", db.head.sections[:3])
		}
		assertKV(t, r, "hot", nil, true)
		assertKV(t, r, "delete", nil, false)
		assertKV(t, other, "clear", nil, false)

		dst, err := openData(filepath.Join(t.TempDir(), "bad.tmp"), false)
		requireOK(t, err)
		defer dst.Close()
		bad := db.head
		bad.sections[secKVLargeBlobs].crc ^= 1
		if _, _, _, err := copyPassthroughKVSections(dst, db.data, bad); err == nil {
			t.Fatal("copied KV section with corrupt CRC")
		}

		requireOK(t, db.Compact())
		if db.head.sealedKVRev <= saved.sealedKVRev || db.head.sections[secKVSSTBlocks] == saved.sections[secKVSSTBlocks] {
			t.Fatalf("KV update did not rewrite sealed blocks: revision=%d", db.head.sealedKVRev)
		}
		db = reopenTestDB(t, db, db.path, db.opts)
		r, other = mustRegion(t, db, "kv"), mustRegion(t, db, "other")
		assertKV(t, r, "big", blob, true)
		assertKV(t, r, "hot", nil, true)
		assertKV(t, r, "delete", nil, false)
		assertKV(t, other, "clear", nil, false)
		assertTSSum(t, mustSeries(t, db, s.id, s.cfg), now.Add(-3*time.Hour), now, 42)
	})

	t.Run("LiveTTLAndLegacyFlagsAcrossReopen", func(t *testing.T) {
		now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
		db, _ := testDB(t, &now, Options{})
		defer func() { _ = db.Close() }()
		r := mustRegion(t, db, "sess")
		requireOK(t, r.NewBatch().Put("keep", nil).PutTTL("temp", nil, 2*time.Minute).Commit())
		requireOK(t, db.Compact())
		db = reopenTestDB(t, db, db.path, db.opts)
		check := func(wantFlags uint8, wantCopy bool) {
			t.Helper()
			flags := db.head.sections[secKVSSTIndex].flags
			if flags&(kvFlagTTLChecked|kvFlagHasLiveTTL) != wantFlags || canPassthroughKV(db.head, db.head.sealedKVRev, db.codecs.target) != wantCopy {
				t.Fatalf("flags=%#x want=%#x passthrough want=%v", flags, wantFlags, wantCopy)
			}
		}
		check(kvFlagTTLChecked|kvFlagHasLiveTTL, false)
		assertKV(t, mustRegion(t, db, "sess"), "temp", nil, true)
		now = now.Add(5 * time.Minute)
		requireOK(t, db.Compact())
		check(kvFlagTTLChecked, true)
		r = mustRegion(t, db, "sess")
		assertKV(t, r, "temp", nil, false)
		assertKV(t, r, "keep", nil, true)

		requireOK(t, db.Close())
		patchSuperblocks(t, db.path, func(_ *osFile, sb *superblock) {
			sb.sections[secKVSSTIndex].flags = 0
		})
		db = reopenTestDB(t, nil, db.path, db.opts)
		check(0, false)
		requireOK(t, db.Compact())
		check(kvFlagTTLChecked, true)
		assertKV(t, mustRegion(t, db, "sess"), "keep", nil, true)
	})
}

func TestKVSSTIndexCompressionAndCodecError(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	db, s := testDB(t, &now, Options{Codec: zstd.New(zstd.Fastest)})
	defer func() { _ = db.Close() }()
	r := mustRegion(t, db, "idx")
	requireOK(t, r.Put("a", []byte("1")))
	requireOK(t, db.Compact())
	idx := db.head.sections[secKVSSTIndex]
	if idx.kind != secKVSSTIndex || idx.count != 1 || idx.codec != 0 || idx.length != uint64(idx.rawLen) {
		t.Fatalf("tiny index should stay raw: %+v", idx)
	}
	db = reopenTestDB(t, db, db.path, db.opts)
	r, s = mustRegion(t, db, "idx"), mustSeries(t, db, s.id, s.cfg)
	assertKV(t, r, "a", []byte("1"), true)

	prefix := strings.Repeat("index-", 8)
	key := func(i int) string { return prefix + strconv.Itoa(10000+i) }
	value := func(i int) []byte { return bytes.Repeat([]byte{byte('a' + i%26)}, 320) }
	for base := 0; base < 1000; base += 250 {
		b := r.NewBatch()
		for i := base; i < base+250; i++ {
			b.Put(key(i), value(i))
		}
		requireOK(t, b.Commit())
	}
	requireOK(t, db.Compact())
	idx = db.head.sections[secKVSSTIndex]
	if idx.count < 10 || idx.codec != 1 || idx.length == 0 || idx.length >= uint64(idx.rawLen) {
		t.Fatalf("multi-block index should be compressed: %+v", idx)
	}
	appendValue(t, s, now.Add(-2*time.Hour), 77)
	requireOK(t, db.Compact())
	if got := db.head.sections[secKVSSTIndex]; got != idx {
		t.Fatalf("passthrough changed compressed index: %+v", got)
	}

	db = reopenTestDB(t, db, db.path, db.opts)
	r = mustRegion(t, db, "idx")
	assertKV(t, r, "a", []byte("1"), true)
	for _, i := range []int{0, 123, 500, 999} {
		assertKV(t, r, key(i), value(i), true)
	}
	requireOK(t, db.Close())
	patchSuperblocks(t, db.path, func(_ *osFile, head *superblock) {
		head.sections[secKVSSTIndex].codec = 9
		head.gen++
	})
	bad, err := Open(db.path, db.opts)
	if err == nil {
		_ = bad.Close()
		t.Fatal("Open accepted unsupported KV index codec")
	}
	if !errors.Is(err, codec.ErrUnsupportedCodec) {
		t.Fatalf("unsupported codec err=%v", err)
	}
}
