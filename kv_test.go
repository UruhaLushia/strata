package strata

import (
	"bytes"
	"context"
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestKVRegionsLazyAllocationAndIsolation(t *testing.T) {
	now := time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "strata.db")
	db, err := Open(path, Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	series, err := db.Series(1, SeriesConfig{Arity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Region(""); !errors.Is(err, ErrInvalidRegion) {
		t.Fatalf("empty region name err=%v", err)
	}

	// Lazy region allocation: Region(), Delete(), Clear() on unallocated region must not touch WAL or catalog.
	walBefore := db.walPos
	lazy, err := db.Region("lazy")
	if err != nil {
		t.Fatal(err)
	}
	if db.walPos != walBefore || db.kvCatalog["lazy"] != 0 {
		t.Fatalf("Region() eagerly allocated walDelta=%d id=%d", db.walPos-walBefore, db.kvCatalog["lazy"])
	}
	if _, ok, err := lazy.Get("missing"); err != nil || ok {
		t.Fatalf("unallocated Get ok=%v err=%v", ok, err)
	}
	if err = lazy.Delete("missing"); err != nil || db.walPos != walBefore || db.kvCatalog["lazy"] != 0 {
		t.Fatalf("unallocated Delete err=%v walDelta=%d id=%d", err, db.walPos-walBefore, db.kvCatalog["lazy"])
	}
	if err = lazy.Clear(); err != nil || db.walPos != walBefore || db.kvCatalog["lazy"] != 0 {
		t.Fatalf("unallocated Clear err=%v walDelta=%d id=%d", err, db.walPos-walBefore, db.kvCatalog["lazy"])
	}

	// syncHook failure on first Put / PutIfAbsent must roll back catalog and kvNext cleanly.
	prevNext := db.kvNext
	db.syncHook = func() error { return errors.New("sync failure") }
	if err = lazy.Put("k1", []byte("v1")); err == nil {
		t.Fatal("expected sync failure on first Put")
	}
	if db.kvCatalog["lazy"] != 0 || db.kvNext != prevNext || db.walPos != walBefore {
		t.Fatalf("first Put sync failure leaked catalog id=%d next=%d walDelta=%d", db.kvCatalog["lazy"], db.kvNext, db.walPos-walBefore)
	}
	if ok, err := lazy.PutIfAbsent("k1", []byte("v1")); err == nil || ok {
		t.Fatalf("expected sync failure on first PutIfAbsent ok=%v err=%v", ok, err)
	}
	if db.kvCatalog["lazy"] != 0 || db.kvNext != prevNext || db.walPos != walBefore {
		t.Fatalf("first PutIfAbsent sync failure leaked catalog id=%d next=%d walDelta=%d", db.kvCatalog["lazy"], db.kvNext, db.walPos-walBefore)
	}
	db.syncHook = nil

	// First Put allocates catalog + data in a single WAL frame.
	seqBefore := db.allocatedSeq
	if err = lazy.Put("k1", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if db.allocatedSeq != seqBefore+1 || db.kvCatalog["lazy"] == 0 {
		t.Fatalf("first Put seqDelta=%d id=%d", db.allocatedSeq-seqBefore, db.kvCatalog["lazy"])
	}

	// First PutIfAbsent on a brand-new region allocates catalog + data in a single WAL frame.
	piaReg, err := db.Region("pia")
	if err != nil {
		t.Fatal(err)
	}
	seqBefore = db.allocatedSeq
	if ok, err := piaReg.PutIfAbsent("init", []byte("ok")); err != nil || !ok {
		t.Fatalf("first PutIfAbsent ok=%v err=%v", ok, err)
	}
	if db.allocatedSeq != seqBefore+1 || db.kvCatalog["pia"] == 0 {
		t.Fatalf("first PutIfAbsent seqDelta=%d id=%d", db.allocatedSeq-seqBefore, db.kvCatalog["pia"])
	}

	// Region isolation and TS ResetAll isolation.
	other, err := db.Region("other")
	if err != nil {
		t.Fatal(err)
	}
	if err = other.Put("x", []byte("keep")); err != nil {
		t.Fatal(err)
	}
	if err = lazy.Clear(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := lazy.Get("k1"); err != nil || ok {
		t.Fatalf("cleared key ok=%v err=%v", ok, err)
	}
	appendValue(t, series, now.Add(-time.Minute), 7)
	if err = series.ResetAll(); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := other.Get("x"); err != nil || !ok || string(v) != "keep" {
		t.Fatalf("isolation get=%q ok=%v err=%v", v, ok, err)
	}

	// Read-only DB still rejects no-op Delete/Clear and empty Batch with ErrReadOnly.
	ro, err := Open(path, Options{ReadOnly: true, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	roMissing, err := ro.Region("never-written")
	if err != nil {
		t.Fatal(err)
	}
	if err = roMissing.Delete("k"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only Delete err=%v", err)
	}
	if err = roMissing.Clear(); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only Clear err=%v", err)
	}
	if err = ro.NewBatch().Commit(); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only empty Batch.Commit err=%v", err)
	}
	if err = ro.Close(); err != nil {
		t.Fatal(err)
	}

	// Closed DB errors on Region, Get, Scan, and Batch.Commit.
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Region("after-close"); err == nil {
		t.Fatal("expected error from Region on closed DB")
	}
	if _, _, err = other.Get("x"); err == nil {
		t.Fatal("expected error from Get on closed DB")
	}
	if err = other.Scan(context.Background(), KVScan{}, nil); err == nil {
		t.Fatal("expected error from Scan on closed DB")
	}
	if err = db.NewBatch().Commit(); err == nil {
		t.Fatal("expected error from Batch.Commit on closed DB")
	}
}

func TestKVBatchAtomicityStickyErrorsAndDurability(t *testing.T) {
	now := time.Date(2026, 4, 3, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	path := filepath.Join(dir, "strata.db")
	client, err := OpenClient(path, ClientOptions{DB: Options{Now: func() time.Time { return now }}})
	if err != nil {
		t.Fatal(err)
	}
	db := client.DB()
	otherDB, err := Open(filepath.Join(dir, "other.db"), Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = otherDB.Close() }()

	left, err := client.Region("left")
	if err != nil {
		t.Fatal(err)
	}
	right, err := client.Region("right")
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := otherDB.Region("foreign")
	if err != nil {
		t.Fatal(err)
	}

	// Sticky errors: missing In, cross-DB region, invalid TTL, nil region, first error preserved.
	if err = db.NewBatch().Put("k", []byte("v")).Commit(); !errors.Is(err, ErrInvalidRegion) {
		t.Fatalf("Put without In err=%v", err)
	}
	if err = db.NewBatch().In(foreign).Put("k", []byte("v")).Commit(); !errors.Is(err, ErrInvalidRegion) {
		t.Fatalf("cross-DB In err=%v", err)
	}
	if err = db.NewBatch().In(nil).In(left).Put("k", []byte("v")).Commit(); !errors.Is(err, ErrInvalidRegion) {
		t.Fatalf("nil In sticky err=%v", err)
	}
	if err = left.PutTTL("k", []byte("v"), 0); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("PutTTL zero err=%v", err)
	}
	if err = left.PutTTL("k", []byte("v"), -time.Second); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("PutTTL negative err=%v", err)
	}
	if err = left.NewBatch().PutTTL("k", []byte("v"), 0).In(foreign).Commit(); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("first sticky error not preserved: err=%v", err)
	}

	// Cross-region atomic batch via Client.NewBatch() allocates both regions in one WAL frame.
	seqBefore := db.allocatedSeq
	if err = client.NewBatch().
		In(left).Put("k", []byte("v")).Put("k2", []byte("v2")).
		In(right).Put("k", []byte("w")).
		Commit(); err != nil {
		t.Fatal(err)
	}
	if db.allocatedSeq != seqBefore+1 {
		t.Fatalf("cross-region batch seqDelta=%d", db.allocatedSeq-seqBefore)
	}

	// Single-region batch via r.NewBatch() + syncHook rollback.
	db.syncHook = func() error { return errors.New("sync failure") }
	if err = left.NewBatch().Put("bad", []byte("value")).Commit(); err == nil {
		t.Fatal("sync failure was ignored")
	}
	db.syncHook = nil
	if _, ok, err := left.Get("bad"); err != nil || ok {
		t.Fatalf("failed batch changed memory ok=%v err=%v", ok, err)
	}

	if err = left.NewBatch().Delete("k").Put("keep", []byte("live")).Commit(); err != nil {
		t.Fatal(err)
	}
	if err = right.Clear(); err != nil {
		t.Fatal(err)
	}
	if err = client.Compact(); err != nil {
		t.Fatal(err)
	}
	if db.head.sections[0].length == 0 || db.head.sections[2].length == 0 {
		t.Fatalf("KV SST sections missing: %+v", db.head.sections)
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	left, _ = reopened.Region("left")
	right, _ = reopened.Region("right")
	if _, ok, err := left.Get("k"); err != nil || ok {
		t.Fatalf("deleted key recovered ok=%v err=%v", ok, err)
	}
	if _, ok, err := right.Get("k"); err != nil || ok {
		t.Fatalf("cleared key recovered ok=%v err=%v", ok, err)
	}
	if value, ok, err := left.Get("keep"); err != nil || !ok || string(value) != "live" {
		t.Fatalf("live key recovered value=%q ok=%v err=%v", value, ok, err)
	}
}

func TestKVBatchClearOrder(t *testing.T) {
	now := time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC)
	db, series := testDB(t, &now, Options{})
	region, err := db.Region("core")
	if err != nil {
		t.Fatal(err)
	}
	if err = region.NewBatch().Put("a", []byte("1")).Clear().Commit(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := region.Get("a"); err != nil || ok {
		t.Fatalf("put-clear left overlay value ok=%v err=%v", ok, err)
	}
	appendValue(t, series, now.Add(-2*time.Hour), 1)
	if err = db.Compact(); err != nil {
		t.Fatal(err)
	}
	if db.head.sections[1].length != 0 || db.head.sections[6].kind != secTSInvert || db.head.sections[6].length == 0 {
		t.Fatalf("unexpected KV/TS slots: slot1=%+v slot6=%+v", db.head.sections[1], db.head.sections[6])
	}
	if _, ok, err := region.Get("a"); err != nil || ok {
		t.Fatalf("put-clear recovered value ok=%v err=%v", ok, err)
	}
	_ = db.Close()
}

func TestKVMixedTSCompactAndReaderRefresh(t *testing.T) {
	for _, useMmap := range []bool{false, true} {
		t.Run(map[bool]string{false: "readat", true: "mmap"}[useMmap], func(t *testing.T) {
			now := time.Date(2026, 4, 4, 12, 0, 0, 0, time.UTC)
			path := filepath.Join(t.TempDir(), "strata.db")
			writer, err := Open(path, Options{Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			series, err := writer.Series(9, SeriesConfig{Arity: 1})
			if err != nil {
				t.Fatal(err)
			}
			reader, err := Open(path, Options{ReadOnly: true, UseMmap: useMmap})
			if err != nil {
				t.Fatal(err)
			}
			later, err := reader.Region("later")
			if err != nil {
				t.Fatal(err)
			}
			if _, ok, err := later.Get("k"); err != nil || ok {
				t.Fatalf("empty dynamic region ok=%v err=%v", ok, err)
			}
			if err = series.AppendSync(now.Add(-2*time.Hour), Summary{Values: [4]int64{3, 3, 3, 3}}); err != nil {
				t.Fatal(err)
			}
			laterWriter, err := writer.Region("later")
			if err != nil {
				t.Fatal(err)
			}
			if err = laterWriter.Put("k", []byte("v")); err != nil {
				t.Fatal(err)
			}
			writer.compactHook = func() {
				if err := series.AppendSync(now.Add(-time.Minute), Summary{Values: [4]int64{4, 4, 4, 4}}); err != nil {
					t.Errorf("hot TS append: %v", err)
				}
				if err := laterWriter.Put("hot", []byte("tail")); err != nil {
					t.Errorf("hot KV append: %v", err)
				}
			}
			if err = writer.Compact(); err != nil {
				t.Fatal(err)
			}
			writer.compactHook = nil
			if err = reader.Refresh(); err != nil {
				t.Fatal(err)
			}
			if value, ok, err := later.Get("k"); err != nil || !ok || string(value) != "v" {
				t.Fatalf("refreshed KV value=%q ok=%v err=%v", value, ok, err)
			}
			if value, ok, err := later.Get("hot"); err != nil || !ok || string(value) != "tail" {
				t.Fatalf("tail KV value=%q ok=%v err=%v", value, ok, err)
			}
			ts, err := series.Aggregate(context.Background(), AggregateQuery{From: now.Add(-3 * time.Hour), To: now})
			if err != nil || ts.Summary.Values[0] < 7 {
				t.Fatalf("tail TS result=%+v err=%v", ts, err)
			}
			ts.Release()
			_ = reader.Close()
			_ = writer.Close()
		})
	}
}

func TestKVTTLAndCASStateMachine(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "kv-cas.db")
	db, err := Open(path, Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	region, err := db.Region("cas")
	if err != nil {
		t.Fatal(err)
	}

	// Unallocated region: CompareAndSwap / CompareAndDelete must be no-op with zero WAL write.
	walPos := db.walPos
	if ok, err := region.CompareAndSwap("k", nil, []byte("v1")); err != nil || ok || db.walPos != walPos {
		t.Fatalf("unallocated CAS ok=%v err=%v walDelta=%d", ok, err, db.walPos-walPos)
	}
	if ok, err := region.CompareAndDelete("k", nil); err != nil || ok || db.walPos != walPos {
		t.Fatalf("unallocated CAD ok=%v err=%v walDelta=%d", ok, err, db.walPos-walPos)
	}

	// PutIfAbsent with nil empty value, followed by empty-value CAS equivalence (nil <-> []byte{}) with NO implicit delete.
	if ok, err := region.PutIfAbsent("empty", nil); err != nil || !ok {
		t.Fatalf("PutIfAbsent(nil) ok=%v err=%v", ok, err)
	}
	walPos = db.walPos
	if ok, err := region.PutIfAbsent("empty", []byte("x")); err != nil || ok || db.walPos != walPos {
		t.Fatalf("duplicate PutIfAbsent ok=%v err=%v walDelta=%d", ok, err, db.walPos-walPos)
	}
	if ok, err := region.CompareAndSwap("empty", []byte{}, nil); err != nil || !ok {
		t.Fatalf("CAS([]byte{}, nil) ok=%v err=%v", ok, err)
	}
	if v, ok, err := region.Get("empty"); err != nil || !ok || len(v) != 0 {
		t.Fatalf("CAS to nil unexpectedly deleted key: val=%q ok=%v err=%v", v, ok, err)
	}
	if ok, err := region.CompareAndSwap("empty", nil, []byte("v1")); err != nil || !ok {
		t.Fatalf("CAS(nil, v1) ok=%v err=%v", ok, err)
	}
	walPos = db.walPos
	if ok, err := region.CompareAndSwap("empty", []byte("bad"), []byte("v2")); err != nil || ok || db.walPos != walPos {
		t.Fatalf("mismatch CAS ok=%v err=%v walDelta=%d", ok, err, db.walPos-walPos)
	}
	if ok, err := region.CompareAndSwap("empty", []byte("v1"), nil); err != nil || !ok {
		t.Fatalf("CAS(v1, nil) ok=%v err=%v", ok, err)
	}
	if v, ok, err := region.Get("empty"); err != nil || !ok || len(v) != 0 {
		t.Fatalf("CAS(v1, nil) must store empty value, not delete: val=%q ok=%v err=%v", v, ok, err)
	}
	walPos = db.walPos
	if ok, err := region.CompareAndDelete("empty", []byte("wrong")); err != nil || ok || db.walPos != walPos {
		t.Fatalf("mismatch CAD ok=%v err=%v walDelta=%d", ok, err, db.walPos-walPos)
	}
	if ok, err := region.CompareAndDelete("empty", []byte{}); err != nil || !ok {
		t.Fatalf("CAD([]byte{}) on nil value ok=%v err=%v", ok, err)
	}
	if _, ok, err := region.Get("empty"); err != nil || ok {
		t.Fatalf("deleted key still exists ok=%v err=%v", ok, err)
	}
	walPos = db.walPos
	if ok, err := region.CompareAndSwap("empty", nil, []byte("resurrect")); err != nil || ok || db.walPos != walPos {
		t.Fatalf("CAS on missing key must not create: ok=%v err=%v walDelta=%d", ok, err, db.walPos-walPos)
	}
	if ok, err := region.CompareAndDelete("empty", nil); err != nil || ok || db.walPos != walPos {
		t.Fatalf("CAD on missing key must not write: ok=%v err=%v walDelta=%d", ok, err, db.walPos-walPos)
	}

	// TTL expiry & PutIfAbsent after expiry.
	if err = region.PutTTL("tmp", []byte("val"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := region.Get("tmp"); err != nil || !ok || string(v) != "val" {
		t.Fatalf("live TTL get=%q ok=%v err=%v", v, ok, err)
	}
	now = now.Add(2 * time.Minute)
	if _, ok, err := region.Get("tmp"); err != nil || ok {
		t.Fatalf("expired TTL ok=%v err=%v", ok, err)
	}
	if ok, err := region.PutIfAbsent("tmp", []byte("reborn")); err != nil || !ok {
		t.Fatalf("PutIfAbsent after TTL expiry ok=%v err=%v", ok, err)
	}
	if v, ok, err := region.Get("tmp"); err != nil || !ok || string(v) != "reborn" {
		t.Fatalf("reborn get=%q ok=%v err=%v", v, ok, err)
	}
	_ = db.Close()

	// Read-only rejection on CAS primitives.
	ro, err := Open(path, Options{Now: func() time.Time { return now }, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	rr, _ := ro.Region("cas")
	if ok, err := rr.PutIfAbsent("k", []byte("v")); !errors.Is(err, ErrReadOnly) || ok {
		t.Fatalf("ro PutIfAbsent ok=%v err=%v", ok, err)
	}
	if ok, err := rr.CompareAndSwap("tmp", []byte("reborn"), []byte("x")); !errors.Is(err, ErrReadOnly) || ok {
		t.Fatalf("ro CAS ok=%v err=%v", ok, err)
	}
	if ok, err := rr.CompareAndDelete("tmp", []byte("reborn")); !errors.Is(err, ErrReadOnly) || ok {
		t.Fatalf("ro CAD ok=%v err=%v", ok, err)
	}
}

func TestKVScanScanSeqAndLargeBlobs(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "kv-scan.db")
	db, err := Open(path, Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.Region("items")
	if err != nil {
		t.Fatal(err)
	}
	other, err := db.Region("other")
	if err != nil {
		t.Fatal(err)
	}

	// Seed SST with normal keys + an external large blob + keys that overlay will later mask.
	blob := bytes.Repeat([]byte("large-value-"), 4000)
	if err = db.NewBatch().
		In(r).
		Put("a/1", []byte("sst-1")).
		Put("a/2", []byte("sst-2")).
		Put("a/3", []byte("sst-3")).
		Put("a/4", []byte("sst-4")).
		Put("a/5", blob).
		Put("b/1", []byte("b-sst")).
		Put("\xff\xff/1", []byte("hi-bytes")).
		In(other).
		Put("a/1", []byte("other-region")).
		Commit(); err != nil {
		t.Fatal(err)
	}
	if err = db.Compact(); err != nil {
		t.Fatal(err)
	}
	if db.head.sections[1].length == 0 || db.head.sections[1].kind != secKVLargeBlobs {
		t.Fatalf("large blob section missing: %+v", db.head.sections[1])
	}

	// Overlay masks SST: overwrite a/2, tombstone a/3, expired TTL over a/4, insert new a/2b.
	if err = r.NewBatch().
		Put("a/2", []byte("ov-2")).
		Put("a/2b", []byte("ov-2b")).
		Delete("a/3").
		PutTTL("a/4", []byte("temp"), time.Minute).
		Commit(); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)

	ctx := context.Background()
	collect := func(q KVScan) ([]KVItem, error) {
		var out []KVItem
		for item, err := range r.ScanSeq(ctx, q) {
			if err != nil {
				return out, err
			}
			out = append(out, item)
		}
		return out, nil
	}

	// Prefix scan: overlay overwrite + insert + tombstone/expired masking + external large blob.
	items, err := collect(KVScan{Prefix: "a/"})
	if err != nil {
		t.Fatal(err)
	}
	wantKeys := []string{"a/1", "a/2", "a/2b", "a/5"}
	gotKeys := make([]string, len(items))
	for i, it := range items {
		gotKeys[i] = it.Key
	}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("prefix scan keys=%v want=%v", gotKeys, wantKeys)
	}
	if string(items[0].Value) != "sst-1" || string(items[1].Value) != "ov-2" || string(items[2].Value) != "ov-2b" || !bytes.Equal(items[3].Value, blob) {
		t.Fatalf("prefix scan values mismatch")
	}

	// Range intersection (Prefix + Start + End), empty disjoint range, all-0xff prefix, Limit.
	items, err = collect(KVScan{Prefix: "a/", Start: "a/2", End: "a/5"})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Key != "a/2" || items[1].Key != "a/2b" {
		t.Fatalf("range intersection=%+v", items)
	}
	if empty, err := collect(KVScan{Prefix: "a/", Start: "a/9", End: "a/5"}); err != nil || len(empty) != 0 {
		t.Fatalf("disjoint range=%+v err=%v", empty, err)
	}
	if ff, err := collect(KVScan{Prefix: "\xff\xff"}); err != nil || len(ff) != 1 || ff[0].Key != "\xff\xff/1" {
		t.Fatalf("0xff prefix scan=%+v err=%v", ff, err)
	}
	if lim, err := collect(KVScan{Prefix: "a/", Limit: 2}); err != nil || len(lim) != 2 || lim[1].Key != "a/2" {
		t.Fatalf("limit scan=%+v err=%v", lim, err)
	}

	// KeysOnly must skip reading secKVLargeBlobs completely (prove by zeroing section desc temporarily).
	savedBlobSec := db.head.sections[1]
	db.head.sections[1] = sectionDesc{}
	keysOnly, err := collect(KVScan{Prefix: "a/", KeysOnly: true})
	if err != nil {
		t.Fatalf("KeysOnly read external blob section: %v", err)
	}
	if len(keysOnly) != 4 || keysOnly[3].Key != "a/5" || keysOnly[3].Value != nil {
		t.Fatalf("KeysOnly result=%+v", keysOnly)
	}
	if _, err := collect(KVScan{Prefix: "a/"}); err == nil {
		t.Fatal("expected error reading external blob with missing section")
	}
	db.head.sections[1] = savedBlobSec

	// ErrStopScan, custom callback error, break in ScanSeq, and ctx cancellation.
	var seen int
	if err = r.Scan(ctx, KVScan{Prefix: "a/"}, func(KVItem) error {
		seen++
		return ErrStopScan
	}); err != nil || seen != 1 {
		t.Fatalf("ErrStopScan seen=%d err=%v", seen, err)
	}
	sentinel := errors.New("boom")
	if err = r.Scan(ctx, KVScan{Prefix: "a/"}, func(KVItem) error {
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("callback error=%v want=%v", err, sentinel)
	}
	seen = 0
	for _, err := range r.ScanSeq(ctx, KVScan{Prefix: "a/"}) {
		if err != nil {
			t.Fatal(err)
		}
		seen++
		if seen == 2 {
			break
		}
	}
	if seen != 2 {
		t.Fatalf("ScanSeq break seen=%d", seen)
	}
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err = r.Scan(canceledCtx, KVScan{Prefix: "a/"}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Scan err=%v", err)
	}
	midCtx, midCancel := context.WithCancel(ctx)
	seen = 0
	if err = r.Scan(midCtx, KVScan{Prefix: "a/"}, func(KVItem) error {
		seen++
		midCancel()
		return nil
	}); !errors.Is(err, context.Canceled) || seen != 1 {
		t.Fatalf("mid-scan cancel seen=%d err=%v", seen, err)
	}

	// Lock-free snapshot scan: mutating the same Region inside ScanSeq must not deadlock.
	var scannedDuringMut []string
	for item, err := range r.ScanSeq(ctx, KVScan{Prefix: "a/"}) {
		if err != nil {
			t.Fatal(err)
		}
		scannedDuringMut = append(scannedDuringMut, item.Key)
		if item.Key == "a/1" {
			if err := r.Put("a/0", []byte("new")); err != nil {
				t.Fatal(err)
			}
			if err := r.Delete("a/2"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !reflect.DeepEqual(scannedDuringMut, wantKeys) {
		t.Fatalf("snapshot isolation violated: got=%v want=%v", scannedDuringMut, wantKeys)
	}
	if v, ok, err := r.Get("a/0"); err != nil || !ok || string(v) != "new" {
		t.Fatalf("post-scan Put missing: val=%q ok=%v err=%v", v, ok, err)
	}
	if _, ok, err := r.Get("a/2"); err != nil || ok {
		t.Fatalf("post-scan Delete missing: ok=%v err=%v", ok, err)
	}

	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen in ReadOnly mode with both ReadAt and mmap and verify large blob + Scan.
	for _, useMmap := range []bool{false, true} {
		ro, err := Open(path, Options{Now: func() time.Time { return now }, ReadOnly: true, UseMmap: useMmap})
		if err != nil {
			t.Fatal(err)
		}
		rr, err := ro.Region("items")
		if err != nil {
			t.Fatal(err)
		}
		if got, ok, err := rr.Get("a/5"); err != nil || !ok || !bytes.Equal(got, blob) {
			t.Fatalf("reopen mmap=%v blob len=%d ok=%v err=%v", useMmap, len(got), ok, err)
		}
		var roKeys []string
		for item, err := range rr.ScanSeq(ctx, KVScan{Prefix: "a/", KeysOnly: true}) {
			if err != nil {
				t.Fatal(err)
			}
			roKeys = append(roKeys, item.Key)
		}
		wantRO := []string{"a/0", "a/1", "a/2b", "a/5"}
		if !reflect.DeepEqual(roKeys, wantRO) {
			t.Fatalf("reopen mmap=%v keys=%v want=%v", useMmap, roKeys, wantRO)
		}
		_ = ro.Close()
	}
}

func TestKVSectionPassthrough(t *testing.T) {
	t.Run("UnchangedKVAndLargeBlobWithTS", func(t *testing.T) {
		now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
		db, series := testDB(t, &now, Options{})
		path := db.path
		seriesID, seriesCfg := series.id, series.cfg

		if !canPassthroughKV(db.head, db.head.sealedKVRev) {
			t.Fatal("expected empty KV passthrough to be allowed")
		}
		appendValue(t, series, now.Add(-2*time.Hour), 1)
		if err := db.Compact(); err != nil {
			t.Fatal(err)
		}
		if db.head.sections[0].length != 0 || db.head.sections[1].length != 0 || db.head.sections[2].length != 0 || db.head.sealedKVRev != 0 {
			t.Fatalf("unexpected non-empty KV sections: %+v rev=%d", db.head.sections[:3], db.head.sealedKVRev)
		}

		// Incompressible >64KiB blob exercises multi-chunk 64KiB streaming and zero-relocation blobOffset.
		blob := make([]byte, 80<<10)
		x := uint32(0x12345678)
		for i := range blob {
			x ^= x << 13
			x ^= x >> 17
			x ^= x << 5
			blob[i] = byte(x)
		}
		reg, err := db.Region("kv")
		if err != nil {
			t.Fatal(err)
		}
		if err = reg.NewBatch().
			Put("k1", []byte("v1")).
			Put("k2", []byte("v2")).
			Put("big", blob).
			Commit(); err != nil {
			t.Fatal(err)
		}
		if err = db.Compact(); err != nil {
			t.Fatal(err)
		}
		if db.head.sections[1].kind != secKVLargeBlobs || db.head.sections[1].length <= 64<<10 ||
			db.head.sections[0].kind != secKVSSTBlocks || db.head.sections[0].length == 0 ||
			db.head.sections[2].kind != secKVSSTIndex || db.head.sections[2].length == 0 {
			t.Fatalf("unexpected KV sections: %+v", db.head.sections[:3])
		}
		if db.head.sections[2].flags&kvFlagTTLChecked == 0 || db.head.sections[2].flags&kvFlagHasLiveTTL != 0 {
			t.Fatalf("unexpected KV index flags=%#x", db.head.sections[2].flags)
		}
		if !canPassthroughKV(db.head, db.head.sealedKVRev) {
			t.Fatal("expected KV passthrough to be eligible")
		}

		savedSec := [3]sectionDesc{db.head.sections[0], db.head.sections[1], db.head.sections[2]}
		savedRev := db.head.sealedKVRev
		savedGen := db.head.gen

		appendValue(t, series, now.Add(-90*time.Minute), 41)
		if err = db.Compact(); err != nil {
			t.Fatal(err)
		}
		if db.head.gen != savedGen+1 || db.head.sealedKVRev != savedRev {
			t.Fatalf("gen=%d want=%d rev=%d want=%d", db.head.gen, savedGen+1, db.head.sealedKVRev, savedRev)
		}
		if db.head.sections[0] != savedSec[0] || db.head.sections[1] != savedSec[1] || db.head.sections[2] != savedSec[2] {
			t.Fatalf("passthrough modified KV sections: got=%+v want=%+v", db.head.sections[:3], savedSec)
		}
		assertKV(t, reg, "k1", []byte("v1"), true)
		assertKV(t, reg, "k2", []byte("v2"), true)
		assertKV(t, reg, "big", blob, true)
		assertTSSum(t, series, now.Add(-3*time.Hour), now, 42)

		badTmp, err := openData(filepath.Join(t.TempDir(), "bad.tmp"), false)
		if err != nil {
			t.Fatal(err)
		}
		badHead := db.head
		badHead.sections[1].crc ^= 1
		if _, _, _, err = copyPassthroughKVSections(badTmp, db.data, badHead); err == nil {
			_ = badTmp.Close()
			t.Fatal("expected CRC mismatch error from copyPassthroughKVSections")
		}
		_ = badTmp.Close()

		reopened := reopenTestDB(t, db, path, Options{Now: func() time.Time { return now }})
		defer func() { _ = reopened.Close() }()
		if reopened.head.sections[0] != savedSec[0] || reopened.head.sections[1] != savedSec[1] || reopened.head.sections[2] != savedSec[2] {
			t.Fatalf("reopened KV sections mismatch: got=%+v want=%+v", reopened.head.sections[:3], savedSec)
		}
		reReg := mustRegion(t, reopened, "kv")
		reSeries := mustSeries(t, reopened, seriesID, seriesCfg)
		assertKV(t, reReg, "k1", []byte("v1"), true)
		assertKV(t, reReg, "k2", []byte("v2"), true)
		assertKV(t, reReg, "big", blob, true)
		assertTSSum(t, reSeries, now.Add(-3*time.Hour), now, 42)
	})

	t.Run("EmptyCatalogPassthrough", func(t *testing.T) {
		now := time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)
		db, series := testDB(t, &now, Options{})
		defer func() { _ = db.Close() }()

		reg, err := db.Region("cfg")
		if err != nil {
			t.Fatal(err)
		}
		if err = reg.Put("a", []byte("1")); err != nil {
			t.Fatal(err)
		}
		if err = reg.Clear(); err != nil {
			t.Fatal(err)
		}
		if err = db.Compact(); err != nil {
			t.Fatal(err)
		}
		if db.head.sections[0].length != 0 || db.head.sections[1].length != 0 ||
			db.head.sections[2].kind != secKVSSTIndex || db.head.sections[2].length == 0 {
			t.Fatalf("expected catalog-only KV sections: %+v", db.head.sections[:3])
		}
		if db.head.sections[2].flags&kvFlagTTLChecked == 0 || db.head.sections[2].flags&kvFlagHasLiveTTL != 0 {
			t.Fatalf("unexpected catalog-only flags=%#x", db.head.sections[2].flags)
		}
		if !canPassthroughKV(db.head, db.head.sealedKVRev) {
			t.Fatal("expected catalog-only KV passthrough to be eligible")
		}

		savedSec2 := db.head.sections[2]
		savedGen := db.head.gen
		appendValue(t, series, now.Add(-2*time.Hour), 9)
		if err = db.Compact(); err != nil {
			t.Fatal(err)
		}
		if db.head.gen != savedGen+1 || db.head.sections[0].length != 0 || db.head.sections[1].length != 0 || db.head.sections[2] != savedSec2 {
			t.Fatalf("catalog-only passthrough mismatch: gen=%d sections=%+v want2=%+v", db.head.gen, db.head.sections[:3], savedSec2)
		}
		assertKV(t, reg, "a", nil, false)
		assertTSSum(t, series, now.Add(-3*time.Hour), now, 9)
	})

	t.Run("LiveTTLAndRecoveryAfterExpiry", func(t *testing.T) {
		now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
		db, series := testDB(t, &now, Options{})
		defer func() { _ = db.Close() }()

		reg, err := db.Region("sess")
		if err != nil {
			t.Fatal(err)
		}
		if err = reg.NewBatch().
			Put("keep", []byte("perm")).
			PutTTL("temp", []byte("ephemeral"), 2*time.Minute).
			Commit(); err != nil {
			t.Fatal(err)
		}
		if err = db.Compact(); err != nil {
			t.Fatal(err)
		}
		if db.head.sections[2].flags&kvFlagTTLChecked == 0 || db.head.sections[2].flags&kvFlagHasLiveTTL == 0 {
			t.Fatalf("expected TTLChecked|HasLiveTTL flags=%#x", db.head.sections[2].flags)
		}
		if canPassthroughKV(db.head, db.head.sealedKVRev) {
			t.Fatal("expected live TTL to disallow passthrough")
		}
		assertKV(t, reg, "temp", []byte("ephemeral"), true)

		now = now.Add(5 * time.Minute)
		appendValue(t, series, now.Add(-2*time.Hour), 5)
		if err = db.Compact(); err != nil {
			t.Fatal(err)
		}
		assertKV(t, reg, "temp", nil, false)
		assertKV(t, reg, "keep", []byte("perm"), true)
		if db.head.sections[2].flags&kvFlagTTLChecked == 0 || db.head.sections[2].flags&kvFlagHasLiveTTL != 0 {
			t.Fatalf("expected expired TTL cleared in flags=%#x", db.head.sections[2].flags)
		}
		if !canPassthroughKV(db.head, db.head.sealedKVRev) {
			t.Fatal("expected passthrough to recover after TTL expiry")
		}

		savedSec := [3]sectionDesc{db.head.sections[0], db.head.sections[1], db.head.sections[2]}
		appendValue(t, series, now.Add(-90*time.Minute), 6)
		if err = db.Compact(); err != nil {
			t.Fatal(err)
		}
		if db.head.sections[0] != savedSec[0] || db.head.sections[1] != savedSec[1] || db.head.sections[2] != savedSec[2] {
			t.Fatalf("expected passthrough after TTL recovery: got=%+v want=%+v", db.head.sections[:3], savedSec)
		}
		assertKV(t, reg, "keep", []byte("perm"), true)
		assertTSSum(t, series, now.Add(-3*time.Hour), now, 11)
	})

	t.Run("KVUpdatesAndConcurrentHookBatch", func(t *testing.T) {
		now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
		db, series := testDB(t, &now, Options{})
		path := db.path
		seriesID, seriesCfg := series.id, series.cfg

		r1, err := db.Region("r1")
		if err != nil {
			t.Fatal(err)
		}
		r2, err := db.Region("r2")
		if err != nil {
			t.Fatal(err)
		}
		if err = db.NewBatch().
			In(r1).Put("old", []byte("v1")).Put("del", []byte("gone")).
			In(r2).Put("wipe", []byte("soon")).
			Commit(); err != nil {
			t.Fatal(err)
		}
		if err = db.Compact(); err != nil {
			t.Fatal(err)
		}
		if !canPassthroughKV(db.head, db.head.sealedKVRev) {
			t.Fatal("expected passthrough after initial KV seal")
		}
		rev1 := db.head.sealedKVRev
		sec1 := [3]sectionDesc{db.head.sections[0], db.head.sections[1], db.head.sections[2]}

		appendValue(t, series, now.Add(-2*time.Hour), 3)
		db.compactHook = func() {
			if hookErr := db.NewBatch().
				In(r1).Put("hot", []byte("during-compact")).Delete("del").
				In(r2).Clear().
				Commit(); hookErr != nil {
				t.Errorf("concurrent KV batch: %v", hookErr)
			}
		}
		err = db.Compact()
		db.compactHook = nil
		if err != nil {
			t.Fatal(err)
		}
		if db.head.sealedKVRev != rev1 {
			t.Fatalf("sealedKVRev changed during passthrough: got=%d want=%d", db.head.sealedKVRev, rev1)
		}
		if db.head.sections[0] != sec1[0] || db.head.sections[1] != sec1[1] || db.head.sections[2] != sec1[2] {
			t.Fatalf("KV sections changed during passthrough: got=%+v want=%+v", db.head.sections[:3], sec1)
		}
		assertKV(t, r1, "old", []byte("v1"), true)
		assertKV(t, r1, "hot", []byte("during-compact"), true)
		assertKV(t, r1, "del", nil, false)
		assertKV(t, r2, "wipe", nil, false)

		if err = db.Compact(); err != nil {
			t.Fatal(err)
		}
		if db.head.sealedKVRev <= rev1 {
			t.Fatalf("expected sealedKVRev to advance past %d, got %d", rev1, db.head.sealedKVRev)
		}
		assertKV(t, r1, "old", []byte("v1"), true)
		assertKV(t, r1, "hot", []byte("during-compact"), true)
		assertKV(t, r1, "del", nil, false)
		assertKV(t, r2, "wipe", nil, false)

		rev2 := db.head.sealedKVRev
		if err = r1.Put("old", []byte("v2")); err != nil {
			t.Fatal(err)
		}
		if err = db.Compact(); err != nil {
			t.Fatal(err)
		}
		if db.head.sealedKVRev <= rev2 {
			t.Fatalf("expected sealedKVRev to advance after Put: got=%d prev=%d", db.head.sealedKVRev, rev2)
		}
		reopened := reopenTestDB(t, db, path, Options{Now: func() time.Time { return now }})
		defer func() { _ = reopened.Close() }()
		rr1 := mustRegion(t, reopened, "r1")
		rr2 := mustRegion(t, reopened, "r2")
		rs := mustSeries(t, reopened, seriesID, seriesCfg)
		assertKV(t, rr1, "old", []byte("v2"), true)
		assertKV(t, rr1, "hot", []byte("during-compact"), true)
		assertKV(t, rr1, "del", nil, false)
		assertKV(t, rr2, "wipe", nil, false)
		assertTSSum(t, rs, now.Add(-3*time.Hour), now, 3)
	})

	t.Run("LegacyUnflaggedUpgrade", func(t *testing.T) {
		now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
		db, series := testDB(t, &now, Options{})
		path := db.path
		seriesID, seriesCfg := series.id, series.cfg

		reg, err := db.Region("legacy")
		if err != nil {
			t.Fatal(err)
		}
		if err = reg.Put("k", []byte("legacy-val")); err != nil {
			t.Fatal(err)
		}
		if err = db.Compact(); err != nil {
			t.Fatal(err)
		}
		if db.head.sections[2].flags&kvFlagTTLChecked == 0 {
			t.Fatalf("expected kvFlagTTLChecked before clearing, got=%#x", db.head.sections[2].flags)
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
		patchSuperblocks(t, path, func(_ *osFile, sb *superblock) {
			sb.sections[2].flags = 0
		})

		db = reopenTestDB(t, nil, path, Options{Now: func() time.Time { return now }})
		defer func() { _ = db.Close() }()
		if db.head.sections[2].flags != 0 || canPassthroughKV(db.head, db.head.sealedKVRev) {
			t.Fatalf("expected legacy unflagged DB to reject passthrough: flags=%#x", db.head.sections[2].flags)
		}
		if err = db.Compact(); err != nil {
			t.Fatal(err)
		}
		if db.head.sections[2].flags&kvFlagTTLChecked == 0 || db.head.sections[2].flags&kvFlagHasLiveTTL != 0 {
			t.Fatalf("expected legacy upgrade to set kvFlagTTLChecked, got=%#x", db.head.sections[2].flags)
		}
		if !canPassthroughKV(db.head, db.head.sealedKVRev) {
			t.Fatal("expected passthrough to become eligible after upgrade compact")
		}

		savedSec := [3]sectionDesc{db.head.sections[0], db.head.sections[1], db.head.sections[2]}
		reSeries, err := db.Series(seriesID, seriesCfg)
		if err != nil {
			t.Fatal(err)
		}
		appendValue(t, reSeries, now.Add(-2*time.Hour), 7)
		if err = db.Compact(); err != nil {
			t.Fatal(err)
		}
		if db.head.sections[0] != savedSec[0] || db.head.sections[1] != savedSec[1] || db.head.sections[2] != savedSec[2] {
			t.Fatalf("post-upgrade passthrough modified KV sections: got=%+v want=%+v", db.head.sections[:3], savedSec)
		}
		reg, err = db.Region("legacy")
		if err != nil {
			t.Fatal(err)
		}
		assertKV(t, reg, "k", []byte("legacy-val"), true)
		assertTSSum(t, reSeries, now.Add(-3*time.Hour), now, 7)
	})
}

func TestKVPutTTLMaxDuration(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "kv_max_ttl.db")
	opts := Options{Now: func() time.Time { return now }}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.Region("ttl")
	if err != nil {
		t.Fatal(err)
	}
	if err = r.PutTTL("forever", []byte("alive"), time.Duration(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := r.Get("forever"); err != nil || !ok || string(v) != "alive" {
		t.Fatalf("immediate Get failed: val=%q ok=%v err=%v", v, ok, err)
	}
	if err = db.Compact(); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := r.Get("forever"); err != nil || !ok || string(v) != "alive" {
		t.Fatalf("post-Compact Get failed: val=%q ok=%v err=%v", v, ok, err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	rr, err := reopened.Region("ttl")
	if err != nil {
		t.Fatal(err)
	}
	if v, ok, err := rr.Get("forever"); err != nil || !ok || string(v) != "alive" {
		t.Fatalf("reopen Get failed: val=%q ok=%v err=%v", v, ok, err)
	}
}
