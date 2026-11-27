# Strata

English | [简体中文](README_zh.md)

An **ultralight, memory-bounded embedded storage toolkit** in pure Go (`CGO_ENABLED=0`). Use it for proxy client traffic statistics, game launch statistics, AI API usage, or desktop and mobile background tools. The 90-day, million-row proxy gateway dataset is the core stress-test baseline.

## Scope

The root package stores fixed-schema time series and transactional key-value data in one file, with a small footprint, bounded memory, and tag filters pushed down into queries.

- **Fixed schema**: Up to 8 string tag dimensions (`Arity: 1..8`) + 4 `int64` metrics (`OpSum`, `OpMin`, `OpMax`, `OpLast`) per series.
- **Built-in rollups**: Automatic `1m` / `1h` / `1d` downsampling with per-tier retention and local-hour rollup (`RollupLocalHours`).
- **Tag search**: Columnar `STR2` blocks, 5-mode adaptive inverted indexes, hybrid inline/disk-paged dictionaries, pushdown filters (`TagEq`, `TagPrefix`, `TagSuffix`, `TagContains`, `TagCIDR`), and a 1-slot bounded light-read lane for recent small pages.
- **Co-located KV**: Pure-byte transactional KV regions (`Region`, `Batch`, `Scan`, `ScanSeq`) with TTL, CAS, and lock-free streaming scans inside the same file.
- **Memory-first**: `MemoryBalanced` / `MemoryConstrained` profiles, pointer-free (`noscan`) indexes, explicit `Client.Reclaim()` (`MADV_DONTNEED` on Unix), and only 2 external dependencies (`klauspost/compress`, `golang.org/x/sys`).

The v0.2.0 root package uses the `UNIDBv02` file format. Opening a v0.1.0 database returns an error.

## Quick Start

```go
client, err := strata.OpenClient("strata.db", strata.ClientOptions{
	DB: strata.Options{MemoryProfile: strata.MemoryBalanced},
})
if err != nil {
	panic(err)
}
defer client.Close()

series, _ := client.RegisterSeries(1, strata.SeriesConfig{
	Arity:      2,
	SummaryOps: [4]strata.MetricOp{strata.OpSum, strata.OpSum, strata.OpSum, strata.OpLast},
	RowOps:     [4]strata.MetricOp{strata.OpSum, strata.OpSum, strata.OpSum, strata.OpLast},
})

now := time.Now().UTC()
_ = series.Append(now,
	strata.Summary{Values: [4]int64{1024, 4096, 1, 1}},
	strata.Row{Tags: []string{"example.com", "tcp"}, Values: [4]int64{1024, 4096, 1, 1}},
)
_ = client.Sync()

ctx := context.Background()

// Grouped Top-K aggregation
_ = client.WithAggregate(ctx, 1, strata.AggregateQuery{
	From: now.Add(-time.Hour), To: now.Add(time.Minute),
	GroupDims: []int{0}, TopK: 20,
}, func(res *strata.AggregateResult) error { return nil })

// Cursor-paginated detail read
_ = client.WithPage(ctx, 1, strata.ReadRequest{
	From: now.Add(-15 * time.Minute), To: now.Add(time.Minute), Limit: 200,
}, func(page *strata.PageResult) error { return nil })

// Zero-materialization streaming scan
_ = client.Scan(ctx, 1, strata.ScanQuery{
	From: now.Add(-time.Hour), To: now.Add(time.Minute),
	Filters: []strata.Filter{{Dim: 1, Op: strata.TagEq, Values: []string{"tcp"}}},
}, func(b strata.ScanBatch) error { return nil })

// Transactional KV
reg, _ := client.Region("config")
_ = reg.Put("theme", []byte("dark"))

// Reclaim idle pools, caches, and mmap pages
client.Reclaim()
```

## Optional event history

Import `github.com/UruhaLushia/strata/eventlog` to keep a bounded local history of structured events. The root package and `eventlog` have no dependency on each other, so applications that omit `eventlog` incur no event history overhead. Records are stored directly, with no Zstd or resident block directory.

`Open` returns `(*eventlog.Log, error)`. `Append`, `Flush()`, `Sync()`, and `Close()` each return `error`. `Scan` visits a bounded set of records in reverse append order and returns `(eventlog.ScanResult, error)`.

```go
log, err := eventlog.Open("events", eventlog.Options{})
if err != nil {
	panic(err)
}
defer log.Close()

if err := log.Append(eventlog.Record{
	TimestampNS: time.Now().UnixNano(), StreamID: 1, Payload: []byte("launch"),
}); err != nil {
	panic(err)
}
if err := log.Sync(); err != nil {
	panic(err)
}
if _, err := log.Scan(context.Background(), eventlog.ScanOptions{MaxRecords: 10}, func(r eventlog.Record) error {
	println(string(r.Payload))
	return nil
}); err != nil {
	panic(err)
}
```

## Benchmarks

90-day gateway dataset: **1,060,830 rows**, 7 tag dims + 4 metrics (`OpSum/OpMin/OpMax/OpLast`), **22,406 sealed blocks** (`1m`/`1h`/`1d`) + 2,000 KV entries on Linux amd64 (`Intel i5-9300H`, Go 1.27, `mmap=false` unless noted).

### Footprint & Idle Memory

| Dataset | `.db` Size | Open `HeapAlloc` | Post-Reclaim `RssAnon` (`mmap=true`) | Incremental `Compact` |
|---|---:|---:|---:|---:|
| Realistic (5K domains, 2K IPs) | 10.15 MiB | 2.22 MiB | 9.56 MiB | 182–195 ms |
| Extreme (1.06M unique IPv6) | 26.45 MiB | 2.32 MiB | 14.40 MiB | 438–454 ms |

### Query Latency & Allocations

| Benchmark | Realistic | Extreme (1.06M IPv6) |
|---|---:|---:|
| `SummaryOnly` (90d zero-disk) | 38.0 µs / 1.1 KiB / 9 allocs | 38.0 µs / 1.1 KiB / 9 allocs |
| `ReadLast15mPage200` | 41.1 µs / 11.1 KiB / 21 allocs | 75.0 µs / 15.8 KiB / 221 allocs |
| `Read90dDeepOffset5000` | 91.1 µs / 80.4 KiB / 14 allocs | 101.2 µs / 83.9 KiB / 214 allocs |
| `Scan90dSparseMeta` (76 rows) | 3.03 ms / 80.6 KiB / 39 allocs | 5.80 ms / 454 KiB / 46 allocs |
| `Scan90dMediumMeta` (1,768 rows) | 6.46 ms / 86.5 KiB / 95 allocs | 10.09 ms / 532 KiB / 103 allocs |
| `Scan90dDenseMeta` (344K / 530K rows) | 9.72 ms / 445 KiB / 98 allocs | 15.11 ms / 990 KiB / 102 allocs |
| `Scan90dTimeFirstIDs` (735K / 1.06M rows) | 8.93 ms / 430 KiB / 87 allocs | 13.23 ms / 957 KiB / 90 allocs |
| `AggregateFiltered` | 1.29 ms / 348 KiB / 87 allocs | — |
| `AggregateTopK` (5K domains Top-50) | 5.40 ms / 757 KiB / 158 allocs | — |
| `AggregateDomain` (5K domains) | 6.68 ms / 1.10 MiB / 135 allocs | — |
| `ExtremeDestIPTopK` (1.06M IPv6 exact Top-50) | — | 44.36 ms / 2.70 MiB / 355 allocs (`39.0 ms` post-Reclaim `mmap=true`) |

Under 8-way mixed concurrency saturation (`mmap=true`), `SummaryOnly` stays at `p99 <= 183 µs` and bounded small-page `Read` stays at `p99 <= 424 µs` (`<= 303 µs` under `MemoryConstrained`).

## Testing

```sh
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -race ./...

# Opt-in 90-day million-row stress report & benchmarks
STRATA_STRESS_90D=1 go test -v -count=1 -run 'TestGateway90DayMillionReport|TestGateway90ReclaimedFirstQuery' .
STRATA_STRESS_90D=1 go test -bench=BenchmarkGateway90 -benchmem -run=^$ .
```

## License

[GNU General Public License v3.0](LICENSE)
