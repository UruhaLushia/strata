# Strata

English | [简体中文](README_zh.md)

An **ultralight, memory-bounded storage engine** in pure Go (`CGO_ENABLED=0`).

## Scope

The root package stores fixed-schema time series and transactional key-value data in one file, with a small footprint, bounded memory, and tag filters pushed down into queries.

- **Fixed schema**: Up to 8 string tag dimensions (`Arity: 1..8`) + 4 `int64` metrics (`OpSum`, `OpMin`, `OpMax`, `OpLast`) per series.
- **Built-in rollups**: Automatic `1m` / `1h` / `1d` downsampling with per-tier retention and local-hour rollup (`RollupLocalHours`).
- **Tag search**: Columnar `STR2` blocks, 5-mode adaptive inverted indexes, hybrid inline/disk-paged dictionaries, pushdown filters (`TagEq`, `TagPrefix`, `TagSuffix`, `TagContains`, `TagCIDR`), and a 1-slot bounded light-read lane for recent small pages.
- **Co-located KV**: Pure-byte transactional KV regions (`Region`, `Batch`, `Scan`, `ScanSeq`) with TTL, CAS, and lock-free streaming scans inside the same file.
- **Memory-first & pluggable compression**: `MemoryBalanced` / `MemoryConstrained` profiles, pointer-free (`noscan`) indexes, explicit `Client.Reclaim()` (`MADV_DONTNEED` on Unix), and pluggable compression via `strata/codec` (uncompressed by default with zero external compression dependencies; opt-in `codec/zstd`, `codec/s2`, `codec/flate`, or custom codecs).

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

Import `github.com/UruhaLushia/strata/eventlog` to keep a bounded local history of structured events. The root package and `eventlog` do not import each other, so omitting `eventlog` adds zero overhead. Compression is optional via `Options.Codec` (default uncompressed, zero resident block directory).

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
| Realistic (5K domains, 2K IPs) | 10.06 MiB | 2.20 MiB | 13.48 MiB | 172–176 ms |
| Extreme (1.06M unique IPv6) | 26.36 MiB | 2.31 MiB | 14.42 MiB | 446–462 ms |

### Query Latency & Allocations

| Benchmark | Realistic | Extreme (1.06M IPv6) |
|---|---:|---:|
| `SummaryOnly` (90d zero-disk) | 36.1 µs / 887 B / 7 allocs | 36.1 µs / 887 B / 7 allocs |
| `ReadLast15mPage200` | 39.1 µs / 10.8 KiB / 14 allocs | 68.4 µs / 15.5 KiB / 214 allocs |
| `Read90dDeepOffset5000` | 84.2 µs / 80.3 KiB / 14 allocs | 86.8 µs / 83.6 KiB / 214 allocs |
| `OutboundLookup` (15m / 1h / 24h / 7d hit) | 83.4 µs / 212.8 µs / 543.0 µs / 1.06 ms | 64.1 µs / 65.5 µs / 64.7 µs / 71.0 µs |
| `OutboundLookup` (unknown miss) | 3.9–4.1 µs / 9.4 KiB / 16 allocs | 10.2–10.8 µs / 9.6 KiB / 17 allocs |
| `Scan90dSparseMeta` (76 rows) | 2.94 ms / 86.2 KiB / 39 allocs | 5.11 ms / 95.3 KiB / 39 allocs |
| `Scan90dMediumMeta` (1,768 rows) | 6.29 ms / 88.0 KiB / 26 allocs | 8.51 ms / 105.7 KiB / 27 allocs |
| `Scan90dDenseMeta` (344K / 530K rows) | 9.60 ms / 445 KiB / 28 allocs | 13.80 ms / 1.00 MiB / 35 allocs |
| `Scan90dTimeFirstIDs` (735K / 1.06M rows) | 7.69 ms / 426 KiB / 16 allocs | 10.72 ms / 968 KiB / 20 allocs |
| `AggregateFiltered` | 1.17 ms / 236 KiB / 81 allocs | — |
| `AggregateTopK` (5K domains Top-50) | 4.77 ms / 824 KiB / 102 allocs | — |
| `AggregateDomain` (5K domains) | 5.47 ms / 1.13 MiB / 72 allocs | — |
| `ExtremeDestIPTopK` (1.06M IPv6 exact Top-50) | — | 38.30 ms / 2.41 MiB / 218 allocs |

### Binary Size & `eventlog` Throughput

Stripped `CGO_ENABLED=0` binary size (`-trimpath -ldflags="-s -w"`, root builds include `OpenClient` + `Compact`):

| Build | Binary Size | Savings vs `zstd` |
|---|---:|---:|
| `strata` (`None`, default) | 2.14 MiB | -976 KiB |
| `strata` + `codec/flate` (stdlib) | 2.44 MiB | -672 KiB |
| `strata` + `codec/s2` | 2.75 MiB | -356 KiB |
| `strata` + `codec/zstd` | 3.09 MiB | — |
| `eventlog` standalone (`None`) | 1.50 MiB | -1.59 MiB |

| `eventlog` Benchmark (64B payload, 4 MiB shard, `None`) | Latency | Memory / Allocs | Throughput |
|---|---:|---:|---:|
| `AppendFlush` (batched write) | 45.0 ns/event | 0 allocs/op | 1,423 MB/s |
| `ScanRecent` (recent 200, ~1.5 KiB frames) | 14.8 µs | 345 B / 6 allocs | — |
| `ScanRecent` (full 4 MiB shard, ~1.5 KiB frames) | 1.40 ms | 344 B / 6 allocs | ~2.9 GB/s |

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
