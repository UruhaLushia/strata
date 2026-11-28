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

### Query Latency & Allocations (`codec/zstd`, `zstd.Fastest`)

| Benchmark | Realistic | Extreme (1.06M IPv6) |
|---|---:|---:|
| `SummaryOnly` (90d zero-disk) | 36.9 µs / 881 B / 7 allocs | 36.9 µs / 881 B / 7 allocs |
| `ReadLast15mPage200` | 41.3 µs / 8.6 KiB / 13 allocs | 68.4 µs / 15.5 KiB / 214 allocs |
| `Read90dDeepOffset5000` | 84.2 µs / 80.3 KiB / 14 allocs | 86.8 µs / 83.6 KiB / 214 allocs |
| `OutboundLookup` (15m / 1h / 24h / 7d hit) | 77.5 µs / 203.2 µs / 500.0 µs / 907.6 µs | 65.0 µs / 64.4 µs / 64.5 µs / 65.1 µs |
| `OutboundLookup` (unknown miss) | 1.33 µs / 2.3 KiB / 5 allocs | 10.6 µs / 10.4 KiB / 13 allocs |
| `Scan90dSparseMeta` (76 rows) | 2.04 ms / 36.3 KiB / 33 allocs | 4.44 ms / 40.8 KiB / 33 allocs |
| `Scan90dMediumMeta` (1,768 rows) | 5.33 ms / 64.2 KiB / 26 allocs | 8.20 ms / 89.7 KiB / 27 allocs |
| `Scan90dDenseMeta` (344K / 530K rows) | 7.97 ms / 401 KiB / 33 allocs | 12.79 ms / 638 KiB / 35 allocs |
| `Scan90dTimeFirstIDs` (735K / 1.06M rows) | 8.10 ms / 371 KiB / 16 allocs | 12.99 ms / 592 KiB / 17 allocs |
| `AggregateFiltered` | 1.17 ms / 236 KiB / 81 allocs | — |
| `AggregateTopK` (5K domains Top-20) | 4.31 ms / 804 KiB / 107 allocs | — |
| `AggregateDomain` (5K domains) | 5.47 ms / 1.13 MiB / 72 allocs | — |
| `ExtremeDestIPTopK` (1.06M IPv6 exact Top-50) | — | 38.30 ms / 2.41 MiB / 218 allocs |

### Compression Codec Comparison

Same 90-day 1.06M-row dataset (`BenchmarkGateway90CodecLatency`, `mmap=false`, each database written and compacted with its own codec):

| Codec | `.db` Size (Domain / Extreme) | Domain `15m` / `24h` Hit | `Read15m` Hit | `Scan90d` Dense / TimeFirst | `AggregateTopK` | Extreme `15m` Hit / `Scan90d` Dense |
|---|---:|---:|---:|---:|---:|---:|
| `None` (default) | 27.3 MiB / 73.6 MiB | **37.9 µs** / **263.6 µs** | **40.1 µs** | **5.51 ms** / **4.83 ms** | 3.82 ms | **41.9 µs** / **9.34 ms** |
| `codec/s2` (`Default`) | 11.5 MiB / 37.1 MiB | 47.5 µs / 304.7 µs | 49.7 µs | 7.76 ms / 7.07 ms | **3.66 ms** | 56.1 µs / 11.31 ms |
| `codec/zstd` (`Fastest`) | 10.1 MiB / 26.4 MiB | 86.6 µs / 563.2 µs | 86.6 µs | 9.16 ms / 8.50 ms | 4.56 ms | 69.1 µs / 13.24 ms |
| `codec/flate` (`BestSpeed`) | **9.68 MiB** / **25.0 MiB** | 276.3 µs / 1.18 ms | 281.3 µs | 17.82 ms / 17.01 ms | 7.36 ms | 141.4 µs / 20.69 ms |

### Binary Size & `eventlog` Throughput

Stripped `CGO_ENABLED=0` binary size (`-trimpath -ldflags="-s -w"`, root builds include `OpenClient` + `Compact`):

| Build | Binary Size | Savings vs `zstd` |
|---|---:|---:|
| `strata` (`None`, default) | 2.14 MiB | -976 KiB |
| `strata` + `codec/flate` (stdlib) | 2.44 MiB | -672 KiB |
| `strata` + `codec/s2` | 2.75 MiB | -356 KiB |
| `strata` + `codec/zstd` | 3.09 MiB | — |
| `eventlog` standalone (`None`) | 1.50 MiB | -1.59 MiB |

`eventlog` across codecs (`BenchmarkEventlogCodecs`, structured 69B JSON payloads, 4 MiB raw equivalent, 20 records/frame):

| Codec | Disk Size | `AppendFlush` (256-rec batch) | `Scan` Recent 200 | `Scan` All |
|---|---:|---:|---:|---:|
| `None` (default) | 4.00 MiB | **52.9 ns/event** (1,304 MB/s, 0 allocs) | **15.6 µs** (297 B / 5 allocs) | **1.56 ms** |
| `codec/s2` (`Default`) | 1.13 MiB | 71.2 ns/event (970 MB/s, 0 allocs) | 21.9 µs (297 B / 5 allocs) | 2.53 ms |
| `codec/zstd` (`Fastest`) | **582 KiB** | 243.6 ns/event (283 MB/s, 0 allocs) | 57.1 µs (302 B / 5 allocs) | 11.15 ms |
| `codec/flate` (`BestSpeed`) | 805 KiB | 303.9 ns/event (227 MB/s, 1 alloc) | 87.1 µs (544 B / 15 allocs) | 17.90 ms |

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
