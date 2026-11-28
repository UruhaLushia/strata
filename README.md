# Strata

English | [简体中文](README_zh.md)

An ultralight, memory-bounded storage engine in pure Go (`CGO_ENABLED=0`).

## Overview

Strata stores fixed-schema time series and transactional key-value data in a single file, built for small footprints, bounded memory, and pushdown tag queries.

- **Fixed-schema time series**: Up to 8 string tag dimensions (`Arity: 1..8`) and 4 `int64` metrics (`OpSum`, `OpMin`, `OpMax`, `OpLast`), with automatic `1m` / `1h` / `1d` rollups, per-tier retention, and local-hour grouping (`RollupLocalHours`).
- **Pushdown tag search**: Columnar `STR2` blocks, adaptive inverted indexes, hybrid inline/paged dictionaries, `TagEq` / `TagPrefix` / `TagSuffix` / `TagContains` / `TagCIDR` filters, and a 1-slot light-read lane for recent small pages.
- **Co-located transactional KV**: Byte-oriented KV regions (`Region`, `Batch`, `Scan`, `ScanSeq`) with TTL, CAS, and lock-free streaming scans inside the same file.
- **Memory-first execution**: `MemoryBalanced` / `MemoryConstrained` profiles, pointer-free (`noscan`) indexes, and explicit `Client.Reclaim()` (`MADV_DONTNEED` on Unix).
- **Pluggable compression & optional event log**: Uncompressed by default via `strata/codec` (opt-in `codec/s2`, `codec/zstd`, `codec/flate`, or custom codecs) and a decoupled `strata/eventlog` subpackage. Importing only `strata` or `eventlog` links no external compression library.

> v0.2.0 uses the `UNIDBv02` file format; opening a v0.1.0 database returns an error.

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

## Optional Event Log (`strata/eventlog`)

Import `github.com/UruhaLushia/strata/eventlog` for a standalone, bounded local history of structured events. It shares no imports with the root package, defaults to uncompressed storage with no resident block directory, and supports optional compression via `Options.Codec`.

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

90-day gateway dataset: **1,060,830 rows**, 7 tag dims + 4 metrics, **22,406 sealed blocks** (`1m`/`1h`/`1d`) + 2,000 KV entries on Linux amd64 (`Intel i5-9300H`, Go 1.27; `go.mod` requires Go 1.26+; `mmap=false` unless noted).

### Footprint & Query Latency (`codec/zstd`, `zstd.Fastest`)

| Dataset | `.db` Size | Open `HeapAlloc` | Post-Reclaim `RssAnon` (`mmap=true`) | Incremental `Compact` |
|---|---:|---:|---:|---:|
| Realistic (5K domains, 2K IPs) | 10.1 MiB | 2.20 MiB | 13.48 MiB | 172–176 ms |
| Extreme (1.06M unique IPv6) | 26.4 MiB | 2.31 MiB | 14.42 MiB | 446–462 ms |

| Benchmark (`BenchmarkGateway90*`) | Realistic | Extreme (1.06M IPv6) |
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

Under 8-way mixed concurrency saturation (`mmap=true`), `SummaryOnly` holds `p99 <= 183 µs` and bounded small-page `Read` holds `p99 <= 424 µs` (`<= 303 µs` under `MemoryConstrained`).

### Compression Codecs

Standalone per-codec comparison on the same 90-day 1.06M-row dataset (`BenchmarkGateway90CodecLatency`; binary sizes are stripped `CGO_ENABLED=0` builds with `OpenClient` + `Compact`):

| Codec | Binary | `.db` Size (Realistic / Extreme) | Domain `15m` / `24h` Hit | `Scan90d` Dense (Realistic / Extreme) | `AggregateTopK` |
|---|---:|---:|---:|---:|---:|
| `None` (default) | **2.14 MiB** | 27.3 / 73.6 MiB | **37.9 µs** / **263.6 µs** | **5.51 ms** / **9.34 ms** | 3.82 ms |
| `codec/s2` (`Default`) | 2.75 MiB | 11.5 / 37.1 MiB | 47.5 µs / 304.7 µs | 7.76 ms / 11.31 ms | **3.66 ms** |
| `codec/zstd` (`Fastest`) | 3.09 MiB | 10.1 / 26.4 MiB | 86.6 µs / 563.2 µs | 9.16 ms / 13.24 ms | 4.56 ms |
| `codec/flate` (`BestSpeed`) | 2.44 MiB | **9.68 / 25.0 MiB** | 276.3 µs / 1.18 ms | 17.82 ms / 20.69 ms | 7.36 ms |

Cold-key miss stays at `1.65–1.76 µs` across all four codecs; `Scan90d/TimeFirst` follows the same ordering (`4.83 / 7.07 / 8.50 / 17.01 ms`). Values differ slightly from the full `BenchmarkGateway90Day` table above because each codec row runs against its own freshly compacted database in a separate pass.

### Event Log (`BenchmarkEventlogCodecs`)

Structured 69B JSON payloads (4 MiB raw equivalent, 20 records/frame; standalone `eventlog` binary with `None` is `1.50 MiB`):

| Codec | Disk Size | `AppendFlush` (256-rec batch) | `Scan` Recent 200 | `Scan` All |
|---|---:|---:|---:|---:|
| `None` (default) | 4.00 MiB | **52.9 ns/event** (1,304 MB/s, 0 allocs) | **15.6 µs** (297 B / 5 allocs) | **1.56 ms** |
| `codec/s2` (`Default`) | 1.13 MiB | 71.2 ns/event (970 MB/s, 0 allocs) | 21.9 µs (297 B / 5 allocs) | 2.53 ms |
| `codec/zstd` (`Fastest`) | **582 KiB** | 243.6 ns/event (283 MB/s, 0 allocs) | 57.1 µs (302 B / 5 allocs) | 11.15 ms |
| `codec/flate` (`BestSpeed`) | 805 KiB | 303.9 ns/event (227 MB/s, 1 alloc) | 87.1 µs (544 B / 15 allocs) | 17.90 ms |

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
