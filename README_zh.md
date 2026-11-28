# Strata

[English](README.md) | 简体中文

一个纯 Go（`CGO_ENABLED=0`）实现的极轻量、内存有界存储引擎。

## 概览

Strata 在单个文件内存储固定结构的时序数据与事务键值数据，面向小体积、可控内存上限与标签下推查询。

- **固定结构时序**：每条序列支持最多 8 维字符串标签（`Arity: 1..8`）与 4 个 `int64` 指标（`OpSum`、`OpMin`、`OpMax`、`OpLast`），内置 `1m` / `1h` / `1d` 自动汇总、分层过期与本地时区按小时聚合（`RollupLocalHours`）。
- **标签下推检索**：`STR2` 列式分块、自适应倒排索引、内联与外置分页混合词典、下推过滤（`TagEq`、`TagPrefix`、`TagSuffix`、`TagContains`、`TagCIDR`），以及防止近期小页被重查询阻塞的 1 槽轻读通道。
- **同文件事务 KV**：单文件内提供纯字节事务 KV 分区（`Region`、`Batch`、`Scan`、`ScanSeq`），支持 TTL、CAS 与无锁快照流式扫描。
- **内存优先**：提供 `MemoryBalanced` / `MemoryConstrained` 双档预算、零指针（`noscan`）索引结构与显式 `Client.Reclaim()`（Unix 下触发 `MADV_DONTNEED`）。
- **可插拔压缩与可选事件日志**：默认不压缩，通过 `strata/codec` 按需启用 `codec/s2`、`codec/zstd`、`codec/flate` 或自定义实现，并提供零耦合的 `strata/eventlog` 子包；仅导入 `strata` 或 `eventlog` 时不会链接外部压缩库。

> v0.2.0 使用 `UNIDBv02` 文件格式；打开 v0.1.0 数据库会返回错误。

## 快速上手

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

// 下推分组 Top-K 聚合
_ = client.WithAggregate(ctx, 1, strata.AggregateQuery{
	From: now.Add(-time.Hour), To: now.Add(time.Minute),
	GroupDims: []int{0}, TopK: 20,
}, func(res *strata.AggregateResult) error { return nil })

// 游标早停分页查询
_ = client.WithPage(ctx, 1, strata.ReadRequest{
	From: now.Add(-15 * time.Minute), To: now.Add(time.Minute), Limit: 200,
}, func(page *strata.PageResult) error { return nil })

// 零全量物化流式扫描
_ = client.Scan(ctx, 1, strata.ScanQuery{
	From: now.Add(-time.Hour), To: now.Add(time.Minute),
	Filters: []strata.Filter{{Dim: 1, Op: strata.TagEq, Values: []string{"tcp"}}},
}, func(b strata.ScanBatch) error { return nil })

// 事务 KV
reg, _ := client.Region("config")
_ = reg.Put("theme", []byte("dark"))

// 空闲时回收缓冲池、缓存与冷 mmap 页
client.Reclaim()
```

## 可选事件日志（`strata/eventlog`）

按需导入 `github.com/UruhaLushia/strata/eventlog` 以保留独立的本地有界结构化事件历史。它与根包双向零依赖，默认不压缩且无常驻块目录，可通过 `Options.Codec` 按需启用压缩。

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

## 基准测试

90 天网关数据集：**1,060,830 行**、7 维标签 + 4 指标、**22,406 个封存块**（`1m`/`1h`/`1d`）+ 2,000 KV。环境：Linux amd64（`Intel i5-9300H`，Go 1.27；`go.mod` 要求 Go 1.26+；除注明外为 `mmap=false`）。

### 体积、内存与查询延迟（`codec/zstd`，`zstd.Fastest`）

| 数据集 | `.db` 大小 | 打开态 `HeapAlloc` | `Reclaim` 后 `RssAnon`（`mmap=true`） | 增量 `Compact` |
|---|---:|---:|---:|---:|
| 常规网关（5K 域名、2K IP） | 10.1 MiB | 2.20 MiB | 13.48 MiB | 172–176 ms |
| 极端高基数（106 万唯一 IPv6） | 26.4 MiB | 2.31 MiB | 14.42 MiB | 446–462 ms |

| 基准项（`BenchmarkGateway90*`） | 常规网关 | 极端高基数（106 万 IPv6） |
|---|---:|---:|
| `SummaryOnly`（90 天零盘读） | 36.9 µs / 881 B / 7 allocs | 36.9 µs / 881 B / 7 allocs |
| `ReadLast15mPage200` | 41.3 µs / 8.6 KiB / 13 allocs | 68.4 µs / 15.5 KiB / 214 allocs |
| `Read90dDeepOffset5000` | 84.2 µs / 80.3 KiB / 14 allocs | 86.8 µs / 83.6 KiB / 214 allocs |
| `OutboundLookup`（15m / 1h / 24h / 7d 单键命中） | 77.5 µs / 203.2 µs / 500.0 µs / 907.6 µs | 65.0 µs / 64.4 µs / 64.5 µs / 65.1 µs |
| `OutboundLookup`（未知冷键未命中） | 1.33 µs / 2.3 KiB / 5 allocs | 10.6 µs / 10.4 KiB / 13 allocs |
| `Scan90dSparseMeta`（76 行） | 2.04 ms / 36.3 KiB / 33 allocs | 4.44 ms / 40.8 KiB / 33 allocs |
| `Scan90dMediumMeta`（1,768 行） | 5.33 ms / 64.2 KiB / 26 allocs | 8.20 ms / 89.7 KiB / 27 allocs |
| `Scan90dDenseMeta`（344K / 530K 行） | 7.97 ms / 401 KiB / 33 allocs | 12.79 ms / 638 KiB / 35 allocs |
| `Scan90dTimeFirstIDs`（735K / 106 万行） | 8.10 ms / 371 KiB / 16 allocs | 12.99 ms / 592 KiB / 17 allocs |
| `AggregateFiltered` | 1.17 ms / 236 KiB / 81 allocs | — |
| `AggregateTopK`（5K 域名 Top-20） | 4.31 ms / 804 KiB / 107 allocs | — |
| `AggregateDomain`（5K 域名） | 5.47 ms / 1.13 MiB / 72 allocs | — |
| `ExtremeDestIPTopK`（106 万 IPv6 精确 Top-50） | — | 38.30 ms / 2.41 MiB / 218 allocs |

8 并发重查询饱和压测下（`mmap=true`），`SummaryOnly` 稳定在 `p99 <= 183 µs`，有界小页 `Read` 稳定在 `p99 <= 424 µs`（`MemoryConstrained` 下 `<= 303 µs`）。

### 压缩算法对照

同一 90 天 106 万行数据集按各算法独立写入压实的对比（`BenchmarkGateway90CodecLatency`；二进制体积为 `CGO_ENABLED=0 -trimpath -ldflags="-s -w"` 且含 `OpenClient` + `Compact`）：

| 压缩算法 | 二进制体积 | `.db` 大小（常规 / 极端） | 常规 `15m` / `24h` 命中 | `Scan90d` 密集（常规 / 极端） | `AggregateTopK` |
|---|---:|---:|---:|---:|---:|
| `None`（默认） | **2.14 MiB** | 27.3 / 73.6 MiB | **37.9 µs** / **263.6 µs** | **5.51 ms** / **9.34 ms** | 3.82 ms |
| `codec/s2`（`Default`） | 2.75 MiB | 11.5 / 37.1 MiB | 47.5 µs / 304.7 µs | 7.76 ms / 11.31 ms | **3.66 ms** |
| `codec/zstd`（`Fastest`） | 3.09 MiB | 10.1 / 26.4 MiB | 86.6 µs / 563.2 µs | 9.16 ms / 13.24 ms | 4.56 ms |
| `codec/flate`（`BestSpeed`） | 2.44 MiB | **9.68 / 25.0 MiB** | 276.3 µs / 1.18 ms | 17.82 ms / 20.69 ms | 7.36 ms |

四种算法下常规冷键未命中均稳定在 `1.65–1.76 µs`，`Scan90d/TimeFirst` 排序一致（`4.83 / 7.07 / 8.50 / 17.01 ms`）。因本表各行在独立压实轮次下测得，与上方完整 `BenchmarkGateway90Day` 基准存在正常轮次波动。

### 事件日志（`BenchmarkEventlogCodecs`）

69B 结构化 JSON 载荷（等价 4 MiB 原始数据，每帧 20 条；单用 `eventlog` 无压缩裁剪二进制体积为 `1.50 MiB`）：

| 压缩算法 | 盘上体积 | `AppendFlush`（256 条批） | `Scan` 近 200 条 | `Scan` 全量 |
|---|---:|---:|---:|---:|
| `None`（默认） | 4.00 MiB | **52.9 ns/event**（1,304 MB/s，0 allocs） | **15.6 µs**（297 B / 5 allocs） | **1.56 ms** |
| `codec/s2`（`Default`） | 1.13 MiB | 71.2 ns/event（970 MB/s，0 allocs） | 21.9 µs（297 B / 5 allocs） | 2.53 ms |
| `codec/zstd`（`Fastest`） | **582 KiB** | 243.6 ns/event（283 MB/s，0 allocs） | 57.1 µs（302 B / 5 allocs） | 11.15 ms |
| `codec/flate`（`BestSpeed`） | 805 KiB | 303.9 ns/event（227 MB/s，1 alloc） | 87.1 µs（544 B / 15 allocs） | 17.90 ms |

## 运行测试

```sh
go test -count=1 ./...
CGO_ENABLED=1 go test -count=1 -race ./...

# 可选：90 天百万行内存报告与基准测试
STRATA_STRESS_90D=1 go test -v -count=1 -run 'TestGateway90DayMillionReport|TestGateway90ReclaimedFirstQuery' .
STRATA_STRESS_90D=1 go test -bench=BenchmarkGateway90 -benchmem -run=^$ .
```

## 开源协议

[GNU General Public License v3.0](LICENSE)
