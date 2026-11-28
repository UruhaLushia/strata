# Strata

[English](README.md) | 简体中文

一个纯 Go（`CGO_ENABLED=0`）实现的**极轻量、内存有界的存储引擎**。

## 定位与适用边界

根包在单个文件内存储固定结构的时序数据和事务键值数据，具备小体积、可控内存上限与标签过滤下推能力。

- **固定遥测 Schema**：每条序列固定支持最多 8 个字符串标签维度（`Arity: 1..8`）与 4 个 `int64` 指标（`OpSum`、`OpMin`、`OpMax`、`OpLast`）。
- **内置多级汇总**：自动按 `1m` / `1h` / `1d` 降采样并按层级过期，内置本地时区按小时聚合（`RollupLocalHours`）。
- **标签检索**：`STR2` 列式分块、5 模式自适应倒排索引、内联与外置分页混合词典、内置下推过滤（`TagEq`、`TagPrefix`、`TagSuffix`、`TagContains`、`TagCIDR`），以及防止首屏小页被重查询阻塞的 1 槽有界轻读通道。
- **同文件事务 KV**：在同一单文件内提供纯字节事务 KV 分区（`Region`、`Batch`、`Scan`、`ScanSeq`），支持 TTL、CAS 与快照无锁流式扫描。
- **内存优先与可插拔压缩**：提供 `MemoryBalanced` / `MemoryConstrained` 双档预算、零指针（`noscan`）索引结构、显式 `Client.Reclaim()`（Unix 下触发 `MADV_DONTNEED`），并通过 `strata/codec` 支持可插拔压缩（默认不压缩、零外部压缩依赖；按需导入 `codec/zstd`、`codec/s2`、`codec/flate` 或自定义实现）。

v0.2.0 根包使用 `UNIDBv02` 文件格式，打开 v0.1.0 数据库会报错。

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

## 可选事件历史

按需导入 `github.com/UruhaLushia/strata/eventlog`，在本地保留有界的结构化事件历史。根包与 `eventlog` 双向零依赖，未导入时不产生额外开销；同样通过 `Options.Codec` 支持可选压缩（默认不压缩、无常驻块目录）。

`Open` 返回 `(*eventlog.Log, error)`。`Append`、`Flush()`、`Sync()` 和 `Close()` 均返回 `error`。`Scan` 按追加顺序倒序扫描，限制扫描范围并返回 `(eventlog.ScanResult, error)`。

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

90 天网关数据集：**1,060,830 行**、7 维标签 + 4 指标（`OpSum/OpMin/OpMax/OpLast`）、**22,406 个封存块**（`1m`/`1h`/`1d`）+ 2,000 KV。环境：Linux amd64（`Intel i5-9300H`，Go 1.27，除注明外为 `mmap=false`）。

### 文件体积与常驻内存

| 数据集 | `.db` 大小 | 打开态 `HeapAlloc` | `Reclaim` 后 `RssAnon`（`mmap=true`） | 增量 `Compact` |
|---|---:|---:|---:|---:|
| 常规网关（5K 域名、2K IP） | 10.06 MiB | 2.20 MiB | 13.48 MiB | 172–176 ms |
| 极端高基数（106 万唯一 IPv6） | 26.36 MiB | 2.31 MiB | 14.42 MiB | 446–462 ms |

### 查询延迟与分配（`codec/zstd`，`zstd.Fastest`）

| 基准项 | 常规网关 | 极端高基数（106 万 IPv6） |
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

### 压缩算法基准对照

同一 90 天 106 万行数据集（`BenchmarkGateway90CodecLatency`，`mmap=false`，各组合均按对应算法真实写入并压实）：

| 压缩算法 | `.db` 大小（常规 / 极端） | 常规 `15m` / `24h` 命中 | `Read15m` 命中 | `Scan90d` 密集 / 全扫 | `AggregateTopK` | 极端 `15m` 命中 / `Scan90d` 密集 |
|---|---:|---:|---:|---:|---:|---:|
| `None`（默认） | 27.3 MiB / 73.6 MiB | **37.9 µs** / **263.6 µs** | **40.1 µs** | **5.51 ms** / **4.83 ms** | 3.82 ms | **41.9 µs** / **9.34 ms** |
| `codec/s2`（`Default`） | 11.5 MiB / 37.1 MiB | 47.5 µs / 304.7 µs | 49.7 µs | 7.76 ms / 7.07 ms | **3.66 ms** | 56.1 µs / 11.31 ms |
| `codec/zstd`（`Fastest`） | 10.1 MiB / 26.4 MiB | 86.6 µs / 563.2 µs | 86.6 µs | 9.16 ms / 8.50 ms | 4.56 ms | 69.1 µs / 13.24 ms |
| `codec/flate`（`BestSpeed`） | **9.68 MiB** / **25.0 MiB** | 276.3 µs / 1.18 ms | 281.3 µs | 17.82 ms / 17.01 ms | 7.36 ms | 141.4 µs / 20.69 ms |

### 编译体积与 `eventlog` 吞吐

纯 Go 裁剪编译体积（`CGO_ENABLED=0 -trimpath -ldflags="-s -w"`，根包测试含 `OpenClient` + `Compact`）：

| 编译组合 | 二进制大小 | 相对 `zstd` 缩减 |
|---|---:|---:|
| `strata`（`None`，默认） | 2.14 MiB | -976 KiB |
| `strata` + `codec/flate`（标准库） | 2.44 MiB | -672 KiB |
| `strata` + `codec/s2` | 2.75 MiB | -356 KiB |
| `strata` + `codec/zstd` | 3.09 MiB | — |
| 单用 `eventlog`（`None`） | 1.50 MiB | -1.59 MiB |

`eventlog` 四算法对照（`BenchmarkEventlogCodecs`，69B 结构化 JSON 载荷，等价 4 MiB 原始数据，每帧 20 条）：

| 压缩算法 | 盘上体积 | `AppendFlush`（256 条批） | `Scan` 近 200 条 | `Scan` 全量 |
|---|---:|---:|---:|---:|
| `None`（默认） | 4.00 MiB | **52.9 ns/event**（1,304 MB/s，0 allocs） | **15.6 µs**（297 B / 5 allocs） | **1.56 ms** |
| `codec/s2`（`Default`） | 1.13 MiB | 71.2 ns/event（970 MB/s，0 allocs） | 21.9 µs（297 B / 5 allocs） | 2.53 ms |
| `codec/zstd`（`Fastest`） | **582 KiB** | 243.6 ns/event（283 MB/s，0 allocs） | 57.1 µs（302 B / 5 allocs） | 11.15 ms |
| `codec/flate`（`BestSpeed`） | 805 KiB | 303.9 ns/event（227 MB/s，1 alloc） | 87.1 µs（544 B / 15 allocs） | 17.90 ms |

8 并发重查询饱和压测下（`mmap=true`），`SummaryOnly` 稳定在 `p99 <= 183 µs`，有界小页 `Read` 稳定在 `p99 <= 424 µs`（`MemoryConstrained` 下 `<= 303 µs`）。

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
