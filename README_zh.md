# Strata

[English](README.md) | 简体中文

一个纯 Go（`CGO_ENABLED=0`）实现的**极轻量、内存有界的嵌入式存储套件**，适用于代理客户端流量统计、游戏启动统计、AI API 用量统计以及桌面与移动后台工具。代理网关的 90 天百万行数据集是核心压测基线。

## 定位与适用边界

根包在单个文件内存储固定结构的时序数据和事务键值数据，具备小体积、可控内存上限与标签过滤下推能力。

- **固定遥测 Schema**：每条序列固定支持最多 8 个字符串标签维度（`Arity: 1..8`）与 4 个 `int64` 指标（`OpSum`、`OpMin`、`OpMax`、`OpLast`）。
- **内置多级汇总**：自动按 `1m` / `1h` / `1d` 降采样并按层级过期，内置本地时区按小时聚合（`RollupLocalHours`）。
- **标签检索**：`STR2` 列式分块、5 模式自适应倒排索引、内联与外置分页混合词典、内置下推过滤（`TagEq`、`TagPrefix`、`TagSuffix`、`TagContains`、`TagCIDR`），以及防止首屏小页被重查询阻塞的 1 槽有界轻读通道。
- **同文件事务 KV**：在同一单文件内提供纯字节事务 KV 分区（`Region`、`Batch`、`Scan`、`ScanSeq`），支持 TTL、CAS 与快照无锁流式扫描。
- **内存优先**：提供 `MemoryBalanced` / `MemoryConstrained` 双档预算、零指针（`noscan`）索引结构、显式 `Client.Reclaim()`（Unix 下触发 `MADV_DONTNEED`），外部依赖仅 `klauspost/compress` 与 `golang.org/x/sys`。

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

按需导入 `github.com/UruhaLushia/strata/eventlog`，在本地保留有界的结构化事件历史。根包与 `eventlog` 双向零依赖，应用未导入该子包时没有事件历史开销。记录直接存盘，无 Zstd 和常驻块目录开销。

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
| 常规网关（5K 域名、2K IP） | 10.15 MiB | 2.22 MiB | 9.56 MiB | 182–195 ms |
| 极端高基数（106 万唯一 IPv6） | 26.45 MiB | 2.32 MiB | 14.40 MiB | 438–454 ms |

### 查询延迟与分配

| 基准项 | 常规网关 | 极端高基数（106 万 IPv6） |
|---|---:|---:|
| `SummaryOnly`（90 天零盘读） | 38.0 µs / 1.1 KiB / 9 allocs | 38.0 µs / 1.1 KiB / 9 allocs |
| `ReadLast15mPage200` | 41.1 µs / 11.1 KiB / 21 allocs | 75.0 µs / 15.8 KiB / 221 allocs |
| `Read90dDeepOffset5000` | 91.1 µs / 80.4 KiB / 14 allocs | 101.2 µs / 83.9 KiB / 214 allocs |
| `Scan90dSparseMeta`（76 行） | 3.03 ms / 80.6 KiB / 39 allocs | 5.80 ms / 454 KiB / 46 allocs |
| `Scan90dMediumMeta`（1,768 行） | 6.46 ms / 86.5 KiB / 95 allocs | 10.09 ms / 532 KiB / 103 allocs |
| `Scan90dDenseMeta`（344K / 530K 行） | 9.72 ms / 445 KiB / 98 allocs | 15.11 ms / 990 KiB / 102 allocs |
| `Scan90dTimeFirstIDs`（735K / 106 万行） | 8.93 ms / 430 KiB / 87 allocs | 13.23 ms / 957 KiB / 90 allocs |
| `AggregateFiltered` | 1.29 ms / 348 KiB / 87 allocs | — |
| `AggregateTopK`（5K 域名 Top-50） | 5.40 ms / 757 KiB / 158 allocs | — |
| `AggregateDomain`（5K 域名） | 6.68 ms / 1.10 MiB / 135 allocs | — |
| `ExtremeDestIPTopK`（106 万 IPv6 精确 Top-50） | — | 44.36 ms / 2.70 MiB / 355 allocs（`Reclaim` 后 `mmap=true` `39.0 ms`） |

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
