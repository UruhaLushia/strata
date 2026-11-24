// Package strata implements an embedded, memory-bounded single-file time-series
// and key-value storage engine in pure Go.
//
// Strata stores multi-dimensional telemetry series (up to 8 tag dimensions and 4
// metrics per row) alongside transactional KV regions inside one crash-safe file
// with WAL durability, multi-resolution rollups (1m / 1h / 1d), columnar STR2
// block compression, adaptive 5-mode inverted indexes, and hybrid inline/external
// paged dictionaries for high-cardinality dimensions.
//
// Applications typically interact through [OpenClient] or [NewClient], which
// manage series/region handle caching, bounded query concurrency, cursor-based
// pagination ([Client.Read] / [Client.WithPage]), streaming scans ([Client.Scan]
// / [Client.ScanSeq]), pushed-down aggregations ([Client.Aggregate] /
// [Client.WithAggregate]), pure-byte transactional KV ([Region] / [Batch] /
// [Region.Scan] / [Region.ScanSeq]), and explicit memory reclamation
// ([Client.Reclaim]).
package strata
