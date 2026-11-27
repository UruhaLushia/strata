// Package strata implements lightweight, memory-bounded embedded storage for
// time-series metrics and transactional key-value data in a single file.
//
// Applications can store traffic metrics, game launch statistics, or API usage
// alongside local settings and state. Use [OpenClient] or [NewClient] for
// time-series rollups, aggregation, paginated reads, streaming scans, and
// transactions across key-value regions.
//
// The independent github.com/UruhaLushia/strata/eventlog subpackage provides
// optional, bounded structured event history.
package strata
