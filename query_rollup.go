package strata

import (
	"sort"
	"time"
)

// RollupLocalHours merges hour-resolution SegmentSummary entries into local timezone buckets (DST-aware).
func RollupLocalHours(segments []SegmentSummary, loc *time.Location, ops [4]MetricOp) []LocalHourBucket {
	if len(segments) == 0 {
		return nil
	}
	if loc == nil {
		loc = time.UTC
	}
	type key struct {
		date      string
		hour, off int
	}
	groups := make(map[key]*LocalHourBucket)
	for _, seg := range segments {
		if seg.Meta.Resolution != time.Hour {
			continue
		}
		t := seg.Meta.CoveredFrom.In(loc)
		_, off := t.Zone()
		k := key{date: t.Format("2006-01-02"), hour: t.Hour(), off: off}
		x := groups[k]
		if x == nil {
			x = &LocalHourBucket{Date: k.date, Hour: k.hour, OffsetSeconds: k.off}
			groups[k] = x
		}
		mergeSummary(&x.Summary, seg.Summary, ops, 0, 0)
	}
	out := make([]LocalHourBucket, 0, len(groups))
	for _, x := range groups {
		out = append(out, *x)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Date != out[j].Date {
			return out[i].Date < out[j].Date
		}
		if out[i].Hour != out[j].Hour {
			return out[i].Hour < out[j].Hour
		}
		return out[i].OffsetSeconds < out[j].OffsetSeconds
	})
	return out
}
