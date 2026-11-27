package strata

import "time"

func defaultOptions(o Options) (Options, memPolicy) {
	if o.CompressionLevel > CompressionBest {
		o.CompressionLevel = CompressionFastest
	}
	policy := compileMemPolicy(o)
	if o.WALCompactBytes <= 0 {
		o.WALCompactBytes = policy.walCompactBytes
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = 30 * time.Second
	}
	if o.SyncInterval <= 0 {
		o.SyncInterval = 5 * time.Minute
	}
	if o.Retention1m == 0 {
		o.Retention1m = 14 * 24 * time.Hour
	}
	if o.Retention1h == 0 {
		o.Retention1h = 90 * 24 * time.Hour
	}
	if o.Retention1d == 0 {
		o.Retention1d = 365 * 24 * time.Hour
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o, policy
}

func defaultClientOptions(o ClientOptions) ClientOptions {
	if o.MaxPageLimit <= 0 || o.MaxPageLimit > MaxPageLimit {
		o.MaxPageLimit = MaxPageLimit
	}
	if o.DefaultPageLimit <= 0 {
		o.DefaultPageLimit = DefaultPageLimit
	}
	if o.DefaultPageLimit > o.MaxPageLimit {
		o.DefaultPageLimit = o.MaxPageLimit
	}
	return o
}

func normalizeMask(mask uint8) uint8 {
	if mask == 0 {
		return 0x0f
	}
	return mask & 0x0f
}

func (c SeriesConfig) valid() bool { return c.Arity >= 1 && c.Arity <= 8 }

func defaultSeriesConfig(arity int) SeriesConfig {
	if arity < 1 || arity > 8 {
		arity = 1
	}
	return SeriesConfig{Arity: arity, SummaryOps: [4]MetricOp{OpSum, OpSum, OpSum, OpSum}, RowOps: [4]MetricOp{OpSum, OpSum, OpSum, OpSum}}
}

func (s Summary) normalized() Summary { s.Mask = normalizeMask(s.Mask); return s }
func (r Row) normalized() Row         { r.Mask = normalizeMask(r.Mask); return r }
func minuteTS(t time.Time) int64      { return t.Unix() / 60 * 60 }
func hourTS(t int64) int64            { return t / 3600 * 3600 }
func dayTS(t int64) int64             { return t / 86400 * 86400 }

func resWidth(res byte) int64 {
	switch res {
	case resHour:
		return 3600
	case resDay:
		return 86400
	default:
		return 60
	}
}
