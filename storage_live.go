package strata

func (db *DB) mergeLive(src *bucket, ownSrc bool) {
	key := bucketKey{series: src.series, minute: src.ts, epoch: src.epoch}
	if src.order > db.order {
		db.order = src.order
	}
	dst := db.live[key]
	if dst == nil {
		if ownSrc {
			dst = src
		} else {
			dst = cloneBucket(src)
		}
		db.live[key] = dst
		return
	}
	s := db.series[src.series]
	var sumOps, rowOps [4]MetricOp
	if s != nil {
		sumOps, rowOps = s.cfg.SummaryOps, s.cfg.RowOps
	} else {
		sumOps, rowOps = src.summaryOps, src.rowOps
	}
	mergeSummary(&dst.summary, src.summary, sumOps, src.order, dst.order)
	mergeIDRows(&dst.rows, src.rows, rowOps, src.order, dst.order)
	if src.order > dst.order {
		dst.order = src.order
	}
}

func (db *DB) resetSeriesDictAndLiveLocked(seriesID uint16, oldEpoch uint64) {
	for dim := range uint8(8) {
		delete(db.dict.dims, dictDimKey{series: seriesID, dim: dim})
	}
	for k := range db.live {
		if k.series == seriesID && k.epoch <= oldEpoch {
			delete(db.live, k)
		}
	}
}
