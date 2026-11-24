package strata

const livePostingHighCardThreshold = 16384

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
		if !db.livePostingsOverflow {
			if len(db.liveOrder) < 64 {
				slot := uint8(len(db.liveOrder))
				db.liveOrder = append(db.liveOrder, key)
				db.liveSlot[key] = slot
			} else {
				db.livePostingsOverflow = true
				clear(db.livePostings)
			}
		}
	} else {
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
	if db.livePostingsOverflow || db.policy.constrained {
		return
	}
	slot, ok := db.liveSlot[key]
	if !ok {
		return
	}
	bit := uint64(1) << slot
	for _, row := range src.rows {
		for dim, tagID := range row.tags {
			if tagID == 0 {
				continue
			}
			dk := dictDimKey{series: src.series, dim: uint8(dim)}
			if db.liveSkipDims[dk] {
				continue
			}
			if db.dict.dimCount(src.series, uint8(dim)) > livePostingHighCardThreshold {
				db.liveSkipDims[dk] = true
				continue
			}
			postKey := encodeInvertKey(invertKey{series: src.series, dim: uint8(dim), tag: tagID})
			db.livePostings[postKey] |= bit
		}
	}
}
