package strata

func fineWatermark(nowSec int64) int64 {
	return (nowSec/60)*60 - 120
}

func (db *DB) autoCompact() {
	db.mu.RLock()
	if db.closed || !db.writer {
		db.mu.RUnlock()
		return
	}
	metas, err := scanWALFrameMetas(db.data, int64(db.head.walOffset), db.walPos)
	if err != nil {
		db.mu.RUnlock()
		return
	}
	nowSec := db.opts.Now().Unix()
	var size int64
	resetReady := false
	var resetAllMaxEpoch map[uint16]uint64
	for _, s := range db.series {
		for _, r := range s.rules {
			if !r.isAll() {
				continue
			}
			if resetAllMaxEpoch == nil {
				resetAllMaxEpoch = make(map[uint16]uint64)
			}
			if prev, ok := resetAllMaxEpoch[s.id]; !ok || r.oldEpoch > prev {
				resetAllMaxEpoch[s.id] = r.oldEpoch
			}
		}
	}
	if len(resetAllMaxEpoch) > 0 {
		for _, m := range db.sealed {
			if maxEp, ok := resetAllMaxEpoch[m.series]; ok && m.epoch <= maxEp {
				resetReady = true
				break
			}
		}
	}
	_, _, _, sealedSeq, _, _ := computeCompactWatermarks(metas, db.head, db.appendSeq, nowSec)
	for _, m := range metas {
		if m.seq <= db.head.sealedWalSeq {
			continue
		}
		if m.seq > sealedSeq {
			break
		}
		size += frameHeader + int64(m.length)
		if m.typ == frameReset {
			resetReady = true
		}
	}
	db.mu.RUnlock()
	if size >= db.opts.WALCompactBytes || resetReady {
		_ = db.Compact()
	}
}
