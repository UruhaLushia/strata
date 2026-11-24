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
	target := fineWatermark(db.opts.Now().Unix())
	var size int64
	resetReady := false
	for _, s := range db.series {
		for _, r := range s.rules {
			if r.to <= target {
				resetReady = true
				break
			}
		}
	}
	if err == nil {
		for _, m := range metas {
			if m.seq <= db.head.sealedWalSeq {
				continue
			}
			ok := false
			switch m.typ {
			case frameDelta:
				ok = m.hasTS && m.maxTS < target
			case frameReset:
				ok = m.hasTS && m.maxTS <= target
			case frameKVBatch:
				ok = m.hasTS
			}
			if !ok {
				break
			}
			size += frameHeader + int64(m.length)
		}
	}
	db.mu.RUnlock()
	if size >= db.opts.WALCompactBytes || resetReady {
		_ = db.Compact()
	}
}
