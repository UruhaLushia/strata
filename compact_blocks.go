package strata

import "slices"

type blockIDKey struct {
	series uint16
	start  int64
	epoch  uint64
}

type dayWindowKey struct {
	series uint16
	day    int64
}

type compactPlan struct {
	dbPath          string
	oldHead         superblock
	sourceView      *fileView
	oldSealed       []sealedMeta
	oldInvert       []invertChunkMeta
	series          map[uint16]*seriesSnap
	eligibleFrames  []walFrameMeta
	tailMetas       []walFrameMeta
	tailResetAllSeq map[uint16]uint64
	walRefs         []walBucketRef
	nowSec          int64
	fineTarget      int64
	hourTarget      int64
	dayTarget       int64
	ret1mCutoff     int64
	ret1hCutoff     int64
	ret1dCutoff     int64
	kvBlocks        []kvSSTBlock
	catalogRaw      []byte
	dictSnap        *dictCompactSnap
	optional        []optionalSection
	level           CompressionLevel
	kvPassthrough   bool
	policy          memPolicy
	invertRunBytes  int
}

func sealableFrameMetas(metas []walFrameMeta, sealed uint64, fineTarget int64) (uint64, int, uint64) {
	last := sealed
	eligibleCount := 0
	var kvRev uint64
	for i, m := range metas {
		if m.seq <= sealed {
			continue
		}
		ok := false
		switch m.typ {
		case frameDelta:
			ok = m.hasTS && m.maxTS < fineTarget
		case frameReset:
			ok = m.hasTS && (m.isResetAll() || m.maxTS <= fineTarget)
		case frameKVBatch:
			ok = m.hasTS
			if ok && m.kvRev > kvRev {
				kvRev = m.kvRev
			}
		}
		if !ok {
			break
		}
		last = m.seq
		eligibleCount = i + 1
	}
	return last, eligibleCount, kvRev
}

func computeCompactWatermarks(metas []walFrameMeta, oldHead superblock, fenceSeq uint64, nowSec int64) (fineTarget, hourTarget, dayTarget int64, sealedSeq, sealedKVRev uint64, eligible []walFrameMeta) {
	fineTarget = fineWatermark(nowSec)
	var eligEnd int
	sealedSeq, eligEnd, sealedKVRev = sealableFrameMetas(metas, oldHead.sealedWalSeq, fineTarget)
	if sealedKVRev < oldHead.sealedKVRev {
		sealedKVRev = oldHead.sealedKVRev
	}
	for {
		late := fineTarget
		for _, m := range metas {
			if m.seq <= sealedSeq || m.seq > fenceSeq || m.typ != frameDelta || !m.hasTS {
				continue
			}
			if m.minTS < late {
				late = m.minTS
			}
		}
		if late >= fineTarget {
			break
		}
		fineTarget = (late / 60) * 60
		sealedSeq, eligEnd, sealedKVRev = sealableFrameMetas(metas, oldHead.sealedWalSeq, fineTarget)
		if sealedKVRev < oldHead.sealedKVRev {
			sealedKVRev = oldHead.sealedKVRev
		}
	}
	hourTarget = min(hourTS(nowSec), hourTS(fineTarget))
	dayTarget = dayTS(hourTarget) - 86400
	for _, m := range metas[:eligEnd] {
		if m.seq > oldHead.sealedWalSeq && m.typ == frameDelta {
			eligible = append(eligible, m)
		}
	}
	return fineTarget, hourTarget, dayTarget, sealedSeq, sealedKVRev, eligible
}

type rebuildMarks struct {
	fine     map[blockIDKey]bool
	hour     map[blockIDKey]bool
	day      map[blockIDKey]bool
	dirtyDay map[dayWindowKey]bool
	dirtyHr  map[dayWindowKey]bool
}

func computeRebuildMarks(plan *compactPlan) rebuildMarks {
	rm := rebuildMarks{
		fine:     make(map[blockIDKey]bool),
		hour:     make(map[blockIDKey]bool),
		day:      make(map[blockIDKey]bool),
		dirtyDay: make(map[dayWindowKey]bool),
		dirtyHr:  make(map[dayWindowKey]bool),
	}
	existHour := make(map[blockIDKey]bool)
	existDay := make(map[blockIDKey]bool)

	for _, m := range plan.oldSealed {
		s := plan.series[m.series]
		if s == nil {
			continue
		}
		k := blockIDKey{series: m.series, start: m.start, epoch: m.epoch}
		switch m.res {
		case resHour:
			existHour[k] = true
			if ruleIntersects(s.rules, m.start, m.end(), m.epoch) {
				rm.hour[k] = true
				rm.dirtyHr[dayWindowKey{m.series, m.start}] = true
				rm.dirtyDay[dayWindowKey{m.series, dayTS(m.start)}] = true
			}
		case resDay:
			existDay[k] = true
			if ruleIntersects(s.rules, m.start, m.end(), m.epoch) {
				rm.day[k] = true
				rm.dirtyDay[dayWindowKey{m.series, m.start}] = true
			}
		}
	}
	for _, ref := range plan.walRefs {
		s := plan.series[ref.series]
		if s == nil || excludedByRuleSlice(s.rules, ref.ts, ref.epoch) {
			continue
		}
		hr, dy := hourTS(ref.ts), dayTS(ref.ts)
		rm.fine[blockIDKey{ref.series, ref.ts, ref.epoch}] = true
		rm.dirtyHr[dayWindowKey{ref.series, hr}] = true
		rm.dirtyDay[dayWindowKey{ref.series, dy}] = true
		if hr < plan.hourTarget {
			rm.hour[blockIDKey{ref.series, hr, ref.epoch}] = true
		}
		if dy < plan.dayTarget {
			rm.day[blockIDKey{ref.series, dy, ref.epoch}] = true
		}
	}
	for _, m := range plan.oldSealed {
		s := plan.series[m.series]
		if s == nil {
			continue
		}
		switch m.res {
		case resMinuteHR:
			if excludedByRuleSlice(s.rules, m.start, m.epoch) {
				rm.dirtyDay[dayWindowKey{m.series, dayTS(m.start)}] = true
				rm.dirtyHr[dayWindowKey{m.series, hourTS(m.start)}] = true
				continue
			}
			hr, dy := hourTS(m.start), dayTS(m.start)
			hk := blockIDKey{m.series, hr, m.epoch}
			if hr < plan.hourTarget && !existHour[hk] && (hr >= plan.ret1hCutoff || ruleIntersects(s.rules, hr, hr+3600, m.epoch)) {
				rm.hour[hk] = true
				rm.dirtyHr[dayWindowKey{m.series, hr}] = true
				rm.dirtyDay[dayWindowKey{m.series, dy}] = true
			}
			dk := blockIDKey{m.series, dy, m.epoch}
			if dy < plan.dayTarget && !existDay[dk] && (dy >= plan.ret1dCutoff || ruleIntersects(s.rules, dy, dy+86400, m.epoch)) {
				rm.day[dk] = true
				rm.dirtyDay[dayWindowKey{m.series, dy}] = true
			}
		case resHour:
			dy := dayTS(m.start)
			dk := blockIDKey{m.series, dy, m.epoch}
			if dy < plan.dayTarget && !existDay[dk] && !ruleIntersects(s.rules, m.start, m.end(), m.epoch) &&
				(dy >= plan.ret1dCutoff || ruleIntersects(s.rules, dy, dy+86400, m.epoch)) {
				rm.day[dk] = true
				rm.dirtyDay[dayWindowKey{m.series, dy}] = true
			}
		}
	}
	return rm
}

func streamCompactBlocks(plan *compactPlan, bw *compactBlockWriter) error {
	rm := computeRebuildMarks(plan)
	slices.SortFunc(plan.walRefs, func(a, b walBucketRef) int {
		if a.series != b.series {
			if a.series < b.series {
				return -1
			}
			return 1
		}
		if ad, bd := dayTS(a.ts), dayTS(b.ts); ad != bd {
			if ad < bd {
				return -1
			}
			return 1
		}
		if ah, bh := hourTS(a.ts), hourTS(b.ts); ah != bh {
			if ah < bh {
				return -1
			}
			return 1
		}
		if a.frameIdx < b.frameIdx {
			return -1
		}
		if a.frameIdx > b.frameIdx {
			return 1
		}
		return 0
	})

	var walBuf []byte
	oldPos, walPos := 0, 0
	for oldPos < len(plan.oldSealed) || walPos < len(plan.walRefs) {
		var cur dayWindowKey
		hasOld := oldPos < len(plan.oldSealed)
		hasWal := walPos < len(plan.walRefs)
		if hasOld {
			cur = dayWindowKey{plan.oldSealed[oldPos].series, dayTS(plan.oldSealed[oldPos].start)}
		}
		if hasWal {
			wk := dayWindowKey{plan.walRefs[walPos].series, dayTS(plan.walRefs[walPos].ts)}
			if !hasOld || wk.series < cur.series || (wk.series == cur.series && wk.day < cur.day) {
				cur = wk
			}
		}
		oldLo := oldPos
		for oldPos < len(plan.oldSealed) && plan.oldSealed[oldPos].series == cur.series && dayTS(plan.oldSealed[oldPos].start) == cur.day {
			oldPos++
		}
		walLo := walPos
		for walPos < len(plan.walRefs) && plan.walRefs[walPos].series == cur.series && dayTS(plan.walRefs[walPos].ts) == cur.day {
			walPos++
		}
		s := plan.series[cur.series]
		if s == nil {
			continue
		}
		if err := processDayWindow(plan, &rm, s, cur.day, oldLo, oldPos, walLo, walPos, bw, &walBuf); err != nil {
			return err
		}
	}
	return nil
}
