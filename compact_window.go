package strata

import (
	"slices"
	"sort"
)

func processDayWindow(plan *compactPlan, rm *rebuildMarks, s *seriesSnap, dayStart int64, oldLo, oldHi, walLo, walHi int, bw *compactBlockWriter, walBuf *[]byte) error {
	if !rm.dirtyDay[dayWindowKey{s.id, dayStart}] && walLo == walHi {
		for idx := oldLo; idx < oldHi; idx++ {
			m := plan.oldSealed[idx]
			if !keepPassthroughMeta(plan, s, m) {
				continue
			}
			if err := bw.writePassthrough(idx, m); err != nil {
				return err
			}
		}
		return nil
	}

	dayEpochs := make(map[uint64]bool)
	for idx := oldLo; idx < oldHi; idx++ {
		m := plan.oldSealed[idx]
		if rm.day[blockIDKey{s.id, dayStart, m.epoch}] {
			dayEpochs[m.epoch] = true
		}
	}
	for wi := walLo; wi < walHi; wi++ {
		ep := plan.walRefs[wi].epoch
		if rm.day[blockIDKey{s.id, dayStart, ep}] {
			dayEpochs[ep] = true
		}
	}
	dayAcc := make(map[uint64]sealedBlock, len(dayEpochs))

	// Pass through unaffected resDay blocks for this day first.
	for idx := oldLo; idx < oldHi; idx++ {
		m := plan.oldSealed[idx]
		if m.res != resDay {
			continue
		}
		if !rm.day[blockIDKey{s.id, dayStart, m.epoch}] && keepPassthroughMeta(plan, s, m) {
			if err := bw.writePassthrough(idx, m); err != nil {
				return err
			}
		}
	}

	// Process day hour by hour so at most one hour of 1m rows lives in memory.
	oPos, wPos := oldLo, walLo
	for oPos < oldHi || wPos < walHi {
		for oPos < oldHi && plan.oldSealed[oPos].res == resDay {
			oPos++
		}
		if oPos >= oldHi && wPos >= walHi {
			break
		}
		var hr int64
		hasO, hasW := oPos < oldHi, wPos < walHi
		if hasO {
			hr = hourTS(plan.oldSealed[oPos].start)
		}
		if hasW {
			wh := hourTS(plan.walRefs[wPos].ts)
			if !hasO || wh < hr {
				hr = wh
			}
		}
		hoLo := oPos
		for oPos < oldHi && (plan.oldSealed[oPos].res == resDay || hourTS(plan.oldSealed[oPos].start) == hr) {
			oPos++
		}
		hwLo := wPos
		for wPos < walHi && hourTS(plan.walRefs[wPos].ts) == hr {
			wPos++
		}
		if err := processHourWindow(plan, rm, s, hr, hoLo, oPos, hwLo, wPos, dayEpochs, dayAcc, bw, walBuf); err != nil {
			return err
		}
	}

	if len(dayAcc) > 0 {
		epochs := make([]uint64, 0, len(dayAcc))
		for ep := range dayAcc {
			epochs = append(epochs, ep)
		}
		slices.Sort(epochs)
		for _, ep := range epochs {
			d := dayAcc[ep]
			if d.end == 0 {
				continue
			}
			if dayStart >= plan.ret1dCutoff || ruleIntersects(s.rules, dayStart, dayStart+86400, ep) {
				if err := bw.writeFresh(d); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func processHourWindow(plan *compactPlan, rm *rebuildMarks, s *seriesSnap, hr int64, oldLo, oldHi, walLo, walHi int, dayEpochs map[uint64]bool, dayAcc map[uint64]sealedBlock, bw *compactBlockWriter, walBuf *[]byte) error {
	preserve := opsHasLast(s.cfg.RowOps)
	if !rm.dirtyHr[dayWindowKey{s.id, hr}] && walLo == walHi {
		hourFolded := make(map[uint64]bool)
		for idx := oldLo; idx < oldHi; idx++ {
			m := plan.oldSealed[idx]
			if m.res == resDay {
				continue
			}
			if keepPassthroughMeta(plan, s, m) {
				if err := bw.writePassthrough(idx, m); err != nil {
					return err
				}
			}
			if m.res == resHour && dayEpochs[m.epoch] && !ruleIntersects(s.rules, m.start, m.end(), m.epoch) {
				b, err := loadSealedBlockFromView(plan.sourceView, plan.oldHead.sections[secTSBlocks], m)
				if err != nil {
					return err
				}
				foldIntoCoarse(dayAcc, b, resDay, dayTS(hr), 86400, s, preserve)
				hourFolded[m.epoch] = true
			}
		}
		if len(dayEpochs) > len(hourFolded) {
			for idx := oldLo; idx < oldHi; idx++ {
				m := plan.oldSealed[idx]
				if m.res == resMinuteHR && dayEpochs[m.epoch] && !hourFolded[m.epoch] && !excludedByRuleSlice(s.rules, m.start, m.epoch) {
					b, err := loadSealedBlockFromView(plan.sourceView, plan.oldHead.sections[secTSBlocks], m)
					if err != nil {
						return err
					}
					foldIntoCoarse(dayAcc, b, resDay, dayTS(hr), 86400, s, preserve)
				}
			}
		}
		return nil
	}

	hourFine := make(map[blockIDKey]sealedBlock)
	passHourEpoch := make(map[uint64]bool)
	for idx := oldLo; idx < oldHi; idx++ {
		m := plan.oldSealed[idx]
		if m.res == resDay {
			continue
		}
		if m.res == resHour {
			hk := blockIDKey{s.id, hr, m.epoch}
			if !rm.hour[hk] && !ruleIntersects(s.rules, m.start, m.end(), m.epoch) {
				if keepPassthroughMeta(plan, s, m) {
					if err := bw.writePassthrough(idx, m); err != nil {
						return err
					}
				}
				passHourEpoch[m.epoch] = true
				if dayEpochs[m.epoch] {
					b, err := loadSealedBlockFromView(plan.sourceView, plan.oldHead.sections[secTSBlocks], m)
					if err != nil {
						return err
					}
					foldIntoCoarse(dayAcc, b, resDay, dayTS(hr), 86400, s, preserve)
				}
			}
			continue
		}
		if excludedByRuleSlice(s.rules, m.start, m.epoch) {
			continue
		}
		mk := blockIDKey{s.id, m.start, m.epoch}
		hk := blockIDKey{s.id, hr, m.epoch}
		needRollup := rm.hour[hk] || (dayEpochs[m.epoch] && !passHourEpoch[m.epoch])
		if !rm.fine[mk] {
			if keepPassthroughMeta(plan, s, m) {
				if err := bw.writePassthrough(idx, m); err != nil {
					return err
				}
			}
			if needRollup {
				b, err := loadSealedBlockFromView(plan.sourceView, plan.oldHead.sections[secTSBlocks], m)
				if err != nil {
					return err
				}
				b.passthrough = true
				b.preserveOrder = preserve
				hourFine[mk] = b
			}
		} else {
			b, err := loadSealedBlockFromView(plan.sourceView, plan.oldHead.sections[secTSBlocks], m)
			if err != nil {
				return err
			}
			b.preserveOrder = preserve
			hourFine[mk] = b
		}
	}

	// Stream ONLY the distinct WAL frames touching this (series, hour).
	lastFrame := ^uint32(0)
	for wi := walLo; wi < walHi; wi++ {
		fIdx := plan.walRefs[wi].frameIdx
		if fIdx == lastFrame {
			continue
		}
		lastFrame = fIdx
		var err error
		*walBuf, err = readWALPayload(plan.sourceView.data, plan.eligibleFrames[fIdx], *walBuf)
		if err != nil {
			return err
		}
		buckets, err := decodeDeltaForHour(*walBuf, s.id, hr)
		if err != nil {
			return err
		}
		for _, x := range buckets {
			if excludedByRuleSlice(s.rules, x.ts, x.epoch) {
				continue
			}
			b := sealedBlock{
				series: x.series, res: resMinuteHR, start: x.ts, end: x.ts + 60,
				epoch: x.epoch, order: x.order, summary: x.summary,
				rows: x.rows, preserveOrder: preserve,
			}
			mk := blockIDKey{b.series, b.start, b.epoch}
			if old, ok := hourFine[mk]; ok {
				mergeSummary(&old.summary, b.summary, s.cfg.SummaryOps, b.order, old.order)
				mergeIDRows(&old.rows, b.rows, s.cfg.RowOps, b.order, old.order)
				if b.order > old.order {
					old.order = b.order
				}
				old.passthrough = false
				old.preserveOrder = preserve
				hourFine[mk] = old
			} else {
				hourFine[mk] = b
			}
		}
	}

	fineList := make([]sealedBlock, 0, len(hourFine))
	for _, b := range hourFine {
		fineList = append(fineList, b)
	}
	sort.Slice(fineList, func(i, j int) bool {
		if fineList[i].start != fineList[j].start {
			return fineList[i].start < fineList[j].start
		}
		if fineList[i].epoch != fineList[j].epoch {
			return fineList[i].epoch < fineList[j].epoch
		}
		return fineList[i].order < fineList[j].order
	})

	hourAcc := make(map[uint64]sealedBlock)
	var freshFine []sealedBlock
	for _, b := range fineList {
		hk := blockIDKey{s.id, hr, b.epoch}
		if rm.hour[hk] || (dayEpochs[b.epoch] && !passHourEpoch[b.epoch]) {
			foldIntoCoarse(hourAcc, b, resHour, hr, 3600, s, preserve)
		}
		if !b.passthrough && (b.start >= plan.ret1mCutoff || ruleIntersects(s.rules, b.start, b.end, b.epoch)) {
			freshFine = append(freshFine, b)
		}
	}
	if err := bw.writeFreshBatch(freshFine); err != nil {
		return err
	}
	if len(hourAcc) > 0 {
		epochs := make([]uint64, 0, len(hourAcc))
		for ep := range hourAcc {
			epochs = append(epochs, ep)
		}
		slices.Sort(epochs)
		for _, ep := range epochs {
			h := hourAcc[ep]
			if h.end == 0 {
				continue
			}
			if rm.hour[blockIDKey{s.id, hr, ep}] && (hr >= plan.ret1hCutoff || ruleIntersects(s.rules, hr, hr+3600, ep)) {
				if err := bw.writeFresh(h); err != nil {
					return err
				}
			}
			if dayEpochs[ep] && !passHourEpoch[ep] {
				foldIntoCoarse(dayAcc, h, resDay, dayTS(hr), 86400, s, preserve)
			}
		}
	}
	return nil
}

func foldIntoCoarse(acc map[uint64]sealedBlock, src sealedBlock, res byte, start, width int64, s *seriesSnap, preserve bool) {
	dst := acc[src.epoch]
	if dst.end == 0 {
		dst = sealedBlock{
			series: src.series, res: res, start: start, end: start + width,
			epoch: src.epoch, preserveOrder: preserve,
		}
	}
	mergeSummary(&dst.summary, src.summary, s.cfg.SummaryOps, 1, 0)
	mergeIDRows(&dst.rows, src.rows, s.cfg.RowOps, 1, 0)
	if src.order > dst.order {
		dst.order = src.order
	}
	acc[src.epoch] = dst
}

func keepPassthroughMeta(plan *compactPlan, s *seriesSnap, m sealedMeta) bool {
	switch m.res {
	case resMinuteHR:
		return !excludedByRuleSlice(s.rules, m.start, m.epoch) &&
			(m.start >= plan.ret1mCutoff || ruleIntersects(s.rules, m.start, m.end(), m.epoch))
	case resHour:
		return !ruleIntersects(s.rules, m.start, m.end(), m.epoch) && m.start >= plan.ret1hCutoff
	case resDay:
		return !ruleIntersects(s.rules, m.start, m.end(), m.epoch) && m.start >= plan.ret1dCutoff
	default:
		return false
	}
}
