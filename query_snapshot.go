package strata

import (
	"errors"
	"maps"
	"math/bits"
	"slices"
)

type querySources struct {
	fine  blockSource
	hours blockSource
	days  blockSource
}

type querySnapshot struct {
	from, to int64
	opts     Options
	policy   memPolicy
	head     superblock
	view     *fileView
	series   Series
	dict     dictionary
	sealed   []sealedMeta
	invert   []invertChunkMeta
	fineAll  []uint32
	hourAll  []uint32
	dayAll   []uint32
	liveFine []sealedBlock
}

func (s *querySnapshot) release() {
	if s.view != nil {
		s.view.release()
		s.view = nil
	}
}

func (s *querySnapshot) buildExecDB() *DB {
	execDB := &DB{
		dict:   s.dict,
		opts:   s.opts,
		policy: s.policy,
		head:   s.head,
		view:   s.view,
	}
	if s.view != nil {
		execDB.data = s.view.data
		execDB.mapped = s.view.mapped
	}
	return execDB
}

func dictionarySnapshot(d dictionary, series uint16) dictionary {
	return dictionarySnapshotForQuery(d, series, nil)
}

func dictionarySnapshotForQuery(d dictionary, series uint16, filters []Filter) dictionary {
	var wantSorted uint8
	for _, f := range filters {
		if (f.Op == TagEq || f.Op == TagPrefix) && f.Dim >= 0 && f.Dim < 8 {
			wantSorted |= uint8(1) << uint(f.Dim)
		}
	}
	var out dictionary
	out.snapshot = true
	out.snapshotSeries = series
	for dim := range uint8(8) {
		dimData := d.dims[dictDimKey{series: series, dim: dim}]
		if dimData == nil {
			continue
		}
		var extra map[uint32]string
		if len(dimData.extra) > 0 {
			extra = make(map[uint32]string, len(dimData.extra))
			maps.Copy(extra, dimData.extra)
		}
		var sorted []uint32
		var sortedNextID uint32
		if wantSorted&(uint8(1)<<dim) != 0 && dimData.ext == nil {
			dimData.revMu.RLock()
			sorted, sortedNextID = dimData.sorted, dimData.sortedNextID
			dimData.revMu.RUnlock()
			if sorted == nil && dimData.count > dictSortedMergeThreshold {
				dimData.revMu.Lock()
				if dimData.sorted == nil {
					dimData.buildSorted()
				}
				sorted, sortedNextID = dimData.sorted, dimData.sortedNextID
				dimData.revMu.Unlock()
			}
			if len(sorted) > 0 {
				sorted = sorted[:len(sorted):len(sorted)]
			}
		}
		out.snapshotDims[dim] = dictDimSnap{
			arena:        dimData.arena[:len(dimData.arena):len(dimData.arena)],
			offsets:      dimData.offsets[:len(dimData.offsets):len(dimData.offsets)],
			extra:        extra,
			tailBase:     dimData.tailBase,
			nextID:       dimData.nextID,
			count:        dimData.count,
			ext:          dimData.ext,
			sorted:       sorted,
			sortedNextID: sortedNextID,
		}
	}
	return out
}

func captureQuerySnapshot(db *DB, s *Series, ws windowSpec, filters []Filter) (querySnapshot, int64, bool, error) {
	var snap querySnapshot
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return querySnapshot{}, 0, false, errors.New("strata: database is closed")
	}
	var planFrom int64
	if ws.fixed {
		snap.from, snap.to, planFrom = ws.fixedFrom, ws.fixedTo, ws.fixedPlanFrom
		if snap.to <= snap.from || planFrom >= snap.to {
			db.mu.RUnlock()
			return snap, planFrom, true, nil
		}
	} else {
		var emptyWin bool
		var err error
		snap.from, snap.to, emptyWin, err = resolveTimeWindow(db, s.id, ws.from, ws.to, ws.window, ws.allTime, ws.defaultWindow)
		if err != nil {
			db.mu.RUnlock()
			return querySnapshot{}, 0, false, err
		}
		if emptyWin {
			db.mu.RUnlock()
			return snap, snap.from, true, nil
		}
		planFrom = snap.from
	}

	for _, f := range filters {
		if !f.Negate && (f.Dim < 0 || f.Dim >= s.cfg.Arity) {
			db.mu.RUnlock()
			return snap, planFrom, true, nil
		}
	}

	rules := slices.Clone(s.rules)
	execSeries := *s
	execSeries.rules = rules

	snap.opts = db.opts
	snap.policy = db.policy
	snap.head = db.head
	snap.view = db.view.acquire()
	snap.series = execSeries
	snap.sealed = db.sealed
	snap.invert = db.invert
	snap.fineAll = db.sealedByRes[sealedResKey{series: s.id, res: resMinuteHR}]
	snap.hourAll = db.sealedByRes[sealedResKey{series: s.id, res: resHour}]
	snap.dayAll = db.sealedByRes[sealedResKey{series: s.id, res: resDay}]

	for key, b := range db.live {
		if key.series != s.id || b.ts+60 <= planFrom || b.ts >= snap.to || blockExcludedRules(rules, b.ts, b.ts+60, b.epoch) {
			continue
		}
		snap.liveFine = append(snap.liveFine, sealedBlock{
			series: s.id, res: resMinuteHR, start: b.ts, end: b.ts + 60,
			epoch: b.epoch, order: b.order, summary: b.summary,
			rowCount: uint32(len(b.rows)), rows: cloneRows(b.rows),
			candidate: true,
		})
	}
	snap.dict = dictionarySnapshotForQuery(db.dict, s.id, filters)
	db.mu.RUnlock()

	return snap, planFrom, false, nil
}

func filterLiveCandidates(liveFine []sealedBlock, compiled []compiledFilter) bool {
	hasLive := false
	for i := range liveFine {
		matched := false
		for _, row := range liveFine[i].rows {
			if matchesCompiledRow(row, compiled) {
				matched = true
				break
			}
		}
		liveFine[i].candidate = matched
		if matched {
			hasLive = true
		} else {
			liveFine[i].rows = nil
		}
	}
	return hasLive
}

func buildQuerySourcesFromSnap(snap *querySnapshot, from, to int64, sealedBits *blockBitmap) querySources {
	if len(snap.liveFine) > 1 {
		sortBlocks(snap.liveFine)
	}
	return buildSealedQuerySources(
		snap.sealed, snap.series.id,
		snap.fineAll, snap.hourAll, snap.dayAll,
		snap.series.rules, from, to, sealedBits, snap.liveFine,
	)
}

func buildQuerySourcesLocked(db *DB, s *Series, from, to int64, sealedBits *blockBitmap, liveMask uint64, useLiveMask, needLiveRows bool) querySources {
	rules := slices.Clone(s.rules)
	var liveFine []sealedBlock
	for key, b := range db.live {
		if key.series != s.id || b.ts+60 <= from || b.ts >= to || blockExcludedRules(rules, b.ts, b.ts+60, b.epoch) {
			continue
		}
		cand := true
		if useLiveMask {
			if slot, ok := db.liveSlot[key]; ok {
				cand = (liveMask & (uint64(1) << slot)) != 0
			} else {
				cand = false
			}
		}
		var rows []idRow
		if needLiveRows && cand {
			rows = cloneRows(b.rows)
		}
		liveFine = append(liveFine, sealedBlock{
			series: s.id, res: resMinuteHR, start: b.ts, end: b.ts + 60,
			epoch: b.epoch, order: b.order, summary: b.summary,
			rowCount: uint32(len(b.rows)), rows: rows,
			candidate: cand,
		})
	}
	if len(liveFine) > 1 {
		sortBlocks(liveFine)
	}
	return buildSealedQuerySources(
		db.sealed, s.id,
		db.sealedByRes[sealedResKey{series: s.id, res: resMinuteHR}],
		db.sealedByRes[sealedResKey{series: s.id, res: resHour}],
		db.sealedByRes[sealedResKey{series: s.id, res: resDay}],
		rules, from, to, sealedBits, liveFine,
	)
}

func buildSealedQuerySources(sealed []sealedMeta, seriesID uint16, fineAll, hourAll, dayAll []uint32, rules []resetRule, from, to int64, sealedBits *blockBitmap, liveFine []sealedBlock) querySources {
	fineIdx := sealedRangeIndexes(sealed, fineAll, from, to)
	hourIdx := sealedRangeIndexes(sealed, hourAll, from, to)
	dayIdx := sealedRangeIndexes(sealed, dayAll, from, to)

	var candBits []uint64
	var fineCand, hourCand, dayCand []uint32
	totalIdx := len(fineIdx) + len(hourIdx) + len(dayIdx)
	if sealedBits != nil {
		candBits = sealedBits.bits
		if sealedBits.count*4 < totalIdx {
			fineCand = make([]uint32, 0)
			hourCand = make([]uint32, 0)
			dayCand = make([]uint32, 0)
			for wIdx, word := range candBits {
				for word != 0 {
					tz := bits.TrailingZeros64(word)
					pos := uint32(wIdx<<6 + tz)
					word &= word - 1
					if int(pos) >= len(sealed) {
						break
					}
					b := &sealed[pos]
					if b.series != seriesID || b.end() <= from || b.start >= to {
						continue
					}
					switch b.res {
					case resMinuteHR:
						if !blockExcludedRules(rules, b.start, b.end(), b.epoch) {
							fineCand = append(fineCand, pos)
						}
					case resHour:
						hourCand = append(hourCand, pos)
					case resDay:
						dayCand = append(dayCand, pos)
					}
				}
			}
			slices.Sort(fineCand)
			slices.Sort(hourCand)
			slices.Sort(dayCand)
		}
	}

	return querySources{
		fine: blockSource{
			sealed: sealed, idx: fineIdx, candIdx: fineCand, live: liveFine,
			candBits: candBits, filterFine: true, rules: rules,
		},
		hours: blockSource{
			sealed: sealed, idx: hourIdx, candIdx: hourCand,
			candBits: candBits, rules: rules,
		},
		days: blockSource{
			sealed: sealed, idx: dayIdx, candIdx: dayCand,
			candBits: candBits, rules: rules,
		},
	}
}
