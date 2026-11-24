package strata

import (
	"maps"
	"math/bits"
	"slices"
)

type querySources struct {
	fine  blockSource
	hours blockSource
	days  blockSource
}

func dictionarySnapshot(d dictionary, series uint16) dictionary {
	var out dictionary
	out.snapshot = true
	out.snapshotSeries = series
	for dim := range uint8(8) {
		if dimData := d.dims[dictDimKey{series: series, dim: dim}]; dimData != nil {
			var extra map[uint32]string
			if len(dimData.extra) > 0 {
				extra = make(map[uint32]string, len(dimData.extra))
				maps.Copy(extra, dimData.extra)
			}
			out.snapshotDims[dim] = dictDimSnap{
				arena:    dimData.arena[:len(dimData.arena):len(dimData.arena)],
				offsets:  dimData.offsets[:len(dimData.offsets):len(dimData.offsets)],
				extra:    extra,
				tailBase: dimData.tailBase,
				nextID:   dimData.nextID,
				ext:      dimData.ext,
			}
		}
	}
	return out
}

func buildQuerySourcesLocked(db *DB, s *Series, from, to int64, sealedBits *blockBitmap, liveMask uint64, useLiveMask, needLiveRows bool) querySources {
	var rules []resetRule
	if len(s.rules) > 0 {
		rules = append([]resetRule(nil), s.rules...)
	}
	sealed := db.sealed
	fineIdx := sealedRangeIndexes(sealed, db.sealedByRes[sealedResKey{series: s.id, res: resMinuteHR}], from, to)
	hourIdx := sealedRangeIndexes(sealed, db.sealedByRes[sealedResKey{series: s.id, res: resHour}], from, to)
	dayIdx := sealedRangeIndexes(sealed, db.sealedByRes[sealedResKey{series: s.id, res: resDay}], from, to)

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
					if b.series != s.id || b.end() <= from || b.start >= to {
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
