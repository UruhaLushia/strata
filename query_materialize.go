package strata

import "errors"

func mergeSlot(db *DB, chosen sealedMeta, sources querySources, s *Series) (sealedBlock, *[]byte, error) {
	var source *blockSource
	switch chosen.res {
	case resMinuteHR:
		source = &sources.fine
	case resHour:
		source = &sources.hours
	case resDay:
		source = &sources.days
	default:
		return sealedBlock{}, nil, errors.New("strata: selected block disappeared")
	}
	type slotCand struct {
		sealed sealedMeta
		live   *sealedBlock
	}
	var stack [4]slotCand
	cands := stack[:0]
	source.eachSlotCandidate(chosen.start, chosen.end(), chosen.res, s.rules, func(m sealedMeta) {
		cands = append(cands, slotCand{sealed: m})
	}, func(lb *sealedBlock) {
		cands = append(cands, slotCand{live: lb})
	})
	if len(cands) == 0 {
		return sealedBlock{}, nil, errors.New("strata: selected block disappeared")
	}
	if len(cands) == 1 {
		first := cands[0]
		if first.live != nil {
			return *first.live, nil, nil
		}
		if db.view == nil {
			return sealedBlock{}, nil, errors.New("strata: missing file view")
		}
		raw, bufPtr, err := db.view.readRawPooled(first.sealed.offset, first.sealed.length, first.sealed.crc)
		if err != nil {
			return sealedBlock{}, nil, err
		}
		loaded := first.sealed.toBlock()
		loaded.encoded = raw
		loaded.codecID = db.view.blocks.codec
		return loaded, bufPtr, nil
	}
	var merged sealedBlock
	var mergeOrder uint64
	for _, cand := range cands {
		var b sealedBlock
		var err error
		if cand.live != nil {
			b = *cand.live
			b.rows = cloneRows(b.rows)
		} else {
			b, err = loadSealedBlockFromView(db.view, db.head.sections[secTSBlocks], cand.sealed)
			if err != nil {
				return sealedBlock{}, nil, err
			}
		}
		if merged.end == 0 {
			merged = b
			mergeOrder = 1
			merged.order = mergeOrder
			continue
		}
		mergeOrder++
		mergeSummary(&merged.summary, b.summary, s.cfg.SummaryOps, mergeOrder, mergeOrder-1)
		mergeIDRows(&merged.rows, b.rows, s.cfg.RowOps, mergeOrder, mergeOrder-1)
		merged.order = mergeOrder
	}
	if merged.end == 0 {
		return sealedBlock{}, nil, errors.New("strata: selected block disappeared")
	}
	return merged, nil, nil
}

func metaCutByRule(b sealedMeta, s *Series) bool {
	end := b.end()
	for _, r := range s.rules {
		if b.epoch <= r.oldEpoch && r.to > b.start && r.from < end {
			return true
		}
	}
	return false
}
