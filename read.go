package strata

import (
	"context"
	"errors"
	"time"
)

const (
	lightReadMaxLimit        = 200
	lightReadMaxOffset       = 200
	lightReadMaxWindow       = 24 * time.Hour
	lightReadMaxFilters      = 2
	lightReadMaxFilterValues = 16
	lightReadMaxPlans        = 64
	lightReadMaxBlockRows    = 4096
	lightReadMaxFilteredRows = 16384
)

func isLightReadCandidate(db *DB, req ReadRequest, defaultWindow time.Duration, limit int, useCursor bool) bool {
	if db.lightReadSem == nil || limit <= 0 || limit > lightReadMaxLimit ||
		req.AllTime || req.Offset > lightReadMaxOffset || len(req.Filters) > lightReadMaxFilters {
		return false
	}
	for _, f := range req.Filters {
		if f.Op != TagEq || len(f.Values) > lightReadMaxFilterValues {
			return false
		}
	}
	if useCursor {
		if req.Cursor.rowSkip > lightReadMaxOffset || req.Cursor.winToSec < req.Cursor.winFromSec {
			return false
		}
		return uint64(req.Cursor.winToSec)-uint64(req.Cursor.winFromSec) <= uint64(lightReadMaxWindow/time.Second)
	}
	if req.Window < 0 {
		return false
	}
	switch {
	case !req.From.IsZero() && !req.To.IsZero():
		return req.Window == 0 && !req.To.Before(req.From) && !req.From.Add(lightReadMaxWindow).Before(req.To)
	case !req.From.IsZero():
		if req.Window != 0 {
			return false
		}
		now := db.opts.Now().UTC()
		return !now.Before(req.From) && !req.From.Add(lightReadMaxWindow).Before(now)
	case !req.To.IsZero():
		return req.Window > 0 && req.Window <= lightReadMaxWindow
	case req.Window > 0:
		return req.Window <= lightReadMaxWindow
	default:
		return defaultWindow > 0 && defaultWindow <= lightReadMaxWindow
	}
}

func fitsLightReadPlan(pq *preparedQuery, startPlanIdx, remainingSkip, limit int, policy memPolicy) bool {
	if len(pq.plans)-startPlanIdx > lightReadMaxPlans || pq.execSeries.cfg.Arity > 8 {
		return false
	}
	maxBlockRows := min(lightReadMaxBlockRows, int(max(policy.queryBytes/512, 512)))
	if len(pq.compiled) == 0 {
		need := remainingSkip + limit + 1
		seen := 0
		for pi := startPlanIdx; pi < len(pq.plans); pi++ {
			sumRows, minProduced, ok := slotCandidateRowBounds(pq.plans[pi], pq.sources, &pq.execSeries)
			if !ok || sumRows > maxBlockRows {
				return false
			}
			seen += minProduced
			if seen >= need {
				break
			}
		}
		return true
	}
	upper, ok := estimateCandidateRowUpperBound(pq.plans[startPlanIdx:], pq.sources, &pq.execSeries)
	maxFiltered := min(lightReadMaxFilteredRows, int(max(policy.queryBytes/128, 1024)))
	if !ok || upper > maxFiltered {
		return false
	}
	for pi := startPlanIdx; pi < len(pq.plans); pi++ {
		sumRows, _, slotOK := slotCandidateRowBounds(pq.plans[pi], pq.sources, &pq.execSeries)
		if !slotOK || sumRows > maxBlockRows {
			return false
		}
	}
	return true
}

func (s *Series) Read(ctx context.Context, req ReadRequest) (*PageResult, error) {
	return s.readWithOptions(ctx, req, 0, DefaultPageLimit, MaxPageLimit)
}

func (s *Series) readWithOptions(ctx context.Context, req ReadRequest, defaultWindow time.Duration, defaultLimit, maxLimit int) (*PageResult, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("strata: nil series")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if defaultLimit <= 0 {
		defaultLimit = DefaultPageLimit
	}
	if maxLimit <= 0 {
		maxLimit = MaxPageLimit
	}
	limit := req.Limit
	if limit == 0 {
		limit = defaultLimit
	}
	if limit < 0 || limit > maxLimit {
		return nil, ErrPageLimitExceeded
	}
	if req.Offset < 0 {
		return nil, ErrInvalidCursor
	}
	qHash := cursorQueryHash(s.id, req.Filters, req.PreferFine, req.RequireExact)
	useCursor := !req.Cursor.IsZero()
	if useCursor && (req.Cursor.queryHash != qHash || req.Offset != 0) {
		return nil, ErrInvalidCursor
	}

	db := s.db
	var slot *querySlot
	var release func()
	var err error
	inLightLane := isLightReadCandidate(db, req, defaultWindow, limit, useCursor)
	if inLightLane {
		slot, release, err = db.acquireLightRead(ctx)
	} else {
		_, _, slot, release, err = db.acquireQuery(ctx)
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if release != nil {
			release()
		}
	}()

	ws := windowSpec{
		from: req.From, to: req.To, window: req.Window,
		allTime: req.AllTime, defaultWindow: defaultWindow,
	}
	if useCursor {
		winFrom := req.Cursor.winFromSec
		winTo := req.Cursor.winToSec
		planFrom := max(winFrom, req.Cursor.slotStart)
		ws = windowSpec{fixed: true, fixedFrom: winFrom, fixedTo: winTo, fixedPlanFrom: planFrom}
	}
	pq, err := prepareQuery(db, s, ws, req.Filters, queryOptions{
		RequireExact: req.RequireExact, PreferFine: req.PreferFine,
	}, slot)
	if err != nil {
		return nil, err
	}
	defer pq.release()

	winFromSec, winToSec := pq.from, pq.to
	if useCursor {
		winFromSec, winToSec = req.Cursor.winFromSec, req.Cursor.winToSec
	}
	page := &PageResult{
		RequestedFrom: time.Unix(winFromSec, 0).UTC(),
		RequestedTo:   time.Unix(winToSec, 0).UTC(),
		Exact:         true,
	}
	if pq.empty || len(pq.plans) == 0 {
		if !pq.empty {
			page.Exact = pq.qres.Exact
		}
		return page, nil
	}

	startPlanIdx := 0
	remainingSkip := req.Offset
	if useCursor {
		first := pq.plans[0]
		switch {
		case first.block.start == req.Cursor.slotStart:
			if first.block.res == req.Cursor.slotRes {
				remainingSkip = int(req.Cursor.rowSkip)
			} else {
				page.ResolutionShifted = true
				page.Exact = false
				remainingSkip = 0
			}
		case first.block.start < req.Cursor.slotStart:
			if req.RequireExact {
				return nil, ErrResolutionTooCoarse
			}
			page.ResolutionShifted = true
			page.Exact = false
			startPlanIdx = 1
			remainingSkip = 0
		default:
			remainingSkip = 0
		}
	}

	if inLightLane && !fitsLightReadPlan(&pq, startPlanIdx, remainingSkip, limit, db.policy) {
		release()
		release = nil
		_, _, slot, release, err = db.acquireQuery(ctx)
		if err != nil {
			return nil, err
		}
	}

	arity := pq.execSeries.cfg.Arity
	page.Rows = getPageRowSlice(limit)[:0]
	page.tagBuf = getTagSlice(limit * arity)[:0]
	allowDim0BS := !opsHasLast(pq.execSeries.cfg.RowOps)
	var rowIdxBuf []int32

	for pi := startPlanIdx; pi < len(pq.plans); pi++ {
		if err := ctx.Err(); err != nil {
			page.Release()
			return nil, err
		}
		plan := pq.plans[pi]

		if len(page.Rows) == limit {
			if len(pq.compiled) == 0 && plan.block.rowCount > 0 {
				page.HasMore = true
				page.NextCursor = makeNextCursor(winFromSec, winToSec, qHash, plan.block, 0)
				break
			}
			hasMatch, probeErr := probeSlotHasMatch(pq.exec.view, plan.block, pq.sources, &pq.execSeries, pq.compiled, allowDim0BS, slot)
			if probeErr != nil {
				page.Release()
				return nil, probeErr
			}
			if hasMatch {
				page.HasMore = true
				page.NextCursor = makeNextCursor(winFromSec, winToSec, qHash, plan.block, 0)
				break
			}
			continue
		}

		if len(pq.compiled) == 0 && remainingSkip > 0 && plan.block.rowCount > 0 &&
			pq.sources.countSlotCandidates(plan.block, pq.execSeries.rules) == 1 &&
			remainingSkip >= int(plan.block.rowCount) {
			remainingSkip -= int(plan.block.rowCount)
			continue
		}

		block, bufPtr, readErr := mergeSlot(pq.exec.view, plan.block, pq.sources, &pq.execSeries)
		if readErr != nil {
			putReadBuf(bufPtr)
			page.Release()
			return nil, readErr
		}
		matchCount, taken, takeStart, exErr := readPageFromBlock(block, plan.meta, pq.exec.dict, s.id, arity, pq.compiled, allowDim0BS, remainingSkip, limit-len(page.Rows), slot, &rowIdxBuf, page)
		putReadBuf(bufPtr)
		if exErr != nil {
			page.Release()
			return nil, exErr
		}
		if matchCount <= remainingSkip {
			remainingSkip -= matchCount
			continue
		}
		remainingSkip = 0
		if taken > 0 && !plan.meta.Exact {
			page.Exact = false
		}
		if len(page.Rows) == limit && takeStart+taken < matchCount {
			page.HasMore = true
			page.NextCursor = makeNextCursor(winFromSec, winToSec, qHash, plan.block, uint32(takeStart+taken))
			break
		}
	}
	if len(page.Rows) == 0 {
		putPageRowSlice(page.Rows)
		putTagSlice(page.tagBuf)
		page.Rows, page.tagBuf = nil, nil
	}
	return page, nil
}

func makeNextCursor(winFromSec, winToSec int64, qHash uint64, b sealedMeta, rowSkip uint32) Cursor {
	return Cursor{
		winFromSec: winFromSec, winToSec: winToSec, queryHash: qHash,
		slotStart: b.start, slotRes: b.res, rowSkip: rowSkip, valid: true,
	}
}

func probeSlotHasMatch(view *fileView, chosen sealedMeta, sources querySources, s *Series, filters []compiledFilter, allowDim0BS bool, slot *querySlot) (bool, error) {
	block, bufPtr, err := mergeSlot(view, chosen, sources, s)
	if bufPtr != nil {
		defer putReadBuf(bufPtr)
	}
	if err != nil {
		return false, err
	}
	if block.encoded != nil {
		var sel rowSelection
		if isStreamBlock(block.encoded) {
			view, err := parseColumnarBlock(block.encoded, block.codecID)
			if err != nil {
				return false, err
			}
			sel, err = filterStreamView(view, filters, allowDim0BS, slot)
			if err != nil {
				return false, err
			}
		} else {
			count, cols, err := decodeSmallBlockCols(block.encoded, block.codecID, slot)
			if err != nil {
				return false, err
			}
			sel, err = filterSmallCols(count, &cols, filters, allowDim0BS)
			if err != nil {
				return false, err
			}
		}
		has := sel.count() > 0
		sel.release()
		return has, nil
	}
	for _, row := range block.rows {
		if filters == nil || matchesCompiledRow(row, filters) {
			return true, nil
		}
	}
	return false, nil
}

func readPageFromBlock(block sealedBlock, meta SegmentMeta, d dictionary, series uint16, arity int, filters []compiledFilter, allowDim0BS bool, skip, need int, slot *querySlot, rowIdxBuf *[]int32, page *PageResult) (int, int, int, error) {
	if block.encoded != nil {
		isStream := isStreamBlock(block.encoded)
		var view columnarBlockView
		var cols [streamColumnCount]decodedCol
		var sel rowSelection
		var err error
		if isStream {
			view, err = parseColumnarBlock(block.encoded, block.codecID)
			if err == nil {
				sel, err = filterStreamView(view, filters, allowDim0BS, slot)
			}
		} else {
			var count int
			count, cols, err = decodeSmallBlockCols(block.encoded, block.codecID, slot)
			if err == nil {
				sel, err = filterSmallCols(count, &cols, filters, allowDim0BS)
			}
		}
		if err != nil {
			return 0, 0, 0, err
		}
		defer sel.release()
		total := sel.count()
		if total <= skip || need <= 0 {
			return total, 0, skip, nil
		}
		take := min(need, total-skip)
		isRange, rangeLo, rows := sel.sliceRows(skip, take, *rowIdxBuf)
		if sel.mode == selBits && cap(rows) > cap(*rowIdxBuf) {
			*rowIdxBuf = rows[:0]
		}
		if isStream {
			if err := appendPageFromStream(view, meta, d, series, arity, isRange, rangeLo, rows, take, slot, page); err != nil {
				return 0, 0, 0, err
			}
		} else {
			if err := appendPageFromCols(&cols, meta, d, series, arity, isRange, rangeLo, rows, take, page); err != nil {
				return 0, 0, 0, err
			}
		}
		return total, take, skip, nil
	}
	matched, taken := 0, 0
	for _, row := range block.rows {
		if filters != nil && !matchesCompiledRow(row, filters) {
			continue
		}
		if matched >= skip && taken < need {
			tagStart := len(page.tagBuf)
			page.tagBuf = page.tagBuf[:tagStart+arity]
			tags := page.tagBuf[tagStart : tagStart+arity]
			for dim := range arity {
				tag, err := d.resolve(series, uint8(dim), row.tags[dim])
				if err != nil {
					return 0, 0, 0, err
				}
				tags[dim] = tag
			}
			page.Rows = append(page.Rows, PageRow{
				From: meta.CoveredFrom, Resolution: meta.Resolution, Exact: meta.Exact,
				Tags: tags, Values: row.values, Mask: row.initMask,
			})
			taken++
		}
		matched++
	}
	return matched, taken, skip, nil
}

// Release clears internal slice references and returns bounded working buffers to internal pools.
// It is nil-safe and idempotent.
func (r *PageResult) Release() {
	if r == nil || r.released {
		return
	}
	r.released = true
	putPageRowSlice(r.Rows)
	r.Rows = nil
	putTagSlice(r.tagBuf)
	r.tagBuf = nil
}
