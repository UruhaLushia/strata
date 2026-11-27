package strata

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"
)

// ScanBatch is a zero-allocation read-only batch view valid only during a single Scan callback.
type ScanBatch struct {
	v *scanBatchView
}

type scanBatchView struct {
	meta    SegmentMeta
	summary Summary
	dict    *dictionary
	tagErr  *atomic.Pointer[error]
	series  uint16
	arity   uint8
	count   int
	tagIDs  [8][]uint32
	values  [4][]int64
	masks   []uint8
}

func (b ScanBatch) Meta() SegmentMeta {
	if b.v == nil {
		return SegmentMeta{}
	}
	return b.v.meta
}

func (b ScanBatch) BlockSummary() Summary {
	if b.v == nil {
		return Summary{}
	}
	return b.v.summary
}

func (b ScanBatch) Len() int {
	if b.v == nil {
		return 0
	}
	return b.v.count
}

func (b ScanBatch) TagID(i, dim int) uint32 {
	if b.v == nil || i < 0 || i >= b.v.count || dim < 0 || dim >= int(b.v.arity) {
		return 0
	}
	return b.v.tagIDs[dim][i]
}

func (b ScanBatch) Tag(i, dim int) string {
	id := b.TagID(i, dim)
	if id == 0 {
		return ""
	}
	tag, err := b.v.dict.resolve(b.v.series, uint8(dim), id)
	if err != nil && b.v.tagErr != nil {
		b.v.tagErr.CompareAndSwap(nil, &err)
	}
	return tag
}

func (b ScanBatch) Values(i int) [4]int64 {
	if b.v == nil || i < 0 || i >= b.v.count {
		return [4]int64{}
	}
	return [4]int64{b.v.values[0][i], b.v.values[1][i], b.v.values[2][i], b.v.values[3][i]}
}

func (b ScanBatch) Mask(i int) uint8 {
	if b.v == nil || i < 0 || i >= b.v.count {
		return 0
	}
	return b.v.masks[i]
}

func (b ScanBatch) CloneRow(i int) Row {
	if b.v == nil || i < 0 || i >= b.v.count {
		return Row{}
	}
	tags := make([]string, int(b.v.arity))
	for dim := range tags {
		tags[dim] = strings.Clone(b.Tag(i, dim))
	}
	return Row{Tags: tags, Values: b.Values(i), Mask: b.Mask(i)}
}

func (s *Series) Scan(ctx context.Context, q ScanQuery, fn func(ScanBatch) error) error {
	return s.scanWithDefaultWindow(ctx, q, 0, fn)
}

func (s *Series) scanWithDefaultWindow(ctx context.Context, q ScanQuery, defaultWindow time.Duration, fn func(ScanBatch) error) error {
	if s == nil || s.db == nil {
		return errors.New("strata: nil series")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db := s.db
	_, defaultBatch, slot, release, err := db.acquireQuery(ctx)
	if err != nil {
		return err
	}
	defer release()

	batchSize := q.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatch
	}
	batchSize = clampInt(batchSize, 1, 4096)

	pq, err := prepareQuery(db, s, windowSpec{
		from: q.From, to: q.To, window: q.Window, allTime: q.AllTime, defaultWindow: defaultWindow,
	}, q.Filters, queryOptions{RequireExact: q.RequireExact, PreferFine: q.PreferFine}, slot)
	if err != nil {
		return err
	}
	defer pq.release()
	if pq.empty || fn == nil {
		return nil
	}

	arity := pq.execSeries.cfg.Arity
	dimMask := q.ProjectDims
	if dimMask == 0 {
		dimMask = uint8((1 << arity) - 1)
	} else {
		dimMask &= uint8((1 << arity) - 1)
	}
	metricMask := q.ProjectMetrics
	if metricMask == 0 {
		metricMask = 0x0f
	} else {
		metricMask &= 0x0f
	}

	allowDim0BS := !opsHasLast(pq.execSeries.cfg.RowOps)
	var rowIdxBuf []int32
	for _, plan := range pq.plans {
		if err := ctx.Err(); err != nil {
			return err
		}
		block, bufPtr, err := mergeSlot(pq.exec.view, plan.block, pq.sources, &pq.execSeries)
		if err != nil {
			putReadBuf(bufPtr)
			return err
		}
		err = scanSlotBlock(ctx, block, plan.meta, &pq.exec, s.id, arity, dimMask, metricMask, batchSize, allowDim0BS, pq.compiled, slot, &rowIdxBuf, fn)
		putReadBuf(bufPtr)
		if err != nil {
			if errors.Is(err, ErrStopScan) {
				return nil
			}
			return err
		}
	}
	return nil
}

func scanSlotBlock(ctx context.Context, block sealedBlock, meta SegmentMeta, db *queryExec, series uint16, arity int, dimMask, metricMask uint8, batchSize int, allowDim0BS bool, filters []compiledFilter, slot *querySlot, rowIdxBuf *[]int32, fn func(ScanBatch) error) error {
	if block.encoded != nil {
		if isStreamBlock(block.encoded) {
			view, err := parseColumnarBlock(block.encoded, block.codecID)
			if err != nil {
				return err
			}
			return scanStreamBlock(ctx, view, block.summary, meta, db, series, arity, dimMask, metricMask, batchSize, allowDim0BS, filters, slot, rowIdxBuf, fn)
		}
		count, cols, err := decodeSmallBlockCols(block.encoded, block.codecID, slot)
		if err != nil {
			return err
		}
		return scanDecodedColsBlock(ctx, count, &cols, block.summary, meta, &db.dict, series, arity, dimMask, metricMask, batchSize, allowDim0BS, filters, slot, rowIdxBuf, fn)
	}
	return scanIDRowsBlock(ctx, block.rows, block.summary, meta, &db.dict, series, arity, dimMask, metricMask, batchSize, filters, slot, fn)
}

func scanStreamBlock(ctx context.Context, view columnarBlockView, summary Summary, meta SegmentMeta, db *queryExec, series uint16, arity int, dimMask, metricMask uint8, batchSize int, allowDim0BS bool, filters []compiledFilter, slot *querySlot, rowIdxBuf *[]int32, fn func(ScanBatch) error) error {
	colMask := uint16(dimMask) | (uint16(metricMask) << 8) | (uint16(1) << 12)
	arenaBudget := clampInt(int(db.policy.queryBytes/8), 128<<10, 1<<20)
	cache := newBlockColCache(view, colMask, filterColMask(filters), arenaBudget, slot)
	sel, err := cache.filter(filters, allowDim0BS)
	if err != nil {
		return err
	}
	defer sel.release()
	total := sel.count()
	if total == 0 {
		return nil
	}
	constTags, constMask := extractScanConstTags(filters, arity)
	constMask &= dimMask
	colMask &^= uint16(constMask)
	cursor := rowSelCursor{sel: sel}
	if !cache.fullBlock && total <= batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		isRange, rangeLo, rows := cursor.next(total, rowIdxBuf)
		if err := fillBatchFromStreamCache(&cache, isRange, rangeLo, rows, total, arity, dimMask&^constMask, metricMask, slot); err != nil {
			return err
		}
		for dim := range arity {
			if constMask&(uint8(1)<<dim) != 0 {
				decodeTagRange(slot.tagIDs[dim][:total], decodedCol{mode: modeConst, val: uint64(constTags[dim])}, 0)
			}
		}
		return yieldScanBatch(fn, makeScanBatch(meta, summary, &db.dict, series, arity, total, slot))
	}
	cache.prepareOutput(colMask, total, sel.mode == selRange)
	if cache.fullBlock {
		if err := cache.loadAll(colMask); err != nil {
			return err
		}
		cols := cache.cols
		setScanConstCols(&cols, constTags, constMask)
		for start := 0; start < total; start += batchSize {
			if err := ctx.Err(); err != nil {
				return err
			}
			n := min(batchSize, total-start)
			isRange, rangeLo, rows := cursor.next(n, rowIdxBuf)
			fillBatchFromCols(&cols, isRange, rangeLo, rows, n, arity, dimMask, metricMask, slot)
			if err := yieldScanBatch(fn, makeScanBatch(meta, summary, &db.dict, series, arity, n, slot)); err != nil {
				return err
			}
		}
		return nil
	}
	rowBytes := cache.packRowBytes(colMask, sel.mode == selRange)
	avail := cache.availPackBytes()
	chunkRows := total
	if rowBytes > 0 && total*rowBytes > avail {
		chunkRows = max(avail/rowBytes, 1)
		if chunkRows >= batchSize {
			chunkRows = (chunkRows / batchSize) * batchSize
		}
	}
	for start := 0; start < total; start += chunkRows {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(chunkRows, total-start)
		isRange, rangeLo, rows := cursor.next(n, rowIdxBuf)
		cols, err := cache.packChunk(colMask, isRange, rangeLo, rows, n)
		if err != nil {
			return err
		}
		setScanConstCols(&cols, constTags, constMask)
		for off := 0; off < n; off += batchSize {
			if err := ctx.Err(); err != nil {
				return err
			}
			cnt := min(batchSize, n-off)
			fillBatchFromCols(&cols, true, off, nil, cnt, arity, dimMask, metricMask, slot)
			if err := yieldScanBatch(fn, makeScanBatch(meta, summary, &db.dict, series, arity, cnt, slot)); err != nil {
				return err
			}
		}
	}
	return nil
}

func scanDecodedColsBlock(ctx context.Context, count int, cols *[streamColumnCount]decodedCol, summary Summary, meta SegmentMeta, d *dictionary, series uint16, arity int, dimMask, metricMask uint8, batchSize int, allowDim0BS bool, filters []compiledFilter, slot *querySlot, rowIdxBuf *[]int32, fn func(ScanBatch) error) error {
	sel, err := filterSmallCols(count, cols, filters, allowDim0BS)
	if err != nil {
		return err
	}
	defer sel.release()
	total := sel.count()
	if total == 0 {
		return nil
	}
	constTags, constMask := extractScanConstTags(filters, arity)
	setScanConstCols(cols, constTags, dimMask&constMask)
	cursor := rowSelCursor{sel: sel}
	for start := 0; start < total; start += batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(batchSize, total-start)
		isRange, rangeLo, rows := cursor.next(n, rowIdxBuf)
		fillBatchFromCols(cols, isRange, rangeLo, rows, n, arity, dimMask, metricMask, slot)
		if err := yieldScanBatch(fn, makeScanBatch(meta, summary, d, series, arity, n, slot)); err != nil {
			return err
		}
	}
	return nil
}

func scanIDRowsBlock(ctx context.Context, rows []idRow, summary Summary, meta SegmentMeta, d *dictionary, series uint16, arity int, dimMask, metricMask uint8, batchSize int, filters []compiledFilter, slot *querySlot, fn func(ScanBatch) error) error {
	if len(rows) == 0 {
		return nil
	}
	slot.ensureBatch(batchSize, arity)
	pos := 0
	flush := func() error {
		if pos == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		err := yieldScanBatch(fn, makeScanBatch(meta, summary, d, series, arity, pos, slot))
		pos = 0
		return err
	}
	for _, row := range rows {
		if filters != nil && !matchesCompiledRow(row, filters) {
			continue
		}
		for dim := range arity {
			if dimMask&(uint8(1)<<dim) != 0 {
				slot.tagIDs[dim][pos] = row.tags[dim]
			} else {
				slot.tagIDs[dim][pos] = 0
			}
		}
		for m := range 4 {
			if metricMask&(uint8(1)<<m) != 0 {
				slot.values[m][pos] = row.values[m]
			} else {
				slot.values[m][pos] = 0
			}
		}
		slot.masks[pos] = row.initMask
		pos++
		if pos == batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func yieldScanBatch(fn func(ScanBatch) error, b ScanBatch) error {
	err := fn(b)
	if tagErr := b.v.tagErr.Swap(nil); tagErr != nil {
		return *tagErr
	}
	return err
}

func makeScanBatch(meta SegmentMeta, summary Summary, d *dictionary, series uint16, arity, count int, slot *querySlot) ScanBatch {
	var tagIDs [8][]uint32
	for dim := range arity {
		tagIDs[dim] = slot.tagIDs[dim][:count]
	}
	slot.batch = scanBatchView{
		meta:    meta,
		summary: summary,
		dict:    d,
		tagErr:  &slot.tagErr,
		series:  series,
		arity:   uint8(arity),
		count:   count,
		tagIDs:  tagIDs,
		values:  [4][]int64{slot.values[0][:count], slot.values[1][:count], slot.values[2][:count], slot.values[3][:count]},
		masks:   slot.masks[:count],
	}
	return ScanBatch{v: &slot.batch}
}
