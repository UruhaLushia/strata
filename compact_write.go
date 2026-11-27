package strata

import (
	"encoding/binary"
	"hash"
	"hash/crc32"
	"sort"
	"sync"

	"github.com/UruhaLushia/strata/codec"
)

type compactBlockWriter struct {
	tf              *osFile
	source          *osFile
	sourceBlocks    sectionDesc
	pos             int64
	blockStart      int64
	blockHash       hash.Hash32
	passBuf         []byte
	metas           []sealedMeta
	passOldToWrite  []int32
	passSeriesCount map[uint16]uint32
	inv             *invertExternalBuilder
	mainEnc         codec.Encoder
	workerEncs      []codec.Encoder
	policy          memPolicy
	plan            *compactPlan
}

func newCompactBlockWriter(tf *osFile, pos int64, plan *compactPlan, inv *invertExternalBuilder, mainEnc codec.Encoder) (*compactBlockWriter, error) {
	passOld := make([]int32, len(plan.oldSealed))
	for i := range passOld {
		passOld[i] = -1
	}
	w := &compactBlockWriter{
		tf:              tf,
		source:          plan.sourceView.data,
		sourceBlocks:    plan.oldHead.sections[secTSBlocks],
		pos:             pos,
		blockStart:      pos,
		blockHash:       crc32.New(castagnoli),
		passBuf:         make([]byte, 64<<10),
		metas:           make([]sealedMeta, 0, len(plan.oldSealed)),
		passOldToWrite:  passOld,
		passSeriesCount: make(map[uint16]uint32, len(plan.series)),
		inv:             inv,
		mainEnc:         mainEnc,
		policy:          plan.policy,
		plan:            plan,
	}
	if workers := plan.policy.compactWorkers; workers > 1 {
		w.workerEncs = make([]codec.Encoder, workers)
		w.workerEncs[0] = mainEnc
		for i := 1; i < workers; i++ {
			enc, err := plan.codecs.newEncoder(plan.policy.encoderWindow)
			if err != nil {
				w.close()
				return nil, err
			}
			w.workerEncs[i] = enc
		}
	}
	return w, nil
}

func (w *compactBlockWriter) close() {
	for i := 1; i < len(w.workerEncs); i++ {
		if w.workerEncs[i] != nil {
			w.workerEncs[i].Close()
			w.workerEncs[i] = nil
		}
	}
}

func (w *compactBlockWriter) writePassthrough(oldIdx int, meta sealedMeta) error {
	if w.sourceBlocks.codec != w.plan.codecs.target {
		b, err := loadSealedBlockFromView(w.plan.sourceView, w.sourceBlocks, meta)
		if err != nil {
			return err
		}
		b.preserveOrder = opsHasLast(w.plan.series[meta.series].cfg.RowOps)
		return w.writeFresh(b)
	}
	writeID := uint32(len(w.metas))
	if err := copyPassthroughBlock(w.tf, w.source, w.sourceBlocks, meta, w.pos, w.blockHash, w.passBuf); err != nil {
		return err
	}
	meta.offset = uint64(w.pos)
	w.pos += int64(meta.length)
	w.metas = append(w.metas, meta)
	w.passOldToWrite[oldIdx] = int32(writeID)
	w.passSeriesCount[meta.series]++
	return nil
}

func (w *compactBlockWriter) commitFresh(b sealedBlock, raw []byte) error {
	writeID := uint32(len(w.metas))
	blockCRC := crc(raw)
	if _, err := w.tf.WriteAt(raw, w.pos); err != nil {
		return err
	}
	_, _ = w.blockHash.Write(raw)
	rowCount := b.rowCount
	if rowCount == 0 {
		rowCount = uint32(len(b.rows))
	}
	meta := sealedMeta{
		start: b.start, epoch: b.epoch, order: b.order,
		offset: uint64(w.pos), values: b.summary.Values,
		length: uint32(len(raw)), crc: blockCRC, rowCount: rowCount,
		series: b.series, res: b.res, mask: normalizeMask(b.summary.Mask),
	}
	w.pos += int64(len(raw))
	w.metas = append(w.metas, meta)
	return w.inv.addBlock(b.series, writeID, b.rows)
}

func (w *compactBlockWriter) writeFresh(b sealedBlock) error {
	raw, err := encodeBlockWithEncoder(b, w.mainEnc)
	if err != nil {
		return err
	}
	return w.commitFresh(b, raw)
}

func (w *compactBlockWriter) writeFreshBatch(batch []sealedBlock) error {
	if len(batch) == 0 {
		return nil
	}
	if len(batch) == 1 || len(w.workerEncs) <= 1 {
		for i := range batch {
			if err := w.writeFresh(batch[i]); err != nil {
				return err
			}
			batch[i].rows = nil
		}
		return nil
	}
	batchBudget := max(w.policy.compactBytes/2, 1<<20)
	for start := 0; start < len(batch); {
		end := start
		var estBytes int64
		for end < len(batch) && end-start < 64 {
			est := int64(len(batch[end].rows))*73 + 256
			if end > start && estBytes+est > batchBudget {
				break
			}
			estBytes += est
			end++
		}
		n := end - start
		encoded := make([][]byte, n)
		var encodeErr error
		var mu sync.Mutex
		jobs := make(chan int)
		var wg sync.WaitGroup
		active := min(len(w.workerEncs), n)
		for wi := range active {
			wg.Add(1)
			go func(enc codec.Encoder) {
				defer wg.Done()
				for idx := range jobs {
					raw, err := encodeBlockWithEncoder(batch[idx], enc)
					if err != nil {
						mu.Lock()
						if encodeErr == nil {
							encodeErr = err
						}
						mu.Unlock()
						continue
					}
					encoded[idx-start] = raw
				}
			}(w.workerEncs[wi])
		}
		for i := start; i < end; i++ {
			jobs <- i
		}
		close(jobs)
		wg.Wait()
		if encodeErr != nil {
			return encodeErr
		}
		for i := start; i < end; i++ {
			if err := w.commitFresh(batch[i], encoded[i-start]); err != nil {
				return err
			}
			encoded[i-start] = nil
			batch[i].rows = nil
		}
		start = end
	}
	return nil
}

func (w *compactBlockWriter) finalizeIndex() ([]byte, []uint32, map[uint16]uint32) {
	n := len(w.metas)
	if n == 0 {
		return nil, nil, nil
	}
	order := make([]uint32, n)
	for i := range order {
		order[i] = uint32(i)
	}
	sort.Slice(order, func(i, j int) bool {
		return metaBefore(w.metas[order[i]], w.metas[order[j]])
	})
	writeToFinal := make([]uint32, n)
	seriesBlocks := make(map[uint16]uint32, len(w.passSeriesCount)+4)
	idx := make([]byte, n*tsIndexEntryBytes)
	for finalID, writeID := range order {
		writeToFinal[writeID] = uint32(finalID)
		m := w.metas[writeID]
		seriesBlocks[m.series]++
		entry := idx[finalID*tsIndexEntryBytes : (finalID+1)*tsIndexEntryBytes]
		binary.LittleEndian.PutUint16(entry[0:2], m.series)
		entry[2] = m.res
		entry[3] = normalizeMask(m.mask)
		binary.LittleEndian.PutUint32(entry[4:8], m.length)
		binary.LittleEndian.PutUint64(entry[8:16], uint64(m.start))
		binary.LittleEndian.PutUint64(entry[16:24], m.epoch)
		binary.LittleEndian.PutUint64(entry[24:32], m.offset)
		binary.LittleEndian.PutUint64(entry[32:40], m.order)
		binary.LittleEndian.PutUint32(entry[40:44], m.crc)
		binary.LittleEndian.PutUint32(entry[44:48], m.rowCount)
		for j, value := range m.values {
			binary.LittleEndian.PutUint64(entry[48+j*8:56+j*8], uint64(value))
		}
	}
	return idx, writeToFinal, seriesBlocks
}

func writeCompactFile(tf *osFile, plan *compactPlan) (int64, [12]sectionDesc, dictLiveBits, map[dictDimKey]bool, error) {
	var ds [12]sectionDesc
	target := plan.codecs.target
	mainEnc, err := plan.codecs.newEncoder(plan.policy.encoderWindow)
	if err != nil {
		return 0, ds, nil, nil, err
	}
	if mainEnc != nil {
		defer mainEnc.Close()
	}

	var pos int64
	var kvDesc [3]sectionDesc
	var blobDesc sectionDesc
	if plan.kvPassthrough {
		pos, kvDesc, blobDesc, err = copyPassthroughKVSections(tf, plan.sourceView.data, plan.oldHead)
	} else {
		pos, kvDesc, blobDesc, err = writeKVSections(tf, int64(dataStart), plan.kvBlocks, plan.catalogRaw, mainEnc, target)
	}
	if err != nil {
		return 0, ds, nil, nil, err
	}
	ds[0], ds[1], ds[2] = kvDesc[0], blobDesc, kvDesc[2]

	inv := newInvertExternalBuilder(plan.dbPath, plan.invertRunBytes)
	defer inv.close()

	bw, err := newCompactBlockWriter(tf, pos, plan, inv, mainEnc)
	if err != nil {
		return 0, ds, nil, nil, err
	}
	defer bw.close()

	if err = streamCompactBlocks(plan, bw); err != nil {
		return 0, ds, nil, nil, err
	}
	pos = bw.pos
	idxRaw, writeToFinal, seriesBlocks := bw.finalizeIndex()
	if len(idxRaw) > 0 {
		idxData, compressed, err := encodeSmaller(mainEnc, nil, idxRaw, maxSectionRawBytes)
		if err != nil {
			return 0, ds, nil, nil, err
		}
		var idxCodec uint8
		if compressed {
			idxCodec = target
		}
		blockLen := pos - bw.blockStart
		if _, err = tf.WriteAt(idxData, pos); err != nil {
			return 0, ds, nil, nil, err
		}
		ds[3] = sectionDesc{kind: 3, codec: target, count: uint32(len(bw.metas)), offset: uint64(bw.blockStart), length: uint64(blockLen), rawLen: uint32(blockLen), crc: bw.blockHash.Sum32()}
		ds[4] = sectionDesc{kind: 4, codec: idxCodec, count: uint32(len(bw.metas)), offset: uint64(pos), length: uint64(len(idxData)), rawLen: uint32(len(idxRaw)), crc: crc(idxData)}
		pos += int64(len(idxData))
	}
	liveBits := newDictLiveBits(plan.dictSnap)
	var onInvKey func(uint64)
	if liveBits != nil {
		onInvKey = func(k uint64) {
			ik := decodeInvertKey(k)
			liveBits.mark(dictDimKey{series: ik.series, dim: ik.dim}, ik.tag)
		}
	}
	if len(plan.oldInvert) > 0 && len(bw.passSeriesCount) > 0 {
		if err = inv.addPassthroughFromInvert(plan.sourceView, plan.oldHead.sections[secTSInvert], plan.oldInvert, bw.passOldToWrite, bw.passSeriesCount); err != nil {
			return 0, ds, nil, nil, err
		}
	}
	var invDesc sectionDesc
	pos, invDesc, err = inv.writeSection(tf, pos, writeToFinal, seriesBlocks, mainEnc, target, onInvKey)
	if err != nil {
		return 0, ds, nil, nil, err
	}
	ds[6] = invDesc

	var prunedDims map[dictDimKey]bool
	if plan.dictSnap != nil {
		if liveBits != nil && len(plan.tailMetas) > 0 {
			if err = markTailDeltasLive(plan.sourceView.data, plan.tailMetas, plan.tailResetAllSeq, liveBits); err != nil {
				return 0, ds, nil, nil, err
			}
		}
		var dictDesc sectionDesc
		pos, dictDesc, prunedDims, err = writeDictSection(tf, pos, plan.dictSnap, plan.sourceView, mainEnc, target, liveBits)
		if err != nil {
			return 0, ds, nil, nil, err
		}
		ds[5] = dictDesc
	}

	for i, o := range plan.optional {
		slot := o.slot
		if slot < 7 || slot >= len(ds) || ds[slot].length != 0 {
			slot = 7 + i
		}
		if slot >= len(ds) {
			break
		}
		if _, err = tf.WriteAt(o.data, pos); err != nil {
			return 0, ds, nil, nil, err
		}
		o.desc.offset, o.desc.length, o.desc.crc = uint64(pos), uint64(len(o.data)), crc(o.data)
		ds[slot] = o.desc
		pos += int64(len(o.data))
	}
	return (pos + 7) &^ 7, ds, liveBits, prunedDims, nil
}
