package strata

import (
	"encoding/binary"
	"hash"
	"hash/crc32"
	"maps"
	"math/bits"
	"slices"
	"sort"
	"unsafe"

	"github.com/UruhaLushia/strata/codec"
)

const dictGCMinDead = 1024

type dictLiveBits map[dictDimKey][]uint64

func newDictLiveBits(snap *dictCompactSnap) dictLiveBits {
	if snap == nil || len(snap.dims) == 0 {
		return nil
	}
	lb := make(dictLiveBits, len(snap.dims))
	for i := range snap.dims {
		if d := &snap.dims[i]; d.nextID > 1 {
			lb[d.key] = make([]uint64, (int(d.nextID)+63)/64)
		}
	}
	return lb
}

func (lb dictLiveBits) mark(key dictDimKey, id uint32) {
	if w := lb[key]; id > 0 && int(id>>6) < len(w) {
		w[id>>6] |= 1 << (id & 63)
	}
}

func (lb dictLiveBits) has(key dictDimKey, id uint32) bool {
	return id > 0 && bitHas(lb[key], id)
}

func bitHas(words []uint64, id uint32) bool {
	idx := int(id >> 6)
	return idx < len(words) && words[idx]&(1<<(id&63)) != 0
}

func hasLiveInRange(words []uint64, startID, endID uint32) bool {
	for id := startID; id < endID; {
		wi := int(id >> 6)
		if wi >= len(words) {
			return false
		}
		lo, rem := id&63, endID-id
		mask := ^uint64(0) << lo
		if rem < 64-lo {
			mask &= (uint64(1) << (lo + rem)) - 1
		}
		if words[wi]&mask != 0 {
			return true
		}
		id += min(rem, 64-lo)
	}
	return false
}

type dictSeriesSnap struct {
	id         uint16
	cfg        SeriesConfig
	resetEpoch uint64
	rules      []resetRule
}

type dictDimCompactSnap struct {
	key      dictDimKey
	arena    []byte
	offsets  []uint32
	extra    map[uint32]string
	tailBase uint32
	nextID   uint32
	count    int
	ext      *dictExtDim
	keepBits []uint64
}

type dictCompactSnap struct {
	series []dictSeriesSnap
	dims   []dictDimCompactSnap
}

func (s *dictDimCompactSnap) resolve(id uint32) (string, error) {
	if v := s.resolveLocal(id); v != "" || s.ext == nil || id == 0 || id >= s.tailBase {
		return v, nil
	}
	if _, ok := s.extra[id]; ok {
		return "", nil
	}
	return s.ext.resolve(id)
}

func (s *dictDimCompactSnap) resolveLocal(id uint32) string {
	if id == 0 {
		return ""
	}
	if s.extra != nil {
		if v, ok := s.extra[id]; ok {
			return v
		}
	}
	if id < s.tailBase {
		return ""
	}
	idx := int(id - s.tailBase)
	if idx+1 >= len(s.offsets) {
		return ""
	}
	lo, hi := s.offsets[idx], s.offsets[idx+1]
	if lo == hi {
		return ""
	}
	return unsafe.String(&s.arena[lo], hi-lo)
}

func snapshotDictForCompact(db *DB, fineTarget int64, unsealedResetMinEpoch map[uint16]uint64) *dictCompactSnap {
	snap := &dictCompactSnap{
		series: make([]dictSeriesSnap, 0, len(db.series)),
		dims:   make([]dictDimCompactSnap, 0, len(db.dict.dims)),
	}
	for _, s := range db.series {
		minUnsealed, hasUnsealed := unsealedResetMinEpoch[s.id]
		var active []resetRule
		for _, r := range s.rules {
			if (hasUnsealed && r.oldEpoch >= minUnsealed) || (!r.isAll() && r.to > fineTarget) {
				active = append(active, r)
			}
		}
		snap.series = append(snap.series, dictSeriesSnap{
			id: s.id, cfg: s.cfg, resetEpoch: s.resetEpoch, rules: active,
		})
	}
	slices.SortFunc(snap.series, func(a, b dictSeriesSnap) int {
		if a.id < b.id {
			return -1
		}
		return 1
	})
	for k, dim := range db.dict.dims {
		if dim == nil || dim.count == 0 {
			continue
		}
		var extra map[uint32]string
		if len(dim.extra) > 0 {
			extra = make(map[uint32]string, len(dim.extra))
			maps.Copy(extra, dim.extra)
		}
		snap.dims = append(snap.dims, dictDimCompactSnap{
			key:      k,
			arena:    dim.arena[:len(dim.arena):len(dim.arena)],
			offsets:  dim.offsets[:len(dim.offsets):len(dim.offsets)],
			extra:    extra,
			tailBase: dim.tailBase,
			nextID:   dim.nextID,
			count:    dim.count,
			ext:      dim.ext,
		})
	}
	slices.SortFunc(snap.dims, func(a, b dictDimCompactSnap) int {
		if a.key.series != b.key.series {
			if a.key.series < b.key.series {
				return -1
			}
			return 1
		}
		if a.key.dim < b.key.dim {
			return -1
		}
		return 1
	})
	return snap
}

type writtenExtDim struct {
	key           dictDimKey
	baseNextID    uint32
	baseCount     uint32
	fwdStartOff   uint64
	fwdTotalBytes uint64
	fwdPages      []dictPageDesc
	sstStartOff   uint64
	sstTotalBytes uint64
	sstFirstArena []byte
	sstFirstOffs  []uint32
	sstBlocks     []dictPageDesc
}

func writeDictSection(tf *osFile, pos int64, snap *dictCompactSnap, sourceView *fileView, enc codec.Encoder, target uint8, live dictLiveBits) (int64, sectionDesc, map[dictDimKey]bool, error) {
	if snap == nil || (len(snap.series) == 0 && len(snap.dims) == 0) {
		return pos, sectionDesc{}, nil, nil
	}
	secStart := pos
	secHash := crc32.New(castagnoli)
	copyBuf := make([]byte, 64<<10)
	var plainBuf, compBuf []byte
	var inlineDims []dictDimCompactSnap
	var extDims []writtenExtDim
	var pruned map[dictDimKey]bool
	markPruned := func(k dictDimKey) {
		if pruned == nil {
			pruned = make(map[dictDimKey]bool)
		}
		pruned[k] = true
	}

	for i := range snap.dims {
		d := &snap.dims[i]
		var keepBits []uint64
		if words := live[d.key]; len(words) > 0 {
			liveCnt := 0
			for _, w := range words {
				liveCnt += bits.OnesCount64(w)
			}
			liveCnt = min(liveCnt, d.count)
			if liveCnt == 0 {
				markPruned(d.key)
				continue
			}
			if dead := d.count - liveCnt; dead >= dictGCMinDead && dead*4 >= d.count {
				keepBits = words
				markPruned(d.key)
			}
		}
		maxID := uint32(0)
		if d.nextID > 0 {
			maxID = d.nextID - 1
		}
		if d.ext == nil && maxID <= highCardThreshold {
			cd := *d
			cd.keepBits = keepBits
			inlineDims = append(inlineDims, cd)
			continue
		}
		var we writtenExtDim
		var err error
		pos, we, err = writeOneExtDim(tf, secStart, pos, secHash, d, sourceView, enc, target, copyBuf, &plainBuf, &compBuf, keepBits)
		if err != nil {
			return pos, sectionDesc{}, nil, err
		}
		if we.baseCount > 0 {
			extDims = append(extDims, we)
		}
	}

	dirRaw := encodeDictDir(snap.series, inlineDims, extDims)
	dirComp, compressed, err := encodeSmaller(enc, nil, dirRaw, maxSectionRawBytes)
	if err != nil {
		return pos, sectionDesc{}, nil, err
	}
	var dirRawLen uint32
	if compressed {
		dirRawLen = uint32(len(dirRaw))
	}
	if _, err := tf.WriteAt(dirComp, pos); err != nil {
		return pos, sectionDesc{}, nil, err
	}
	_, _ = secHash.Write(dirComp)
	pos += int64(len(dirComp))

	var trailer [16]byte
	copy(trailer[0:4], dictMagic[:])
	binary.LittleEndian.PutUint32(trailer[4:8], uint32(len(dirComp)))
	binary.LittleEndian.PutUint32(trailer[8:12], dirRawLen)
	binary.LittleEndian.PutUint32(trailer[12:16], crc(dirComp))
	if _, err := tf.WriteAt(trailer[:], pos); err != nil {
		return pos, sectionDesc{}, nil, err
	}
	_, _ = secHash.Write(trailer[:])
	pos += int64(len(trailer))

	desc := sectionDesc{
		kind: 5, codec: target, count: 1,
		offset: uint64(secStart), length: uint64(pos - secStart),
		rawLen: dirRawLen, crc: secHash.Sum32(),
	}
	return pos, desc, pruned, nil
}

func writeOneExtDim(tf *osFile, secStart, pos int64, secHash hash.Hash32, d *dictDimCompactSnap, sourceView *fileView, enc codec.Encoder, target uint8, copyBuf []byte, plainBuf, compBuf *[]byte, keepBits []uint64) (int64, writtenExtDim, error) {
	we := writtenExtDim{key: d.key, baseNextID: d.nextID, baseCount: uint32(d.count)}
	sameCodec := d.ext != nil && d.ext.secDesc.codec == target
	if keepBits == nil && sameCodec && d.nextID == d.ext.baseNextID && len(d.extra) == 0 {
		// Unchanged external dim: pure raw byte passthrough (0 decode, 0 recompress).
		we.fwdStartOff = uint64(pos - secStart)
		we.fwdTotalBytes = d.ext.fwdTotalBytes
		we.fwdPages = d.ext.fwdPages
		var err error
		pos, err = streamCopyDictRange(tf, pos, secHash, sourceView.data, int64(d.ext.secDesc.offset+d.ext.fwdStartOff), d.ext.fwdTotalBytes, copyBuf)
		if err != nil {
			return pos, we, err
		}
		we.sstStartOff = uint64(pos - secStart)
		we.sstTotalBytes = d.ext.sstTotalBytes
		we.sstFirstArena = d.ext.sstFirstArena
		we.sstFirstOffs = d.ext.sstFirstOffs
		we.sstBlocks = d.ext.sstBlocks
		pos, err = streamCopyDictRange(tf, pos, secHash, sourceView.data, int64(d.ext.secDesc.offset+d.ext.sstStartOff), d.ext.sstTotalBytes, copyBuf)
		return pos, we, err
	}

	// 1. Write Forward Pages (512 IDs/page).
	fwdAreaStart := pos
	we.fwdStartOff = uint64(fwdAreaStart - secStart)
	totalIDs := int(d.nextID - 1)
	totalPages := (totalIDs + dictFwdPageSize - 1) / dictFwdPageSize
	we.fwdPages = make([]dictPageDesc, 0, totalPages)
	fullOldPages := 0
	if d.ext != nil && d.ext.baseNextID > 1 {
		fullOldPages = int(d.ext.baseNextID-1) / dictFwdPageSize
	}
	slot := querySlot{codecs: sourceView.codecs}
	defer slot.close()
	var oldPlain []byte
	vals := make([]string, 0, dictFwdPageSize)
	var liveWritten uint32
	for p := range totalPages {
		startID := uint32(p*dictFwdPageSize + 1)
		endID := min(startID+dictFwdPageSize, d.nextID)
		if keepBits != nil && !hasLiveInRange(keepBits, startID, endID) {
			we.fwdPages = append(we.fwdPages, dictPageDesc{})
			continue
		}
		canPass := keepBits == nil && sameCodec && p < fullOldPages && !pageHasExtra(d.extra, startID, endID)
		if canPass {
			oldDesc := d.ext.fwdPages[p]
			if oldDesc.length == 0 {
				we.fwdPages = append(we.fwdPages, dictPageDesc{})
				continue
			}
			relOff := uint64(pos - fwdAreaStart)
			var err error
			pos, err = streamCopyDictRange(tf, pos, secHash, sourceView.data, int64(d.ext.secDesc.offset+d.ext.fwdStartOff+oldDesc.relOff), uint64(oldDesc.length), copyBuf)
			if err != nil {
				return pos, we, err
			}
			we.fwdPages = append(we.fwdPages, dictPageDesc{relOff: relOff, length: oldDesc.length, rawLen: oldDesc.rawLen})
			continue
		}
		vals = vals[:0]
		hasNonEmpty := false
		oldCount := 0
		if d.ext != nil && p < len(d.ext.fwdPages) && d.ext.fwdPages[p].length > 0 {
			var err error
			oldPlain, err = d.ext.readPageInto(d.ext.fwdStartOff, d.ext.fwdPages[p], &oldPlain, &slot)
			if err != nil {
				return pos, we, err
			}
			oldCount = decodeFwdPageCount(oldPlain)
		}
		for slot := range int(endID - startID) {
			id := startID + uint32(slot)
			if keepBits != nil && !bitHas(keepBits, id) {
				vals = append(vals, "")
				continue
			}
			var v string
			if ev, ok := d.extra[id]; ok {
				v = ev
			} else if d.ext != nil && id < d.ext.baseNextID {
				if slot < oldCount {
					v = decodeFwdPageAt(oldPlain, slot)
				}
			} else {
				v = d.resolveLocal(id)
			}
			if v != "" {
				hasNonEmpty = true
				liveWritten++
			}
			vals = append(vals, v)
		}
		if !hasNonEmpty {
			we.fwdPages = append(we.fwdPages, dictPageDesc{})
			continue
		}
		out, compressed, err := encodeFwdPage(vals, plainBuf, *compBuf, enc)
		if err != nil {
			return pos, we, err
		}
		var rawLen uint32
		if compressed {
			*compBuf = out
			rawLen = uint32(len(*plainBuf))
		}
		relOff := uint64(pos - fwdAreaStart)
		if _, err := tf.WriteAt(out, pos); err != nil {
			return pos, we, err
		}
		_, _ = secHash.Write(out)
		pos += int64(len(out))
		we.fwdPages = append(we.fwdPages, dictPageDesc{relOff: relOff, length: uint32(len(out)), rawLen: rawLen})
	}
	we.fwdTotalBytes = uint64(pos - fwdAreaStart)
	if keepBits != nil {
		we.baseCount = liveWritten
		if liveWritten == 0 {
			return pos, we, nil
		}
	}

	// 2. Write Reverse SST Blocks.
	sstAreaStart := pos
	we.sstStartOff = uint64(sstAreaStart - secStart)
	var err error
	if d.ext != nil {
		tail := make([]dictSSTEntry, 0, int(d.nextID-d.tailBase)+len(d.extra))
		for id := d.tailBase; id < d.nextID; id++ {
			if keepBits != nil && !bitHas(keepBits, id) {
				continue
			}
			if _, ok := d.extra[id]; ok {
				continue
			}
			if v := d.resolveLocal(id); v != "" {
				tail = append(tail, dictSSTEntry{key: v, id: id})
			}
		}
		for id, v := range d.extra {
			if v != "" && (keepBits == nil || bitHas(keepBits, id)) {
				tail = append(tail, dictSSTEntry{key: v, id: id})
			}
		}
		pos, we.sstFirstArena, we.sstFirstOffs, we.sstBlocks, err = writeExtDimMergedSST(tf, pos, secHash, enc, d.ext, tail, d.extra, keepBits)
	} else {
		ids := make([]uint32, 0, we.baseCount)
		for id := uint32(1); id < d.nextID; id++ {
			if keepBits != nil && !bitHas(keepBits, id) {
				continue
			}
			if d.resolveLocal(id) != "" {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool {
			return d.resolveLocal(ids[i]) < d.resolveLocal(ids[j])
		})
		w := newDictSSTWriter(tf, pos, secHash, enc)
		for _, id := range ids {
			if err = w.add(d.resolveLocal(id), id); err != nil {
				return pos, we, err
			}
		}
		if err = w.flush(); err != nil {
			return pos, we, err
		}
		pos, we.sstFirstArena, we.sstFirstOffs, we.sstBlocks = w.pos, w.firstArena, w.firstOffs, w.blocks
	}
	if err != nil {
		return pos, we, err
	}
	we.sstTotalBytes = uint64(pos - sstAreaStart)
	return pos, we, nil
}

func pageHasExtra(extra map[uint32]string, startID, endID uint32) bool {
	for id := range extra {
		if id >= startID && id < endID {
			return true
		}
	}
	return false
}
