package strata

import (
	"encoding/binary"
	"hash"
	"hash/crc32"
	"maps"
	"slices"
	"sort"
	"unsafe"

	"github.com/klauspost/compress/zstd"
)

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
}

type dictCompactSnap struct {
	series []dictSeriesSnap
	dims   []dictDimCompactSnap
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

func writeDictSectionV2(tf *osFile, pos int64, snap *dictCompactSnap, sourceView *fileView, enc *zstd.Encoder) (int64, sectionDesc, error) {
	if snap == nil || (len(snap.series) == 0 && len(snap.dims) == 0) {
		return pos, sectionDesc{}, nil
	}
	secStart := pos
	secHash := crc32.New(castagnoli)
	copyBuf := make([]byte, 64<<10)
	var plainBuf, compBuf []byte
	var inlineDims []dictDimCompactSnap
	var extDims []writtenExtDim

	for i := range snap.dims {
		d := &snap.dims[i]
		maxID := uint32(0)
		if d.nextID > 0 {
			maxID = d.nextID - 1
		}
		if d.ext == nil && maxID <= highCardThreshold {
			inlineDims = append(inlineDims, *d)
			continue
		}
		var we writtenExtDim
		var err error
		pos, we, err = writeOneExtDim(tf, secStart, pos, secHash, d, sourceView, enc, copyBuf, &plainBuf, &compBuf)
		if err != nil {
			return pos, sectionDesc{}, err
		}
		extDims = append(extDims, we)
	}

	dirRaw := encodeDictV2Dir(snap.series, inlineDims, extDims)
	dirComp := enc.EncodeAll(dirRaw, nil)
	if _, err := tf.WriteAt(dirComp, pos); err != nil {
		return pos, sectionDesc{}, err
	}
	_, _ = secHash.Write(dirComp)
	pos += int64(len(dirComp))

	var trailer [16]byte
	copy(trailer[0:4], dictV2Magic[:])
	binary.LittleEndian.PutUint32(trailer[4:8], uint32(len(dirComp)))
	binary.LittleEndian.PutUint32(trailer[8:12], uint32(len(dirRaw)))
	binary.LittleEndian.PutUint32(trailer[12:16], crc(dirComp))
	if _, err := tf.WriteAt(trailer[:], pos); err != nil {
		return pos, sectionDesc{}, err
	}
	_, _ = secHash.Write(trailer[:])
	pos += int64(len(trailer))

	desc := sectionDesc{
		kind: 5, codec: 2, count: 1,
		offset: uint64(secStart), length: uint64(pos - secStart),
		rawLen: uint32(len(dirRaw)), crc: secHash.Sum32(),
	}
	return pos, desc, nil
}

func writeOneExtDim(tf *osFile, secStart, pos int64, secHash hash.Hash32, d *dictDimCompactSnap, sourceView *fileView, enc *zstd.Encoder, copyBuf []byte, plainBuf, compBuf *[]byte) (int64, writtenExtDim, error) {
	we := writtenExtDim{key: d.key, baseNextID: d.nextID, baseCount: uint32(d.count)}
	if d.ext != nil && d.nextID == d.ext.baseNextID && len(d.extra) == 0 {
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
	var dec *zstd.Decoder
	var oldPlain []byte
	vals := make([]string, 0, dictFwdPageSize)
	for p := range totalPages {
		startID := uint32(p*dictFwdPageSize + 1)
		endID := min(startID+dictFwdPageSize, d.nextID)
		canPass := d.ext != nil && p < fullOldPages && !pageHasExtra(d.extra, startID, endID)
		if canPass {
			oldDesc := d.ext.fwdPages[p]
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
		if d.ext != nil && p < len(d.ext.fwdPages) {
			if dec == nil {
				dec = getZstdDecoder()
				defer putZstdDecoder(dec)
			}
			var err error
			oldPlain, err = d.ext.readPageInto(d.ext.fwdStartOff, d.ext.fwdPages[p], &oldPlain, dec)
			if err != nil {
				return pos, we, err
			}
			oldCount := decodeFwdPageCount(oldPlain)
			for slot := range int(endID - startID) {
				id := startID + uint32(slot)
				if d.extra != nil {
					if v, ok := d.extra[id]; ok {
						vals = append(vals, v)
						continue
					}
				}
				if slot < oldCount && id < d.ext.baseNextID {
					vals = append(vals, decodeFwdPageAt(oldPlain, slot))
				} else {
					vals = append(vals, d.resolveLocal(id))
				}
			}
		} else {
			for id := startID; id < endID; id++ {
				vals = append(vals, d.resolveLocal(id))
			}
		}
		comp := encodeFwdPage(vals, plainBuf, compBuf, enc)
		out := comp
		rawLen := uint32(len(*plainBuf))
		if len(comp) >= len(*plainBuf) {
			out = *plainBuf
			rawLen = 0
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

	// 2. Write Reverse SST Blocks.
	sstAreaStart := pos
	we.sstStartOff = uint64(sstAreaStart - secStart)
	var err error
	if d.ext != nil {
		tail := make([]dictSSTEntry, 0, int(d.nextID-d.tailBase)+len(d.extra))
		for id := d.tailBase; id < d.nextID; id++ {
			if d.extra != nil {
				if _, ok := d.extra[id]; ok {
					continue
				}
			}
			if v := d.resolveLocal(id); v != "" {
				tail = append(tail, dictSSTEntry{key: v, id: id})
			}
		}
		for id, v := range d.extra {
			if v != "" {
				tail = append(tail, dictSSTEntry{key: v, id: id})
			}
		}
		pos, we.sstFirstArena, we.sstFirstOffs, we.sstBlocks, err = writeExtDimMergedSST(tf, secStart, pos, secHash, enc, d.ext, tail, d.extra)
	} else {
		ids := make([]uint32, 0, d.count)
		for id := uint32(1); id < d.nextID; id++ {
			if d.resolveLocal(id) != "" {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool {
			return d.resolveLocal(ids[i]) < d.resolveLocal(ids[j])
		})
		w := newDictSSTWriter(tf, secStart, pos, secHash, enc)
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
