package strata

import (
	"encoding/binary"
	"hash"
	"slices"

	"github.com/klauspost/compress/zstd"
)

type dictSSTWriter struct {
	tf         *osFile
	areaStart  int64
	pos        int64
	secHash    hash.Hash32
	enc        *zstd.Encoder
	rawBuf     []byte
	compBuf    []byte
	prevKey    []byte
	firstKey   []byte
	entryCount int
	firstArena []byte
	firstOffs  []uint32
	blocks     []dictPageDesc
}

func newDictSSTWriter(tf *osFile, pos int64, secHash hash.Hash32, enc *zstd.Encoder) *dictSSTWriter {
	return &dictSSTWriter{
		tf:        tf,
		areaStart: pos,
		pos:       pos,
		secHash:   secHash,
		enc:       enc,
		rawBuf:    make([]byte, 0, dictSSTTargetBytes+256),
		firstOffs: []uint32{0},
	}
}

func (w *dictSSTWriter) add(key string, id uint32) error {
	if key == "" || id == 0 {
		return nil
	}
	if w.entryCount > 0 && len(w.rawBuf)+len(key)+16 > dictSSTTargetBytes {
		if err := w.flush(); err != nil {
			return err
		}
	}
	shared := 0
	if w.entryCount == 0 {
		w.firstKey = append(w.firstKey[:0], key...)
	} else {
		lim := min(len(w.prevKey), len(key))
		for shared < lim && w.prevKey[shared] == key[shared] {
			shared++
		}
	}
	suffix := key[shared:]
	w.rawBuf = binary.AppendUvarint(w.rawBuf, uint64(shared))
	w.rawBuf = binary.AppendUvarint(w.rawBuf, uint64(len(suffix)))
	w.rawBuf = append(w.rawBuf, suffix...)
	w.rawBuf = binary.AppendUvarint(w.rawBuf, uint64(id))
	w.prevKey = append(w.prevKey[:0], key...)
	w.entryCount++
	return nil
}

func (w *dictSSTWriter) flush() error {
	if w.entryCount == 0 {
		return nil
	}
	var hdr [10]byte
	hn := binary.PutUvarint(hdr[:], uint64(w.entryCount))
	plain := make([]byte, hn+len(w.rawBuf))
	copy(plain, hdr[:hn])
	copy(plain[hn:], w.rawBuf)

	w.compBuf = w.enc.EncodeAll(plain, w.compBuf[:0])
	out := plain
	var rawLen uint32
	if len(w.compBuf) < len(plain) {
		out = w.compBuf
		rawLen = uint32(len(plain))
	}
	relOff := uint64(w.pos - w.areaStart)
	if _, err := w.tf.WriteAt(out, w.pos); err != nil {
		return err
	}
	_, _ = w.secHash.Write(out)
	w.pos += int64(len(out))

	w.firstArena = append(w.firstArena, w.firstKey...)
	w.firstOffs = append(w.firstOffs, uint32(len(w.firstArena)))
	w.blocks = append(w.blocks, dictPageDesc{relOff: relOff, length: uint32(len(out)), rawLen: rawLen})
	w.rawBuf = w.rawBuf[:0]
	w.prevKey = w.prevKey[:0]
	w.entryCount = 0
	return nil
}

type dictSSTEntry struct {
	key string
	id  uint32
}

type dictSSTStream struct {
	ext      *dictExtDim
	dec      *zstd.Decoder
	blkIdx   int
	plainBuf []byte
	keyBuf   []byte
	entries  []dictSSTEntry
	pos      int
}

func newDictSSTStream(ext *dictExtDim) *dictSSTStream {
	return &dictSSTStream{ext: ext, dec: getZstdDecoder()}
}

func (s *dictSSTStream) close() {
	if s.dec != nil {
		putZstdDecoder(s.dec)
		s.dec = nil
	}
}

func (s *dictSSTStream) next() (dictSSTEntry, bool, error) {
	for s.pos >= len(s.entries) {
		if s.ext == nil || s.blkIdx >= len(s.ext.sstBlocks) {
			return dictSSTEntry{}, false, nil
		}
		plain, err := s.ext.readPageInto(s.ext.sstStartOff, s.ext.sstBlocks[s.blkIdx], &s.plainBuf, s.dec)
		if err != nil {
			return dictSSTEntry{}, false, err
		}
		s.blkIdx++
		s.entries = s.entries[:0]
		s.pos = 0
		if err := decodeSSTBlock(plain, &s.keyBuf, func(key []byte, id uint32) bool {
			s.entries = append(s.entries, dictSSTEntry{key: string(key), id: id})
			return true
		}); err != nil {
			return dictSSTEntry{}, false, err
		}
	}
	e := s.entries[s.pos]
	s.pos++
	return e, true, nil
}

func streamCopyDictRange(tf *osFile, pos int64, secHash hash.Hash32, src *osFile, srcOff int64, length uint64, buf []byte) (int64, error) {
	rem := int64(length)
	for rem > 0 {
		n := min(rem, int64(len(buf)))
		chunk := buf[:n]
		if _, err := src.ReadAt(chunk, srcOff); err != nil {
			return pos, err
		}
		if _, err := tf.WriteAt(chunk, pos); err != nil {
			return pos, err
		}
		_, _ = secHash.Write(chunk)
		srcOff += n
		pos += n
		rem -= n
	}
	return pos, nil
}

func encodeFwdPage(vals []string, plainBuf, compBuf *[]byte, enc *zstd.Encoder) []byte {
	count := len(vals)
	totalStr := 0
	for _, v := range vals {
		totalStr += len(v)
	}
	hdrLen := 2 + (count+1)*4
	need := hdrLen + totalStr
	if cap(*plainBuf) < need {
		*plainBuf = make([]byte, need)
	} else {
		*plainBuf = (*plainBuf)[:need]
	}
	p := *plainBuf
	binary.LittleEndian.PutUint16(p[0:2], uint16(count))
	var strOff uint32
	for i, v := range vals {
		binary.LittleEndian.PutUint32(p[2+i*4:6+i*4], strOff)
		copy(p[hdrLen+int(strOff):], v)
		strOff += uint32(len(v))
	}
	binary.LittleEndian.PutUint32(p[2+count*4:6+count*4], strOff)
	*compBuf = enc.EncodeAll(p, (*compBuf)[:0])
	return *compBuf
}

func writeExtDimMergedSST(tf *osFile, pos int64, secHash hash.Hash32, enc *zstd.Encoder, oldExt *dictExtDim, tail []dictSSTEntry, overridden map[uint32]string, keepBits []uint64) (int64, []byte, []uint32, []dictPageDesc, error) {
	slices.SortFunc(tail, func(a, b dictSSTEntry) int {
		if a.key < b.key {
			return -1
		}
		if a.key > b.key {
			return 1
		}
		if a.id < b.id {
			return -1
		}
		if a.id > b.id {
			return 1
		}
		return 0
	})
	w := newDictSSTWriter(tf, pos, secHash, enc)
	stream := newDictSSTStream(oldExt)
	defer stream.close()

	readOld := func() (dictSSTEntry, bool, error) {
		for {
			e, ok, err := stream.next()
			if err != nil || !ok {
				return e, ok, err
			}
			if keepBits != nil && !bitHas(keepBits, e.id) {
				continue
			}
			if overridden != nil {
				if _, skip := overridden[e.id]; skip {
					continue
				}
			}
			return e, true, nil
		}
	}
	oldCur, hasOld, err := readOld()
	if err != nil {
		return pos, nil, nil, nil, err
	}
	ti := 0
	for hasOld || ti < len(tail) {
		takeTail := !hasOld || (ti < len(tail) && tail[ti].key < oldCur.key)
		if takeTail {
			if err := w.add(tail[ti].key, tail[ti].id); err != nil {
				return pos, nil, nil, nil, err
			}
			ti++
		} else {
			if err := w.add(oldCur.key, oldCur.id); err != nil {
				return pos, nil, nil, nil, err
			}
			oldCur, hasOld, err = readOld()
			if err != nil {
				return pos, nil, nil, nil, err
			}
		}
	}
	if err := w.flush(); err != nil {
		return pos, nil, nil, nil, err
	}
	return w.pos, w.firstArena, w.firstOffs, w.blocks, nil
}

func encodeDictDir(series []dictSeriesSnap, inline []dictDimCompactSnap, ext []writtenExtDim) []byte {
	var b []byte
	b = binary.AppendUvarint(b, uint64(len(series)))
	for _, s := range series {
		b = binary.LittleEndian.AppendUint16(b, s.id)
		b = append(b, byte(s.cfg.Arity))
		for _, op := range s.cfg.SummaryOps {
			b = append(b, byte(op))
		}
		for _, op := range s.cfg.RowOps {
			b = append(b, byte(op))
		}
		b = binary.LittleEndian.AppendUint64(b, s.resetEpoch)
		b = binary.AppendUvarint(b, uint64(len(s.rules)))
		for _, r := range s.rules {
			b = binary.LittleEndian.AppendUint64(b, r.oldEpoch)
			b = binary.LittleEndian.AppendUint64(b, uint64(r.from))
			b = binary.LittleEndian.AppendUint64(b, uint64(r.to))
		}
	}
	b = binary.AppendUvarint(b, uint64(len(inline)))
	for i := range inline {
		d := &inline[i]
		maxID := d.nextID - 1
		var arenaLen, liveCnt uint32
		for id := uint32(1); id <= maxID; id++ {
			if d.keepBits != nil && !bitHas(d.keepBits, id) {
				continue
			}
			if ln := uint32(len(d.resolveLocal(id))); ln > 0 {
				arenaLen += ln
				liveCnt++
			}
		}
		if d.keepBits == nil {
			liveCnt = uint32(d.count)
		}
		b = binary.LittleEndian.AppendUint16(b, d.key.series)
		b = append(b, d.key.dim)
		b = binary.LittleEndian.AppendUint32(b, maxID)
		b = binary.LittleEndian.AppendUint32(b, liveCnt)
		b = binary.LittleEndian.AppendUint32(b, arenaLen)
		for id := uint32(1); id <= maxID; id++ {
			if d.keepBits != nil && !bitHas(d.keepBits, id) {
				b = binary.AppendUvarint(b, 0)
				continue
			}
			b = binary.AppendUvarint(b, uint64(len(d.resolveLocal(id))))
		}
		for id := uint32(1); id <= maxID; id++ {
			if d.keepBits != nil && !bitHas(d.keepBits, id) {
				continue
			}
			b = append(b, d.resolveLocal(id)...)
		}
	}
	b = binary.AppendUvarint(b, uint64(len(ext)))
	for i := range ext {
		e := &ext[i]
		b = binary.LittleEndian.AppendUint16(b, e.key.series)
		b = append(b, e.key.dim)
		b = binary.LittleEndian.AppendUint32(b, e.baseNextID)
		b = binary.LittleEndian.AppendUint32(b, e.baseCount)
		b = binary.LittleEndian.AppendUint64(b, e.fwdStartOff)
		b = binary.LittleEndian.AppendUint64(b, e.fwdTotalBytes)
		b = binary.AppendUvarint(b, uint64(len(e.fwdPages)))
		for _, p := range e.fwdPages {
			b = binary.AppendUvarint(b, p.relOff)
			b = binary.AppendUvarint(b, uint64(p.length))
			b = binary.AppendUvarint(b, uint64(p.rawLen))
		}
		b = binary.LittleEndian.AppendUint64(b, e.sstStartOff)
		b = binary.LittleEndian.AppendUint64(b, e.sstTotalBytes)
		b = binary.AppendUvarint(b, uint64(len(e.sstBlocks)))
		b = binary.LittleEndian.AppendUint32(b, uint32(len(e.sstFirstArena)))
		for j := range e.sstBlocks {
			b = binary.AppendUvarint(b, uint64(e.sstFirstOffs[j+1]-e.sstFirstOffs[j]))
		}
		b = append(b, e.sstFirstArena...)
		for _, blk := range e.sstBlocks {
			b = binary.AppendUvarint(b, blk.relOff)
			b = binary.AppendUvarint(b, uint64(blk.length))
			b = binary.AppendUvarint(b, uint64(blk.rawLen))
		}
	}
	return b
}
