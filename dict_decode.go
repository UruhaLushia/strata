package strata

import (
	"bytes"
	"encoding/binary"
	"errors"
)

func decodeDict(f *osFile, desc sectionDesc, mapped []byte, view *fileView, d *dictionary, series map[uint16]*Series) error {
	if desc.length < 16 {
		return errors.New("strata: truncated dict section")
	}
	trRaw, trBuf, err := sectionBlockPooled(f, desc, desc.offset+desc.length-16, 16, mapped)
	if trBuf != nil {
		defer putReadBuf(trBuf)
	}
	if err != nil {
		return err
	}
	if !bytes.Equal(trRaw[0:4], dictMagic[:]) {
		return errors.New("strata: invalid dict magic")
	}
	dirCompLen := binary.LittleEndian.Uint32(trRaw[4:8])
	dirRawLen := binary.LittleEndian.Uint32(trRaw[8:12])
	dirCRC := binary.LittleEndian.Uint32(trRaw[12:16])
	if dirRawLen > maxSectionRawBytes || uint64(dirCompLen)+16 > desc.length {
		return errors.New("strata: invalid dict directory length")
	}
	dirOff := desc.offset + desc.length - 16 - uint64(dirCompLen)
	dirComp, dirBuf, err := sectionBlockPooled(f, desc, dirOff, dirCompLen, mapped)
	if dirBuf != nil {
		defer putReadBuf(dirBuf)
	}
	if err != nil {
		return err
	}
	if crc(dirComp) != dirCRC {
		return errors.New("strata: dictionary CRC mismatch")
	}
	raw := dirComp
	if dirRawLen != 0 {
		raw, err = view.codecs.decode(desc.codec, nil, dirComp, int(dirRawLen))
		if err != nil {
			return err
		}
		if len(raw) != int(dirRawLen) {
			return errors.New("strata: invalid dict directory length")
		}
	}
	return parseDictDir(raw, desc, view, d, series)
}

func parseDictDir(raw []byte, desc sectionDesc, view *fileView, d *dictionary, series map[uint16]*Series) error {
	off := 0
	sn, err := readDictUvarint(raw, &off)
	if err != nil {
		return err
	}
	for range sn {
		sid, err := readDictUint16(raw, &off)
		if err != nil || off >= len(raw) {
			return errors.New("strata: invalid dict series")
		}
		arity := raw[off]
		off++
		if arity < 1 || arity > 8 || off+16 > len(raw) {
			return errors.New("strata: invalid dict series arity")
		}
		var c SeriesConfig
		c.Arity = int(arity)
		for j := range c.SummaryOps {
			c.SummaryOps[j] = MetricOp(raw[off])
			off++
		}
		for j := range c.RowOps {
			c.RowOps[j] = MetricOp(raw[off])
			off++
		}
		epoch, err := readDictUint64(raw, &off)
		if err != nil {
			return err
		}
		s := series[sid]
		if s == nil {
			s = &Series{id: sid}
			series[sid] = s
		}
		s.cfg, s.resetEpoch = c, epoch
		rn, err := readDictUvarint(raw, &off)
		if err != nil || rn > uint64(len(raw)-off)/24 {
			return errors.New("strata: invalid dict reset rules")
		}
		s.rules = make([]resetRule, rn)
		for j := range s.rules {
			if s.rules[j].oldEpoch, err = readDictUint64(raw, &off); err != nil {
				return err
			}
			if s.rules[j].from, err = readDictInt64(raw, &off); err != nil {
				return err
			}
			if s.rules[j].to, err = readDictInt64(raw, &off); err != nil {
				return err
			}
		}
	}

	if d.dims == nil {
		d.dims = make(map[dictDimKey]*dictDim)
	}
	inN, err := readDictUvarint(raw, &off)
	if err != nil {
		return err
	}
	for range inN {
		sid, err := readDictUint16(raw, &off)
		if err != nil || off >= len(raw) {
			return errors.New("strata: invalid dict inline dim")
		}
		dimIdx := raw[off]
		off++
		maxID, err := readDictUint32(raw, &off)
		if err != nil {
			return err
		}
		cnt, err := readDictUint32(raw, &off)
		if err != nil {
			return err
		}
		arenaLen, err := readDictUint32(raw, &off)
		if err != nil {
			return err
		}
		if uint64(maxID) > uint64(len(raw)-off) {
			return errors.New("strata: invalid dict inline maxID")
		}
		offs := make([]uint32, int(maxID)+2)
		var cur uint32
		for id := uint32(1); id <= maxID; id++ {
			ln, err := readDictUvarint(raw, &off)
			if err != nil || uint64(cur)+ln > uint64(arenaLen) {
				return errors.New("strata: invalid dict inline length")
			}
			cur += uint32(ln)
			offs[id+1] = cur
		}
		if cur != arenaLen || int(arenaLen) > len(raw)-off {
			return errors.New("strata: truncated dict inline arena")
		}
		arena := make([]byte, arenaLen)
		copy(arena, raw[off:off+int(arenaLen)])
		off += int(arenaLen)
		d.dims[dictDimKey{series: sid, dim: dimIdx}] = &dictDim{
			arena:   arena,
			offsets: offs,
			nextID:  maxID + 1,
			count:   int(cnt),
		}
	}

	extN, err := readDictUvarint(raw, &off)
	if err != nil {
		return err
	}
	for range extN {
		sid, err := readDictUint16(raw, &off)
		if err != nil || off >= len(raw) {
			return errors.New("strata: invalid dict ext dim")
		}
		dimIdx := raw[off]
		off++
		baseNextID, err := readDictUint32(raw, &off)
		if err != nil {
			return err
		}
		baseCount, err := readDictUint32(raw, &off)
		if err != nil {
			return err
		}
		fwdStart, err := readDictUint64(raw, &off)
		if err != nil {
			return err
		}
		fwdTotal, err := readDictUint64(raw, &off)
		if err != nil {
			return err
		}
		fwdN, err := readDictUvarint(raw, &off)
		if err != nil || fwdN > uint64(len(raw)-off) {
			return errors.New("strata: invalid dict fwd page count")
		}
		fwdPages := make([]dictPageDesc, fwdN)
		for j := range fwdPages {
			rel, err := readDictUvarint(raw, &off)
			if err != nil {
				return err
			}
			ln, err := readDictUvarint(raw, &off)
			if err != nil {
				return err
			}
			rln, err := readDictUvarint(raw, &off)
			if err != nil {
				return err
			}
			fwdPages[j] = dictPageDesc{relOff: rel, length: uint32(ln), rawLen: uint32(rln)}
		}
		sstStart, err := readDictUint64(raw, &off)
		if err != nil {
			return err
		}
		sstTotal, err := readDictUint64(raw, &off)
		if err != nil {
			return err
		}
		sstN, err := readDictUvarint(raw, &off)
		if err != nil || sstN > uint64(len(raw)-off) {
			return errors.New("strata: invalid dict sst block count")
		}
		firstArenaLen, err := readDictUint32(raw, &off)
		if err != nil {
			return err
		}
		firstOffs := make([]uint32, int(sstN)+1)
		var fcur uint32
		for j := range int(sstN) {
			ln, err := readDictUvarint(raw, &off)
			if err != nil || uint64(fcur)+ln > uint64(firstArenaLen) {
				return errors.New("strata: invalid dict sst firstKey length")
			}
			fcur += uint32(ln)
			firstOffs[j+1] = fcur
		}
		if fcur != firstArenaLen || int(firstArenaLen) > len(raw)-off {
			return errors.New("strata: truncated dict sst firstKeys")
		}
		firstArena := make([]byte, firstArenaLen)
		copy(firstArena, raw[off:off+int(firstArenaLen)])
		off += int(firstArenaLen)

		sstBlocks := make([]dictPageDesc, sstN)
		for j := range sstBlocks {
			rel, err := readDictUvarint(raw, &off)
			if err != nil {
				return err
			}
			ln, err := readDictUvarint(raw, &off)
			if err != nil {
				return err
			}
			rln, err := readDictUvarint(raw, &off)
			if err != nil {
				return err
			}
			sstBlocks[j] = dictPageDesc{relOff: rel, length: uint32(ln), rawLen: uint32(rln)}
		}
		ext := &dictExtDim{
			series:        sid,
			dim:           dimIdx,
			baseNextID:    baseNextID,
			baseCount:     baseCount,
			secDesc:       desc,
			fwdStartOff:   fwdStart,
			fwdTotalBytes: fwdTotal,
			fwdPages:      fwdPages,
			sstStartOff:   sstStart,
			sstTotalBytes: sstTotal,
			sstFirstArena: firstArena,
			sstFirstOffs:  firstOffs,
			sstBlocks:     sstBlocks,
			view:          view,
			cache:         d.pageCache,
		}
		d.dims[dictDimKey{series: sid, dim: dimIdx}] = &dictDim{
			offsets:  []uint32{0},
			tailBase: baseNextID,
			nextID:   baseNextID,
			count:    int(baseCount),
			ext:      ext,
		}
	}
	if off != len(raw) {
		return errors.New("strata: trailing dict directory bytes")
	}
	return nil
}
