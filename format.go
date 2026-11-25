package strata

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"time"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const (
	slotSize               = 2048
	headerSize             = 4096
	dataStart              = 0x1000
	magic                  = "UNIDBv01"
	knownCompat     uint32 = 0
	knownReadOnly   uint32 = 0
	knownIncompat   uint32 = 0
	secFlagOptional uint8  = 1
	secKVSSTBlocks  uint16 = 0
	secKVLargeBlobs uint16 = 1
	secKVSSTIndex   uint16 = 2
	secTSBlocks     uint16 = 3
	secTSIndex      uint16 = 4
	secTSDict       uint16 = 5
	secTSInvert     uint16 = 6
)

type sectionDesc struct {
	kind           uint16
	codec, flags   uint8
	count          uint32
	offset, length uint64
	rawLen, crc    uint32
}

type superblock struct {
	version, flags                            uint16
	compat, roCompat, incompat                uint32
	gen, walOffset, sealedKVRev, sealedWalSeq uint64
	watermark, created, compacted             int64
	uuid                                      [16]byte
	user                                      [64]byte
	sections                                  [12]sectionDesc
}

// sealedMeta is the 80-byte pointer-free (100% noscan) resident block directory entry.
type sealedMeta struct {
	start    int64
	epoch    uint64
	order    uint64
	offset   uint64
	values   [4]int64
	length   uint32
	crc      uint32
	rowCount uint32
	series   uint16
	res      byte
	mask     uint8
}

func (m sealedMeta) end() int64 { return m.start + resWidth(m.res) }

func (m sealedMeta) summary() Summary { return Summary{Values: m.values, Mask: m.mask} }

func (m sealedMeta) toBlock() sealedBlock {
	return sealedBlock{
		series:   m.series,
		res:      m.res,
		start:    m.start,
		end:      m.end(),
		epoch:    m.epoch,
		order:    m.order,
		summary:  m.summary(),
		rowCount: m.rowCount,
		offset:   m.offset,
		length:   m.length,
		rawLen:   m.length,
		crc:      m.crc,
	}
}

// sealedBlock is a transient working structure used only during compaction and single-slot materialization.
type sealedBlock struct {
	series         uint16
	res            byte
	start, end     int64
	epoch          uint64
	order          uint64
	summary        Summary
	rowCount       uint32
	rows           []idRow
	offset         uint64
	length, rawLen uint32
	crc            uint32
	encoded        []byte
	candidate      bool
	passthrough    bool
	preserveOrder  bool
}

type resetRule struct {
	oldEpoch uint64
	from, to int64
}

func (r resetRule) isAll() bool {
	return r.from == math.MinInt64 && r.to == math.MaxInt64
}

type bucketKey struct {
	series uint16
	minute int64
	epoch  uint64
}

type bucket struct {
	series     uint16
	ts         int64
	epoch      uint64
	order      uint64
	arity      uint8
	summaryOps [4]MetricOp
	rowOps     [4]MetricOp
	summary    Summary
	rows       []idRow
}

type idRow struct {
	tags     [8]uint32
	values   [4]int64
	initMask uint8
}

type dictEntry struct {
	id     uint32
	series uint16
	dim    uint8
	value  string
}

type dictDimKey struct {
	series uint16
	dim    uint8
}

type resetFrame struct {
	series                  uint16
	seq, oldEpoch, newEpoch uint64
	from, to                int64
}

type optionalSection struct {
	desc sectionDesc
	data []byte
	slot int
}

func crc(raw []byte) uint32 { return crc32.Checksum(raw, castagnoli) }

func encodeSlot(s superblock) []byte {
	raw := make([]byte, slotSize)
	copy(raw[0:8], magic)
	binary.LittleEndian.PutUint16(raw[8:10], s.version)
	binary.LittleEndian.PutUint16(raw[10:12], s.flags)
	binary.LittleEndian.PutUint32(raw[12:16], s.compat)
	binary.LittleEndian.PutUint32(raw[16:20], s.roCompat)
	binary.LittleEndian.PutUint32(raw[20:24], s.incompat)
	binary.LittleEndian.PutUint64(raw[24:32], s.gen)
	binary.LittleEndian.PutUint64(raw[32:40], s.walOffset)
	binary.LittleEndian.PutUint64(raw[40:48], s.sealedKVRev)
	binary.LittleEndian.PutUint64(raw[48:56], s.sealedWalSeq)
	binary.LittleEndian.PutUint64(raw[56:64], uint64(s.watermark))
	binary.LittleEndian.PutUint64(raw[64:72], uint64(s.created))
	binary.LittleEndian.PutUint64(raw[72:80], uint64(s.compacted))
	copy(raw[80:96], s.uuid[:])
	copy(raw[136:200], s.user[:])
	for i, d := range s.sections {
		p := 200 + i*32
		binary.LittleEndian.PutUint16(raw[p:p+2], d.kind)
		raw[p+2] = d.codec
		raw[p+3] = d.flags
		binary.LittleEndian.PutUint32(raw[p+4:p+8], d.count)
		binary.LittleEndian.PutUint64(raw[p+8:p+16], d.offset)
		binary.LittleEndian.PutUint64(raw[p+16:p+24], d.length)
		binary.LittleEndian.PutUint32(raw[p+24:p+28], d.rawLen)
		binary.LittleEndian.PutUint32(raw[p+28:p+32], d.crc)
	}
	binary.LittleEndian.PutUint32(raw[slotSize-4:], crc(raw[:slotSize-4]))
	return raw
}

func decodeSlot(raw []byte) (superblock, bool) {
	if len(raw) != slotSize || string(raw[:8]) != magic || binary.LittleEndian.Uint16(raw[8:10]) != 1 || binary.LittleEndian.Uint32(raw[slotSize-4:]) != crc(raw[:slotSize-4]) {
		return superblock{}, false
	}
	var s superblock
	s.version = 1
	s.flags = binary.LittleEndian.Uint16(raw[10:12])
	s.compat = binary.LittleEndian.Uint32(raw[12:16])
	s.roCompat = binary.LittleEndian.Uint32(raw[16:20])
	s.incompat = binary.LittleEndian.Uint32(raw[20:24])
	s.gen = binary.LittleEndian.Uint64(raw[24:32])
	s.walOffset = binary.LittleEndian.Uint64(raw[32:40])
	s.sealedKVRev = binary.LittleEndian.Uint64(raw[40:48])
	s.sealedWalSeq = binary.LittleEndian.Uint64(raw[48:56])
	s.watermark = int64(binary.LittleEndian.Uint64(raw[56:64]))
	s.created = int64(binary.LittleEndian.Uint64(raw[64:72]))
	s.compacted = int64(binary.LittleEndian.Uint64(raw[72:80]))
	copy(s.uuid[:], raw[80:96])
	copy(s.user[:], raw[136:200])
	for i := range s.sections {
		p := 200 + i*32
		s.sections[i] = sectionDesc{kind: binary.LittleEndian.Uint16(raw[p : p+2]), codec: raw[p+2], flags: raw[p+3], count: binary.LittleEndian.Uint32(raw[p+4 : p+8]), offset: binary.LittleEndian.Uint64(raw[p+8 : p+16]), length: binary.LittleEndian.Uint64(raw[p+16 : p+24]), rawLen: binary.LittleEndian.Uint32(raw[p+24 : p+28]), crc: binary.LittleEndian.Uint32(raw[p+28 : p+32])}
	}
	return s, true
}

func initialSuper() superblock {
	var u [16]byte
	binary.LittleEndian.PutUint64(u[:8], rand.Uint64())
	binary.LittleEndian.PutUint64(u[8:], rand.Uint64())
	if u == [16]byte{} {
		copy(u[:], []byte("strata-phase1a"))
	}
	now := time.Now().Unix()
	return superblock{version: 1, gen: 1, walOffset: dataStart, created: now, uuid: u}
}

func readSuper(f *osFile) (superblock, error) {
	raw := make([]byte, headerSize)
	if _, err := f.ReadAt(raw, 0); err != nil {
		return superblock{}, err
	}
	var best superblock
	ok := false
	for _, slot := range [][]byte{raw[:slotSize], raw[slotSize:]} {
		if s, valid := decodeSlot(slot); valid && (!ok || s.gen > best.gen) {
			best = s
			ok = true
		}
	}
	if !ok {
		return superblock{}, errors.New("strata: invalid superblock")
	}
	if best.walOffset < dataStart {
		return superblock{}, errors.New("strata: invalid WAL offset")
	}
	if best.incompat&^knownIncompat != 0 {
		return superblock{}, ErrIncompatFormat
	}
	return best, nil
}

func writeSupers(f *osFile, s superblock) error {
	raw := encodeSlot(s)
	if _, err := f.WriteAt(raw, 0); err != nil {
		return err
	}
	if _, err := f.WriteAt(raw, slotSize); err != nil {
		return err
	}
	return nil
}

func validateSections(f *osFile, s superblock) ([]optionalSection, error) {
	var out []optionalSection
	for i, d := range s.sections {
		if d.length == 0 {
			continue
		}
		if d.kind <= 6 {
			continue
		}
		if d.offset < dataStart || d.length > uint64(^uint64(0))-d.offset {
			return nil, errors.New("strata: invalid section bounds")
		}
		raw := make([]byte, d.length)
		if _, err := f.ReadAt(raw, int64(d.offset)); err != nil {
			return nil, err
		}
		if crc(raw) != d.crc {
			return nil, errors.New("strata: section CRC mismatch")
		}
		if d.flags&secFlagOptional == 0 {
			return nil, ErrIncompatFormat
		}
		out = append(out, optionalSection{desc: d, data: raw, slot: i})
	}
	return out, nil
}
