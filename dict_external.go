package strata

import (
	"encoding/binary"
	"errors"
	"sort"
	"strings"
	"unsafe"
)

const maxDictPageRawBytes = 16 << 20

// dictPageDesc is a 16-byte pointer-free (noscan) descriptor for one external
// forward dictionary page or reverse SST block.
type dictPageDesc struct {
	relOff uint64
	length uint32
	rawLen uint32 // 0 means stored uncompressed; >0 means compressed
}

// dictExtDim holds immutable directory metadata for one high-cardinality external
// dictionary dimension backed by SecTSDict.
type dictExtDim struct {
	series        uint16
	dim           uint8
	baseNextID    uint32
	baseCount     uint32
	secDesc       sectionDesc
	fwdStartOff   uint64
	fwdTotalBytes uint64
	fwdPages      []dictPageDesc
	sstStartOff   uint64
	sstTotalBytes uint64
	sstFirstArena []byte
	sstFirstOffs  []uint32
	sstBlocks     []dictPageDesc
	view          *fileView
	cache         *dictPageCache
}

func (e *dictExtDim) sstFirstKey(idx int) string {
	lo, hi := e.sstFirstOffs[idx], e.sstFirstOffs[idx+1]
	if lo == hi {
		return ""
	}
	return unsafe.String(&e.sstFirstArena[lo], hi-lo)
}

func (e *dictExtDim) readPageInto(areaStart uint64, desc dictPageDesc, plainBuf *[]byte, slot *querySlot) ([]byte, error) {
	if desc.length == 0 {
		if plainBuf != nil {
			*plainBuf = (*plainBuf)[:0]
		}
		return nil, nil
	}
	if e == nil || e.view == nil || e.view.data == nil {
		return nil, errors.New("strata: missing file view for external dict")
	}
	if desc.rawLen > maxDictPageRawBytes {
		return nil, errors.New("strata: invalid dictionary page length")
	}
	absOff := e.secDesc.offset + areaStart + desc.relOff
	raw, bufPtr, err := sectionBlockPooled(e.view.data, e.secDesc, absOff, desc.length, e.view.mappedBytes())
	if bufPtr != nil {
		defer putReadBuf(bufPtr)
	}
	if err != nil {
		return nil, err
	}
	if desc.rawLen == 0 {
		if plainBuf != nil {
			*plainBuf = append((*plainBuf)[:0], raw...)
			return *plainBuf, nil
		}
		out := make([]byte, len(raw))
		copy(out, raw)
		return out, nil
	}
	var dst, plain []byte
	if plainBuf != nil {
		dst = (*plainBuf)[:0]
	}
	if slot == nil {
		plain, err = e.view.codecs.decode(e.secDesc.codec, dst, raw, int(desc.rawLen))
	} else {
		plain, err = slot.decodeInto(e.secDesc.codec, raw, dst, int(desc.rawLen))
	}
	if err != nil {
		return nil, err
	}
	if len(plain) != int(desc.rawLen) {
		return nil, errors.New("strata: invalid dictionary page length")
	}
	if plainBuf != nil {
		*plainBuf = plain
	}
	return plain, nil
}

func decodeFwdPageCount(plain []byte) int {
	if len(plain) < 2 {
		return 0
	}
	return int(binary.LittleEndian.Uint16(plain[0:2]))
}

func decodeFwdPageAt(plain []byte, slot int) string {
	count := decodeFwdPageCount(plain)
	if slot < 0 || slot >= count {
		return ""
	}
	hdrEnd := 2 + (count+1)*4
	if hdrEnd > len(plain) {
		return ""
	}
	lo := binary.LittleEndian.Uint32(plain[2+slot*4 : 6+slot*4])
	hi := binary.LittleEndian.Uint32(plain[2+(slot+1)*4 : 6+(slot+1)*4])
	if lo >= hi || uint64(hi) > uint64(len(plain)-hdrEnd) {
		return ""
	}
	return string(plain[hdrEnd+int(lo) : hdrEnd+int(hi)])
}

func decodeFwdPageSlice(plain []byte, slot int) []byte {
	count := decodeFwdPageCount(plain)
	if slot < 0 || slot >= count {
		return nil
	}
	hdrEnd := 2 + (count+1)*4
	if hdrEnd > len(plain) {
		return nil
	}
	lo := binary.LittleEndian.Uint32(plain[2+slot*4 : 6+slot*4])
	hi := binary.LittleEndian.Uint32(plain[2+(slot+1)*4 : 6+(slot+1)*4])
	if lo >= hi || uint64(hi) > uint64(len(plain)-hdrEnd) {
		return nil
	}
	return plain[hdrEnd+int(lo) : hdrEnd+int(hi)]
}

func (e *dictExtDim) resolve(id uint32) (string, error) {
	if e == nil || id == 0 || id >= e.baseNextID {
		return "", nil
	}
	pageIdx := int(id-1) / dictFwdPageSize
	slot := int(id-1) % dictFwdPageSize
	if pageIdx >= len(e.fwdPages) || e.fwdPages[pageIdx].length == 0 {
		return "", nil
	}
	ck := dictCacheKey{series: e.series, dim: e.dim, page: uint32(pageIdx)}
	if page, ok := e.cache.get(ck); ok {
		return decodeFwdPageAt(page, slot), nil
	}
	plain, err := e.readPageInto(e.fwdStartOff, e.fwdPages[pageIdx], nil, nil)
	if err != nil {
		return "", err
	}
	e.cache.put(ck, plain)
	return decodeFwdPageAt(plain, slot), nil
}

func decodeSSTBlock(plain []byte, keyBuf *[]byte, yield func(key []byte, id uint32) bool) error {
	off := 0
	n, err := readDictUvarint(plain, &off)
	if err != nil {
		return err
	}
	buf := (*keyBuf)[:0]
	for range n {
		shared, err := readDictUvarint(plain, &off)
		if err != nil || shared > uint64(len(buf)) {
			return errors.New("strata: corrupt dict SST shared prefix")
		}
		suffixLen, err := readDictUvarint(plain, &off)
		if err != nil || suffixLen > uint64(len(plain)-off) {
			return errors.New("strata: corrupt dict SST suffix")
		}
		buf = append(buf[:shared], plain[off:off+int(suffixLen)]...)
		off += int(suffixLen)
		id, err := readDictUvarint(plain, &off)
		if err != nil || id > uint64(^uint32(0)) {
			return errors.New("strata: corrupt dict SST id")
		}
		if !yield(buf, uint32(id)) {
			break
		}
	}
	*keyBuf = buf
	return nil
}

func (e *dictExtDim) lookupSST(value string, slot *querySlot) (uint32, error) {
	if e == nil || len(e.sstBlocks) == 0 || value == "" {
		return 0, nil
	}
	if value < e.sstFirstKey(0) {
		return 0, nil
	}
	idx := sort.Search(len(e.sstBlocks), func(i int) bool {
		return e.sstFirstKey(i) > value
	}) - 1
	if idx < 0 {
		return 0, nil
	}
	plainPtr := getDecodeScratch()
	defer func() {
		putDecodeScratch(plainPtr, *plainPtr)
	}()
	plain, err := e.readPageInto(e.sstStartOff, e.sstBlocks[idx], plainPtr, slot)
	if err != nil {
		return 0, err
	}
	var keyBuf []byte
	var found uint32
	err = decodeSSTBlock(plain, &keyBuf, func(key []byte, id uint32) bool {
		s := unsafe.String(unsafe.SliceData(key), len(key))
		if s < value {
			return true
		}
		if s == value {
			found = id
		}
		return false
	})
	return found, err
}

func (e *dictExtDim) matchPrefixSST(prefix string, extra map[uint32]string, dst []uint32, slot *querySlot) ([]uint32, error) {
	if e == nil || len(e.sstBlocks) == 0 {
		return dst, nil
	}
	startIdx := 0
	if prefix != "" {
		idx := sort.Search(len(e.sstBlocks), func(i int) bool {
			return e.sstFirstKey(i) >= prefix
		})
		if idx > 0 {
			startIdx = idx - 1
		}
	}
	if slot == nil {
		local := querySlot{codecs: e.view.codecs}
		defer local.close()
		slot = &local
	}
	plainPtr := getDecodeScratch()
	defer func() {
		putDecodeScratch(plainPtr, *plainPtr)
	}()
	var keyBuf []byte
	for blk := startIdx; blk < len(e.sstBlocks); blk++ {
		if blk > startIdx && prefix != "" {
			fk := e.sstFirstKey(blk)
			if fk > prefix && !strings.HasPrefix(fk, prefix) {
				break
			}
		}
		plain, err := e.readPageInto(e.sstStartOff, e.sstBlocks[blk], plainPtr, slot)
		if err != nil {
			return nil, err
		}
		stopped := false
		err = decodeSSTBlock(plain, &keyBuf, func(key []byte, id uint32) bool {
			s := unsafe.String(unsafe.SliceData(key), len(key))
			if s < prefix {
				return true
			}
			if prefix != "" && !strings.HasPrefix(s, prefix) {
				stopped = true
				return false
			}
			if extra != nil {
				if _, overridden := extra[id]; overridden {
					return true
				}
			}
			dst = append(dst, id)
			return true
		})
		if err != nil {
			return nil, err
		}
		if stopped {
			break
		}
	}
	return dst, nil
}

func (e *dictExtDim) matchScanFwd(op TagOp, target string, extra map[uint32]string, dst []uint32, slot *querySlot) ([]uint32, error) {
	if e == nil || len(e.fwdPages) == 0 {
		return dst, nil
	}
	var local querySlot
	if slot == nil {
		local.codecs = e.view.codecs
		slot = &local
		defer local.close()
	}
	plainPtr := getDecodeScratch()
	defer func() {
		putDecodeScratch(plainPtr, *plainPtr)
	}()
	for pIdx, pDesc := range e.fwdPages {
		if pDesc.length == 0 {
			continue
		}
		plain, err := e.readPageInto(e.fwdStartOff, pDesc, plainPtr, slot)
		if err != nil {
			return nil, err
		}
		count := decodeFwdPageCount(plain)
		baseID := uint32(pIdx*dictFwdPageSize + 1)
		for slot := range count {
			id := baseID + uint32(slot)
			if extra != nil {
				if _, overridden := extra[id]; overridden {
					continue
				}
			}
			raw := decodeFwdPageSlice(plain, slot)
			if len(raw) == 0 {
				continue
			}
			s := unsafe.String(unsafe.SliceData(raw), len(raw))
			if tagMatches(op, s, target) {
				dst = append(dst, id)
			}
		}
	}
	return dst, nil
}
