package strata

import (
	"encoding/binary"
	"errors"
	"sort"
	"strings"
	"unsafe"

	"github.com/klauspost/compress/zstd"
)

// dictPageDesc is a 16-byte pointer-free (noscan) descriptor for one external
// forward dictionary page or reverse SST block.
type dictPageDesc struct {
	relOff uint64
	length uint32
	rawLen uint32 // 0 means stored uncompressed; >0 means zstd-compressed
}

// dictExtDim holds immutable directory metadata for one high-cardinality external
// dictionary dimension backed by SecTSDict codec=2.
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

func (e *dictExtDim) readPageInto(areaStart uint64, desc dictPageDesc, plainBuf *[]byte, dec *zstd.Decoder) ([]byte, error) {
	if e == nil || e.view == nil || e.view.data == nil {
		return nil, errors.New("strata: missing file view for external dict")
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
	var dst []byte
	if plainBuf != nil {
		dst = (*plainBuf)[:0]
	} else {
		dst = make([]byte, 0, desc.rawLen)
	}
	plain, err := dec.DecodeAll(raw, dst)
	if err != nil {
		return nil, err
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
	if lo == hi || hdrEnd+int(hi) > len(plain) {
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
	if lo == hi || hdrEnd+int(hi) > len(plain) {
		return nil
	}
	return plain[hdrEnd+int(lo) : hdrEnd+int(hi)]
}

func (e *dictExtDim) resolve(id uint32) string {
	if e == nil || id == 0 || id >= e.baseNextID {
		return ""
	}
	pageIdx := int(id-1) / dictFwdPageSize
	slot := int(id-1) % dictFwdPageSize
	if pageIdx >= len(e.fwdPages) {
		return ""
	}
	ck := dictCacheKey{series: e.series, dim: e.dim, page: uint32(pageIdx)}
	if page, ok := e.cache.get(ck); ok {
		return decodeFwdPageAt(page, slot)
	}
	dec := getZstdDecoder()
	plain, err := e.readPageInto(e.fwdStartOff, e.fwdPages[pageIdx], nil, dec)
	putZstdDecoder(dec)
	if err != nil {
		return ""
	}
	e.cache.put(ck, plain)
	return decodeFwdPageAt(plain, slot)
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

func (e *dictExtDim) lookupSST(value string) uint32 {
	if e == nil || len(e.sstBlocks) == 0 || value == "" {
		return 0
	}
	if value < e.sstFirstKey(0) {
		return 0
	}
	idx := sort.Search(len(e.sstBlocks), func(i int) bool {
		return e.sstFirstKey(i) > value
	}) - 1
	if idx < 0 {
		return 0
	}
	dec := getZstdDecoder()
	defer putZstdDecoder(dec)
	plainPtr := getZstdScratch()
	defer func() {
		putZstdScratch(plainPtr, *plainPtr)
	}()
	plain, err := e.readPageInto(e.sstStartOff, e.sstBlocks[idx], plainPtr, dec)
	if err != nil {
		return 0
	}
	var keyBuf []byte
	var found uint32
	_ = decodeSSTBlock(plain, &keyBuf, func(key []byte, id uint32) bool {
		s := unsafe.String(unsafe.SliceData(key), len(key))
		if s < value {
			return true
		}
		if s == value {
			found = id
		}
		return false
	})
	return found
}

func (e *dictExtDim) matchPrefixSST(prefix string, extra map[uint32]string, dst []uint32) []uint32 {
	if e == nil || len(e.sstBlocks) == 0 {
		return dst
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
	dec := getZstdDecoder()
	defer putZstdDecoder(dec)
	plainPtr := getZstdScratch()
	defer func() {
		putZstdScratch(plainPtr, *plainPtr)
	}()
	var keyBuf []byte
	for blk := startIdx; blk < len(e.sstBlocks); blk++ {
		if blk > startIdx && prefix != "" {
			fk := e.sstFirstKey(blk)
			if fk > prefix && !strings.HasPrefix(fk, prefix) {
				break
			}
		}
		plain, err := e.readPageInto(e.sstStartOff, e.sstBlocks[blk], plainPtr, dec)
		if err != nil {
			break
		}
		stopped := false
		_ = decodeSSTBlock(plain, &keyBuf, func(key []byte, id uint32) bool {
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
		if stopped {
			break
		}
	}
	return dst
}

func (e *dictExtDim) matchScanFwd(op TagOp, target string, extra map[uint32]string, dst []uint32) []uint32 {
	if e == nil || len(e.fwdPages) == 0 {
		return dst
	}
	dec := getZstdDecoder()
	defer putZstdDecoder(dec)
	plainPtr := getZstdScratch()
	defer func() {
		putZstdScratch(plainPtr, *plainPtr)
	}()
	for pIdx, pDesc := range e.fwdPages {
		plain, err := e.readPageInto(e.fwdStartOff, pDesc, plainPtr, dec)
		if err != nil {
			break
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
	return dst
}
