package strata

import "sync"

const (
	maxReadBufCap        = 256 << 10
	maxDecodeScratchCap  = 512 << 10
	maxMatchIdxCap       = 32 << 10 // 32768 * 4B = 128 KiB
	maxRowBitWordsCap    = 16 << 10 // 16384 * 8B = 128 KiB (up to 1M rows)
	maxPooledPageRowCap  = 1024     // ~104 KiB
	maxPooledTagCap      = 8192     // ~128 KiB
	maxPooledSegSumCap   = 1024     // ~152 KiB
	maxPooledGroupRowCap = 4096     // ~224 KiB
	maxPoolDrainSteps    = 256
)

var (
	decodeScratchPool  sync.Pool
	readBufPool        sync.Pool
	matchIdxPool       sync.Pool
	rowBitsPool        sync.Pool
	pageRowSlicePool   sync.Pool
	tagSlicePool       sync.Pool
	segmentSummaryPool sync.Pool
	groupRowPool       sync.Pool
)

func getDecodeScratch() *[]byte {
	if v := decodeScratchPool.Get(); v != nil {
		return v.(*[]byte)
	}
	buf := make([]byte, 0, 64<<10)
	return &buf
}

func putDecodeScratch(p *[]byte, buf []byte) {
	if p == nil {
		return
	}
	if cap(buf) <= maxDecodeScratchCap {
		*p = buf[:0]
		decodeScratchPool.Put(p)
		return
	}
	*p = nil
}

func getReadBuf(n int) *[]byte {
	if v := readBufPool.Get(); v != nil {
		p := v.(*[]byte)
		if cap(*p) >= n {
			*p = (*p)[:n]
			return p
		}
		if cap(*p) <= maxReadBufCap {
			*p = make([]byte, n)
			return p
		}
	}
	buf := make([]byte, n)
	return &buf
}

func putReadBuf(p *[]byte) {
	if p == nil {
		return
	}
	if cap(*p) <= maxReadBufCap {
		*p = (*p)[:0]
		readBufPool.Put(p)
		return
	}
	*p = nil
}

func getMatchIdx(capHint int) *[]int32 {
	if v := matchIdxPool.Get(); v != nil {
		p := v.(*[]int32)
		*p = (*p)[:0]
		return p
	}
	if capHint > maxMatchIdxCap {
		capHint = 256
	}
	if capHint < 64 {
		capHint = 64
	}
	s := make([]int32, 0, capHint)
	return &s
}

func putMatchIdx(p *[]int32) {
	if p == nil {
		return
	}
	if cap(*p) <= maxMatchIdxCap {
		*p = (*p)[:0]
		matchIdxPool.Put(p)
		return
	}
	*p = nil
}

func getRowBits(words int) *[]uint64 {
	if v := rowBitsPool.Get(); v != nil {
		p := v.(*[]uint64)
		if cap(*p) >= words {
			s := (*p)[:words]
			clear(s)
			*p = s
			return p
		}
	}
	s := make([]uint64, words)
	return &s
}

func putRowBits(p *[]uint64) {
	if p == nil {
		return
	}
	if cap(*p) <= maxRowBitWordsCap {
		*p = (*p)[:0]
		rowBitsPool.Put(p)
		return
	}
	*p = nil
}

func getPageRowSlice(n int) []PageRow {
	if n <= 0 {
		return nil
	}
	if n <= maxPooledPageRowCap {
		if v := pageRowSlicePool.Get(); v != nil {
			p := v.(*[]PageRow)
			if cap(*p) >= n {
				return (*p)[:n]
			}
		}
	}
	return make([]PageRow, n)
}

func putPageRowSlice(s []PageRow) {
	if cap(s) == 0 {
		return
	}
	clear(s[:cap(s)])
	if cap(s) <= maxPooledPageRowCap {
		buf := s[:0]
		pageRowSlicePool.Put(&buf)
	}
}

func getTagSlice(n int) []string {
	if n <= 0 {
		return nil
	}
	if n <= maxPooledTagCap {
		if v := tagSlicePool.Get(); v != nil {
			p := v.(*[]string)
			if cap(*p) >= n {
				return (*p)[:n]
			}
		}
	}
	return make([]string, n)
}

func putTagSlice(s []string) {
	if cap(s) == 0 {
		return
	}
	clear(s[:cap(s)])
	if cap(s) <= maxPooledTagCap {
		buf := s[:0]
		tagSlicePool.Put(&buf)
	}
}

func getSegmentSummarySlice(capHint int) []SegmentSummary {
	if capHint <= maxPooledSegSumCap {
		if v := segmentSummaryPool.Get(); v != nil {
			p := v.(*[]SegmentSummary)
			if cap(*p) >= capHint {
				return (*p)[:0]
			}
		}
	}
	if capHint <= 0 {
		return nil
	}
	return make([]SegmentSummary, 0, capHint)
}

func putSegmentSummarySlice(s []SegmentSummary) {
	if cap(s) == 0 {
		return
	}
	clear(s[:cap(s)])
	if cap(s) <= maxPooledSegSumCap {
		buf := s[:0]
		segmentSummaryPool.Put(&buf)
	}
}

func getGroupRowSlice(capHint int) []GroupRow {
	if capHint <= maxPooledGroupRowCap {
		if v := groupRowPool.Get(); v != nil {
			p := v.(*[]GroupRow)
			if cap(*p) >= capHint {
				return (*p)[:0]
			}
		}
	}
	return make([]GroupRow, 0, capHint)
}

func putGroupRowSlice(s []GroupRow) {
	if cap(s) == 0 {
		return
	}
	clear(s[:cap(s)])
	if cap(s) <= maxPooledGroupRowCap {
		buf := s[:0]
		groupRowPool.Put(&buf)
	}
}

func drainPool(p *sync.Pool) {
	for range maxPoolDrainSteps {
		if p.Get() == nil {
			return
		}
	}
}

func reclaimPools() {
	drainPool(&decodeScratchPool)
	drainPool(&readBufPool)
	drainPool(&matchIdxPool)
	drainPool(&rowBitsPool)
	drainPool(&pageRowSlicePool)
	drainPool(&tagSlicePool)
	drainPool(&segmentSummaryPool)
	drainPool(&groupRowPool)
}
