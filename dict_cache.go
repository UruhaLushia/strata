package strata

import "sync"

const (
	highCardThreshold  = 16384
	dictFwdPageSize    = 512
	dictSSTTargetBytes = 4 << 10
	dictHotSlots       = 4096
)

var dictMagic = [4]byte{'S', 'D', 'T', '1'}

type dictCacheKey struct {
	series uint16
	dim    uint8
	_      uint8
	page   uint32
}

type dictCacheEntry struct {
	key  dictCacheKey
	data []byte
}

// dictPageCache is a thread-safe, byte-bounded FIFO cache of immutable decompressed
// forward dictionary pages. Returned slices are never mutated in place.
type dictPageCache struct {
	mu        sync.Mutex
	maxBytes  int
	usedBytes int
	entries   []dictCacheEntry
	index     map[dictCacheKey]int
}

func newDictPageCache(maxBytes int) *dictPageCache {
	if maxBytes <= 0 {
		maxBytes = 256 << 10
	}
	return &dictPageCache{maxBytes: maxBytes}
}

func (c *dictPageCache) get(k dictCacheKey) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	idx, ok := c.index[k]
	if !ok {
		c.mu.Unlock()
		return nil, false
	}
	data := c.entries[idx].data
	c.mu.Unlock()
	return data, true
}

func (c *dictPageCache) put(k dictCacheKey, data []byte) {
	if c == nil || len(data) == 0 || len(data) > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.index[k]; ok {
		return
	}
	if c.index == nil {
		c.index = make(map[dictCacheKey]int, 32)
	}
	for c.usedBytes+len(data) > c.maxBytes && len(c.entries) > 0 {
		victim := c.entries[0]
		c.usedBytes -= len(victim.data)
		delete(c.index, victim.key)
		copy(c.entries, c.entries[1:])
		c.entries[len(c.entries)-1] = dictCacheEntry{}
		c.entries = c.entries[:len(c.entries)-1]
		for ek, eidx := range c.index {
			c.index[ek] = eidx - 1
		}
	}
	if len(c.entries) == 0 && cap(c.entries) > 64 {
		c.entries = nil
	}
	c.index[k] = len(c.entries)
	c.entries = append(c.entries, dictCacheEntry{key: k, data: data})
	c.usedBytes += len(data)
}

func (c *dictPageCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.entries = nil
	c.index = nil
	c.usedBytes = 0
	c.mu.Unlock()
}

// dictHotSlot is a 16-byte pointer-free (noscan) open-addressing slot referencing
// key bytes stored inside dictHotCache.ring.
type dictHotSlot struct {
	hash uint64
	id   uint32
	off  uint32
	ln   uint16
	gen  uint16
}

// dictHotCache is a fixed-budget, pointer-free Tier-1 write deduplication cache
// for external high-cardinality dimensions.
type dictHotCache struct {
	slots   []dictHotSlot
	ring    []byte
	ringPos uint32
	ringGen uint16
	clock   uint32
}

func newDictHotCache(budgetBytes int) *dictHotCache {
	slotBytes := dictHotSlots * 20
	ringSize := max(budgetBytes-slotBytes, 64<<10)
	return &dictHotCache{
		slots:   make([]dictHotSlot, dictHotSlots),
		ring:    make([]byte, ringSize),
		ringGen: 1,
	}
}

func hashDictKey64(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	if h == 0 {
		return 1
	}
	return h
}

func (c *dictHotCache) validSlot(sl dictHotSlot) bool {
	if sl.hash == 0 || int(sl.off)+int(sl.ln) > len(c.ring) {
		return false
	}
	if sl.gen == c.ringGen {
		return sl.off+uint32(sl.ln) <= c.ringPos
	}
	return c.ringGen > 1 && sl.gen == c.ringGen-1 && sl.off >= c.ringPos
}

func (c *dictHotCache) get(value string) uint32 {
	if c == nil || len(c.slots) == 0 || len(value) == 0 {
		return 0
	}
	h := hashDictKey64(value)
	mask := uint32(len(c.slots) - 1)
	base := uint32(h) & mask
	for step := range uint32(8) {
		sl := c.slots[(base+step)&mask]
		if sl.hash == 0 {
			return 0
		}
		if sl.hash == h && int(sl.ln) == len(value) && c.validSlot(sl) &&
			string(c.ring[sl.off:sl.off+uint32(sl.ln)]) == value {
			return sl.id
		}
	}
	return 0
}

func (c *dictHotCache) put(value string, id uint32) {
	if c == nil || len(c.slots) == 0 || id == 0 || len(value) == 0 || len(value) > 4096 || len(value) > len(c.ring)/2 {
		return
	}
	h := hashDictKey64(value)
	mask := uint32(len(c.slots) - 1)
	base := uint32(h) & mask
	freeIdx := -1
	for step := range uint32(8) {
		idx := (base + step) & mask
		sl := c.slots[idx]
		if sl.hash == 0 || !c.validSlot(sl) {
			if freeIdx < 0 {
				freeIdx = int(idx)
			}
			if sl.hash == 0 {
				break
			}
			continue
		}
		if sl.hash == h && int(sl.ln) == len(value) && string(c.ring[sl.off:sl.off+uint32(sl.ln)]) == value {
			c.slots[idx].id = id
			return
		}
	}
	if int(c.ringPos)+len(value) > len(c.ring) {
		c.ringPos = 0
		c.ringGen++
		if c.ringGen == 0 {
			clear(c.slots)
			c.ringGen = 1
		}
	}
	off := c.ringPos
	copy(c.ring[off:], value)
	c.ringPos += uint32(len(value))
	target := freeIdx
	if target < 0 {
		c.clock = (c.clock + 1) & 7
		target = int((base + c.clock) & mask)
	}
	c.slots[target] = dictHotSlot{
		hash: h, id: id, off: off, ln: uint16(len(value)), gen: c.ringGen,
	}
}

func (dim *dictDim) cacheHot(value string, id uint32, budget int) {
	dim.revMu.Lock()
	if dim.hot == nil {
		dim.hot = newDictHotCache(budget)
	}
	dim.hot.put(value, id)
	dim.revMu.Unlock()
}

func (dim *dictDim) lookupExt(value string) (uint32, bool, error) {
	for id, v := range dim.extra {
		if v == value {
			return id, false, nil
		}
	}
	if id := dim.tailByKey[value]; id != 0 && !hasExtraOverride(dim.extra, id) {
		return id, false, nil
	}
	dim.revMu.RLock()
	id := dim.hot.get(value)
	dim.revMu.RUnlock()
	if id != 0 && !hasExtraOverride(dim.extra, id) {
		return id, false, nil
	}
	id, err := dim.ext.lookupSST(value, nil)
	if err != nil {
		return 0, false, err
	}
	if id != 0 && !hasExtraOverride(dim.extra, id) {
		return id, true, nil
	}
	return 0, false, nil
}

func (d *dictionary) internExtTail(dim *dictDim, value string) (uint32, bool, error) {
	if id := dim.tailByKey[value]; id != 0 && !hasExtraOverride(dim.extra, id) {
		return id, false, nil
	}
	id := dim.nextID
	if err := dim.appendValue(id, value); err != nil {
		return 0, false, err
	}
	if dim.tailByKey == nil {
		dim.tailByKey = make(map[string]uint32)
	}
	dim.tailByKey[value] = id
	dim.cacheHot(value, id, d.hotBudget)
	return id, true, nil
}

func (d *dictionary) reclaim() {
	d.pageCache.clear()
	for _, dim := range d.dims {
		if dim != nil && dim.ext != nil {
			dim.revMu.Lock()
			dim.hot = nil
			dim.revMu.Unlock()
		}
	}
}
