package strata

// openAddrSlot is a 40-byte pointer-free (noscan) open-addressing hash entry.
type openAddrSlot struct {
	values [4]int64
	key    uint32 // tagID + 1; 0 means empty
	mask   uint8
	_      [3]byte
}

type openAddrGroups struct {
	slots      []openAddrSlot
	lastOrders []uint64
	orders     [][4]uint64
	count      int
	lastCount  uint8
}

func (t *openAddrGroups) init(capHint int, lastCount uint8) {
	n := 64
	for n < capHint {
		n <<= 1
	}
	t.slots = make([]openAddrSlot, n)
	if lastCount == 1 {
		t.lastOrders = make([]uint64, n)
		t.orders = nil
	} else if lastCount >= 2 {
		t.orders = make([][4]uint64, n)
		t.lastOrders = nil
	}
	t.count = 0
	t.lastCount = lastCount
}

func mixTagKey(k uint32) uint32 {
	k ^= k >> 16
	k *= 0x7feb352d
	k ^= k >> 15
	k *= 0x846ca68b
	k ^= k >> 16
	return k
}

func (t *openAddrGroups) grow() {
	oldSlots := t.slots
	oldLastOrders := t.lastOrders
	oldOrders := t.orders
	newLen := max(len(oldSlots)*2, 64)
	t.slots = make([]openAddrSlot, newLen)
	if t.lastCount == 1 {
		t.lastOrders = make([]uint64, newLen)
	} else if t.lastCount >= 2 {
		t.orders = make([][4]uint64, newLen)
	}
	mask := uint32(newLen - 1)
	for i := range oldSlots {
		k := oldSlots[i].key
		if k == 0 {
			continue
		}
		idx := mixTagKey(k) & mask
		for t.slots[idx].key != 0 {
			idx = (idx + 1) & mask
		}
		t.slots[idx] = oldSlots[i]
		if t.lastCount == 1 {
			t.lastOrders[idx] = oldLastOrders[i]
		} else if t.lastCount >= 2 {
			t.orders[idx] = oldOrders[i]
		}
	}
}

func (t *openAddrGroups) findOrInsert(id uint32) int {
	if len(t.slots) == 0 {
		t.init(64, t.lastCount)
	} else if (t.count+1)*10 >= len(t.slots)*7 {
		t.grow()
	}
	k := id + 1
	mask := uint32(len(t.slots) - 1)
	idx := mixTagKey(k) & mask
	for {
		sk := t.slots[idx].key
		if sk == k {
			return int(idx)
		}
		if sk == 0 {
			t.slots[idx].key = k
			t.count++
			return int(idx)
		}
		idx = (idx + 1) & mask
	}
}
