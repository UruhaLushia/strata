package strata

const aggTileRows = 128

func applyMetricOp(dst *int64, v int64, op MetricOp) {
	switch op {
	case OpMin:
		if v < *dst {
			*dst = v
		}
	case OpMax:
		if v > *dst {
			*dst = v
		}
	case OpLast:
		*dst = v
	default:
		*dst += v
	}
}

func aggregateRangeDenseAllSum(cols *[streamColumnCount]decodedCol, lo, hi, dim int, g *aggregateGroups) {
	tagCol := cols[dim]
	c0, c1, c2, c3 := cols[8], cols[9], cols[10], cols[11]
	dense := g.dense
	denseLen := uint32(len(dense))
	var tagBuf [aggTileRows]uint32
	var v0Buf, v1Buf, v2Buf, v3Buf [aggTileRows]int64
	for base := lo; base < hi; base += aggTileRows {
		n := min(aggTileRows, hi-base)
		decodeTagRange(tagBuf[:n], tagCol, base)
		decodeValRange(v0Buf[:n], c0, base)
		decodeValRange(v1Buf[:n], c1, base)
		decodeValRange(v2Buf[:n], c2, base)
		decodeValRange(v3Buf[:n], c3, base)
		for i := range n {
			id := tagBuf[i]
			if id >= denseLen {
				continue
			}
			dst := &dense[id]
			dst.Mask = 0x0f
			dst.Values[0] += v0Buf[i]
			dst.Values[1] += v1Buf[i]
			dst.Values[2] += v2Buf[i]
			dst.Values[3] += v3Buf[i]
		}
	}
}

func aggregateRangeDenseMixed(cols *[streamColumnCount]decodedCol, lo, hi, slotIdx, orderOffset, dim int, g *aggregateGroups, ops [4]MetricOp) {
	tagCol := cols[dim]
	c0, c1, c2, c3 := cols[8], cols[9], cols[10], cols[11]
	dense := g.dense
	denseLen := uint32(len(dense))
	lastCount := g.lastCount
	lastOrd := g.denseLastOrder
	multiOrd := g.denseOrder
	op0, op1, op2, op3 := ops[0], ops[1], ops[2], ops[3]
	slotBase := uint64(slotIdx+1) << 32
	var tagBuf [aggTileRows]uint32
	var v0Buf, v1Buf, v2Buf, v3Buf [aggTileRows]int64
	for pos, base := 0, lo; base < hi; pos, base = pos+aggTileRows, base+aggTileRows {
		n := min(aggTileRows, hi-base)
		decodeTagRange(tagBuf[:n], tagCol, base)
		decodeValRange(v0Buf[:n], c0, base)
		decodeValRange(v1Buf[:n], c1, base)
		decodeValRange(v2Buf[:n], c2, base)
		decodeValRange(v3Buf[:n], c3, base)
		baseOrd := slotBase | uint64(orderOffset+pos+1)
		for i := range n {
			id := tagBuf[i]
			if id >= denseLen {
				continue
			}
			dst := &dense[id]
			x0, x1, x2, x3 := v0Buf[i], v1Buf[i], v2Buf[i], v3Buf[i]
			ord := baseOrd + uint64(i)
			switch dst.Mask {
			case 0x0f:
				applyMetricOp(&dst.Values[0], x0, op0)
				applyMetricOp(&dst.Values[1], x1, op1)
				applyMetricOp(&dst.Values[2], x2, op2)
				applyMetricOp(&dst.Values[3], x3, op3)
				if lastCount == 1 {
					lastOrd[id] = ord
				} else if lastCount >= 2 {
					multiOrd[id] = [4]uint64{ord, ord, ord, ord}
				}
			case 0:
				dst.Mask = 0x0f
				dst.Values = [4]int64{x0, x1, x2, x3}
				if lastCount == 1 {
					lastOrd[id] = ord
				} else if lastCount >= 2 {
					multiOrd[id] = [4]uint64{ord, ord, ord, ord}
				}
			default:
				g.add(id, Summary{Values: [4]int64{x0, x1, x2, x3}, Mask: 0x0f}, ops, ord)
			}
		}
	}
}
