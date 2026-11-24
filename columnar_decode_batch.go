package strata

import "encoding/binary"

func decodeTagRange(dst []uint32, col decodedCol, lo int) {
	n := len(dst)
	if n == 0 {
		return
	}
	_ = dst[n-1]
	switch col.mode {
	case modeZero:
		clear(dst)
	case modeConst:
		v := uint32(col.val)
		if v == 0 {
			clear(dst)
			return
		}
		for i := range dst {
			dst[i] = v
		}
	case modeU8:
		src := col.data[lo : lo+n]
		_ = src[n-1]
		for i, b := range src {
			dst[i] = uint32(b)
		}
	case modeU16:
		src := col.data[lo*2 : (lo+n)*2]
		_ = src[n*2-1]
		for i := range n {
			o := i * 2
			dst[i] = uint32(binary.LittleEndian.Uint16(src[o : o+2]))
		}
	default:
		src := col.data[lo*4 : (lo+n)*4]
		_ = src[n*4-1]
		for i := range n {
			o := i * 4
			dst[i] = binary.LittleEndian.Uint32(src[o : o+4])
		}
	}
}

func decodeTagIndexed(dst []uint32, col decodedCol, rows []int32) {
	n := len(rows)
	if n == 0 {
		return
	}
	dst = dst[:n]
	_ = dst[n-1]
	switch col.mode {
	case modeZero:
		clear(dst)
	case modeConst:
		v := uint32(col.val)
		if v == 0 {
			clear(dst)
			return
		}
		for i := range dst {
			dst[i] = v
		}
	case modeU8:
		data := col.data
		for i, r := range rows {
			dst[i] = uint32(data[r])
		}
	case modeU16:
		data := col.data
		for i, r := range rows {
			o := int(r) * 2
			dst[i] = uint32(binary.LittleEndian.Uint16(data[o : o+2]))
		}
	default:
		data := col.data
		for i, r := range rows {
			o := int(r) * 4
			dst[i] = binary.LittleEndian.Uint32(data[o : o+4])
		}
	}
}

func decodeValRange(dst []int64, col decodedCol, lo int) {
	n := len(dst)
	if n == 0 {
		return
	}
	_ = dst[n-1]
	switch col.mode {
	case modeZero:
		clear(dst)
	case modeConst:
		v := int64(col.val)
		if v == 0 {
			clear(dst)
			return
		}
		for i := range dst {
			dst[i] = v
		}
	case modeU8:
		src := col.data[lo : lo+n]
		_ = src[n-1]
		for i, b := range src {
			dst[i] = int64(b)
		}
	case modeU16:
		src := col.data[lo*2 : (lo+n)*2]
		_ = src[n*2-1]
		for i := range n {
			o := i * 2
			dst[i] = int64(binary.LittleEndian.Uint16(src[o : o+2]))
		}
	case modeU32:
		src := col.data[lo*4 : (lo+n)*4]
		_ = src[n*4-1]
		for i := range n {
			o := i * 4
			dst[i] = int64(binary.LittleEndian.Uint32(src[o : o+4]))
		}
	default:
		src := col.data[lo*8 : (lo+n)*8]
		_ = src[n*8-1]
		for i := range n {
			o := i * 8
			dst[i] = int64(binary.LittleEndian.Uint64(src[o : o+8]))
		}
	}
}

func decodeValIndexed(dst []int64, col decodedCol, rows []int32) {
	n := len(rows)
	if n == 0 {
		return
	}
	dst = dst[:n]
	_ = dst[n-1]
	switch col.mode {
	case modeZero:
		clear(dst)
	case modeConst:
		v := int64(col.val)
		if v == 0 {
			clear(dst)
			return
		}
		for i := range dst {
			dst[i] = v
		}
	case modeU8:
		data := col.data
		for i, r := range rows {
			dst[i] = int64(data[r])
		}
	case modeU16:
		data := col.data
		for i, r := range rows {
			o := int(r) * 2
			dst[i] = int64(binary.LittleEndian.Uint16(data[o : o+2]))
		}
	case modeU32:
		data := col.data
		for i, r := range rows {
			o := int(r) * 4
			dst[i] = int64(binary.LittleEndian.Uint32(data[o : o+4]))
		}
	default:
		data := col.data
		for i, r := range rows {
			o := int(r) * 8
			dst[i] = int64(binary.LittleEndian.Uint64(data[o : o+8]))
		}
	}
}

func decodeMaskRange(dst []uint8, col decodedCol, lo int) {
	n := len(dst)
	if n == 0 {
		return
	}
	if col.mode == modeConst {
		v := byte(col.val)
		for i := range dst {
			dst[i] = v
		}
		return
	}
	copy(dst, col.data[lo:lo+n])
}

func decodeMaskIndexed(dst []uint8, col decodedCol, rows []int32) {
	n := len(rows)
	if n == 0 {
		return
	}
	dst = dst[:n]
	if col.mode == modeConst {
		v := byte(col.val)
		for i := range dst {
			dst[i] = v
		}
		return
	}
	_ = dst[n-1]
	data := col.data
	for i, r := range rows {
		dst[i] = data[r]
	}
}
