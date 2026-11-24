package strata

func tagColumnModeRows(rows []idRow, dim int) (byte, uint32) {
	if len(rows) == 0 {
		return modeConst, 0
	}
	constant := rows[0].tags[dim]
	maxValue := constant
	for _, row := range rows[1:] {
		value := row.tags[dim]
		if value != constant {
			constant = 0
		}
		if value > maxValue {
			maxValue = value
		}
	}
	if constant != 0 || rows[0].tags[dim] == 0 && allTagRowsEqual(rows, dim) {
		return modeConst, rows[0].tags[dim]
	}
	switch {
	case maxValue <= uint32(^uint8(0)):
		return modeU8, 0
	case maxValue <= uint32(^uint16(0)):
		return modeU16, 0
	default:
		return modeU32, 0
	}
}

func valueColumnModeRows(rows []idRow, metric int) (byte, int64) {
	if len(rows) == 0 {
		return modeConst, 0
	}
	constant := rows[0].values[metric]
	maxValue := constant
	nonNegative := constant >= 0
	for _, row := range rows[1:] {
		value := row.values[metric]
		if value != constant {
			constant = 0
		}
		if value > maxValue {
			maxValue = value
		}
		if value < 0 {
			nonNegative = false
		}
	}
	if constant != 0 || rows[0].values[metric] == 0 && allValueRowsEqual(rows, metric) {
		if rows[0].values[metric] == 0 {
			return modeZero, 0
		}
		return modeConst, rows[0].values[metric]
	}
	if !nonNegative {
		return modeI64, 0
	}
	switch {
	case maxValue <= int64(^uint8(0)):
		return modeU8, 0
	case maxValue <= int64(^uint16(0)):
		return modeU16, 0
	case maxValue <= int64(^uint32(0)):
		return modeU32, 0
	default:
		return modeI64, 0
	}
}

func maskColumnMode(rows []idRow) (byte, byte) {
	if len(rows) == 0 {
		return modeConst, 0
	}
	mask := normalizeMask(rows[0].initMask)
	for _, row := range rows[1:] {
		if normalizeMask(row.initMask) != mask {
			return modeU8, 0
		}
	}
	return modeConst, mask
}

func allTagRowsEqual(rows []idRow, dim int) bool {
	for _, row := range rows[1:] {
		if row.tags[dim] != rows[0].tags[dim] {
			return false
		}
	}
	return true
}

func allValueRowsEqual(rows []idRow, metric int) bool {
	for _, row := range rows[1:] {
		if row.values[metric] != rows[0].values[metric] {
			return false
		}
	}
	return true
}
