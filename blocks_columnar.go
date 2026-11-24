package strata

import (
	"encoding/binary"
	"errors"
)

const (
	blockColumnarMarker        byte = 0xc1
	blockColumnarStreamsMarker byte = 0xc2
)

const (
	modeConst byte = iota
	modeU8
	modeU16
	modeU32
	modeI64
	modeZero
)

const streamColumnCount = 13

type columnStreamDesc struct {
	mode   byte
	width  byte
	value  uint64
	offset uint32
	length uint32
	rawLen uint32
}

func appendColumnarRows(raw []byte, rows []idRow) ([]byte, error) {
	raw = append(raw, blockColumnarMarker)
	var tagModes [8]byte
	var tagConsts [8]uint32
	for dim := range tagModes {
		tagModes[dim], tagConsts[dim] = tagColumnModeRows(rows, dim)
		raw = append(raw, tagModes[dim])
		if tagModes[dim] == modeConst {
			raw = binary.LittleEndian.AppendUint32(raw, tagConsts[dim])
		}
	}
	var valueModes [4]byte
	var valueConsts [4]int64
	for metric := range valueModes {
		valueModes[metric], valueConsts[metric] = valueColumnModeRows(rows, metric)
		raw = append(raw, valueModes[metric])
		if valueModes[metric] == modeConst {
			raw = binary.LittleEndian.AppendUint64(raw, uint64(valueConsts[metric]))
		}
	}
	maskMode, maskConst := maskColumnMode(rows)
	raw = append(raw, maskMode)
	if maskMode == modeConst {
		raw = append(raw, maskConst)
	}
	for dim, mode := range tagModes {
		if mode == modeConst {
			continue
		}
		for _, row := range rows {
			switch mode {
			case modeU8:
				raw = append(raw, byte(row.tags[dim]))
			case modeU16:
				raw = binary.LittleEndian.AppendUint16(raw, uint16(row.tags[dim]))
			case modeU32:
				raw = binary.LittleEndian.AppendUint32(raw, row.tags[dim])
			default:
				return nil, errors.New("strata: invalid tag column mode")
			}
		}
	}
	for metric, mode := range valueModes {
		if mode == modeConst || mode == modeZero {
			continue
		}
		for _, row := range rows {
			switch mode {
			case modeU8:
				raw = append(raw, byte(row.values[metric]))
			case modeU16:
				raw = binary.LittleEndian.AppendUint16(raw, uint16(row.values[metric]))
			case modeU32:
				raw = binary.LittleEndian.AppendUint32(raw, uint32(row.values[metric]))
			case modeI64:
				raw = binary.LittleEndian.AppendUint64(raw, uint64(row.values[metric]))
			default:
				return nil, errors.New("strata: invalid value column mode")
			}
		}
	}
	if maskMode != modeConst {
		for _, row := range rows {
			raw = append(raw, normalizeMask(row.initMask))
		}
	}
	return raw, nil
}

func decodeColumnarRows(raw []byte, off *int, b *sealedBlock, count int) error {
	var tagModes [8]byte
	var tagConsts [8]uint32
	for dim := range tagModes {
		field, err := takeBlockBytes(raw, off, 1)
		if err != nil {
			return err
		}
		tagModes[dim] = field[0]
		if tagModes[dim] == modeConst {
			field, err = takeBlockBytes(raw, off, 4)
			if err != nil {
				return err
			}
			tagConsts[dim] = binary.LittleEndian.Uint32(field)
		} else if tagModes[dim] != modeU8 && tagModes[dim] != modeU16 && tagModes[dim] != modeU32 {
			return errors.New("strata: invalid tag column mode")
		}
	}
	var valueModes [4]byte
	var valueConsts [4]int64
	for metric := range valueModes {
		field, err := takeBlockBytes(raw, off, 1)
		if err != nil {
			return err
		}
		valueModes[metric] = field[0]
		if valueModes[metric] == modeConst {
			field, err = takeBlockBytes(raw, off, 8)
			if err != nil {
				return err
			}
			valueConsts[metric] = int64(binary.LittleEndian.Uint64(field))
		} else if valueModes[metric] != modeZero && valueModes[metric] != modeU8 &&
			valueModes[metric] != modeU16 && valueModes[metric] != modeU32 && valueModes[metric] != modeI64 {
			return errors.New("strata: invalid value column mode")
		}
	}
	field, err := takeBlockBytes(raw, off, 1)
	if err != nil {
		return err
	}
	maskMode := field[0]
	var maskConst byte
	if maskMode == modeConst {
		field, err = takeBlockBytes(raw, off, 1)
		if err != nil {
			return err
		}
		maskConst = field[0]
	} else if maskMode != modeU8 {
		return errors.New("strata: invalid mask column mode")
	}
	b.rows = make([]idRow, count)
	for dim, mode := range tagModes {
		if mode == modeConst {
			for i := range b.rows {
				b.rows[i].tags[dim] = tagConsts[dim]
			}
			continue
		}
		for i := range b.rows {
			switch mode {
			case modeU8:
				field, err = takeBlockBytes(raw, off, 1)
				if err != nil {
					return err
				}
				b.rows[i].tags[dim] = uint32(field[0])
			case modeU16:
				field, err = takeBlockBytes(raw, off, 2)
				if err != nil {
					return err
				}
				b.rows[i].tags[dim] = uint32(binary.LittleEndian.Uint16(field))
			case modeU32:
				field, err = takeBlockBytes(raw, off, 4)
				if err != nil {
					return err
				}
				b.rows[i].tags[dim] = binary.LittleEndian.Uint32(field)
			}
		}
	}
	for metric, mode := range valueModes {
		if mode == modeConst || mode == modeZero {
			for i := range b.rows {
				b.rows[i].values[metric] = valueConsts[metric]
			}
			continue
		}
		for i := range b.rows {
			switch mode {
			case modeU8:
				field, err = takeBlockBytes(raw, off, 1)
				if err != nil {
					return err
				}
				b.rows[i].values[metric] = int64(field[0])
			case modeU16:
				field, err = takeBlockBytes(raw, off, 2)
				if err != nil {
					return err
				}
				b.rows[i].values[metric] = int64(binary.LittleEndian.Uint16(field))
			case modeU32:
				field, err = takeBlockBytes(raw, off, 4)
				if err != nil {
					return err
				}
				b.rows[i].values[metric] = int64(binary.LittleEndian.Uint32(field))
			case modeI64:
				field, err = takeBlockBytes(raw, off, 8)
				if err != nil {
					return err
				}
				b.rows[i].values[metric] = int64(binary.LittleEndian.Uint64(field))
			}
		}
	}
	if maskMode == modeConst {
		for i := range b.rows {
			b.rows[i].initMask = maskConst
		}
	} else {
		for i := range b.rows {
			field, err = takeBlockBytes(raw, off, 1)
			if err != nil {
				return err
			}
			b.rows[i].initMask = field[0]
		}
	}
	return nil
}
