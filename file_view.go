package strata

import (
	"errors"
	"sync/atomic"
)

var errBlockCRC = errors.New("strata: block CRC mismatch")

type fileView struct {
	data   *osFile
	mapped mapView
	blocks sectionDesc
	refs   atomic.Int32
}

func newFileView(data *osFile, mapped mapView, blocks sectionDesc) *fileView {
	view := &fileView{data: data, mapped: mapped, blocks: blocks}
	view.refs.Store(1)
	return view
}

func (v *fileView) acquire() *fileView {
	if v != nil {
		v.refs.Add(1)
	}
	return v
}

func (v *fileView) release() {
	if v == nil || v.refs.Add(-1) != 0 {
		return
	}
	if v.mapped != nil {
		_ = v.mapped.close()
	}
	if v.data != nil {
		_ = v.data.Close()
	}
}

func (v *fileView) mappedBytes() []byte {
	if v == nil || v.mapped == nil {
		return nil
	}
	return v.mapped.bytes()
}

func (v *fileView) adviseDontNeed() {
	if v != nil && v.mapped != nil {
		v.mapped.adviseDontNeed()
	}
}

func (v *fileView) readRawPooled(offset uint64, length, wantCRC uint32) ([]byte, *[]byte, error) {
	if offset < v.blocks.offset || offset+uint64(length) > v.blocks.offset+v.blocks.length {
		return nil, nil, errBlockOutsideSection
	}
	raw, bufPtr, err := sectionBlockPooled(v.data, v.blocks, offset, length, v.mappedBytes())
	if err != nil {
		putReadBuf(bufPtr)
		return nil, nil, err
	}
	if crc(raw) != wantCRC {
		putReadBuf(bufPtr)
		return nil, nil, errBlockCRC
	}
	return raw, bufPtr, nil
}
