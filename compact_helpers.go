package strata

import "errors"

type seriesSnap struct {
	id    uint16
	cfg   SeriesConfig
	rules []resetRule
}

func excludedByRuleSlice(rules []resetRule, start int64, epoch uint64) bool {
	for _, r := range rules {
		if epoch <= r.oldEpoch && start >= r.from && start < r.to {
			return true
		}
	}
	return false
}

func loadSealedBlockFromView(view *fileView, sec sectionDesc, meta sealedMeta) (sealedBlock, error) {
	if view == nil || view.data == nil {
		return sealedBlock{}, errors.New("strata: missing source file view")
	}
	var mapped []byte
	if view.mapped != nil {
		mapped = view.mapped.bytes()
	}
	raw, bufPtr, err := sectionBlockPooled(view.data, sec, meta.offset, meta.length, mapped)
	defer putReadBuf(bufPtr)
	if err != nil {
		return sealedBlock{}, err
	}
	if crc(raw) != meta.crc {
		return sealedBlock{}, errors.New("strata: block CRC mismatch")
	}
	decoded, err := decodeBlock(raw)
	if err != nil {
		return sealedBlock{}, err
	}
	decoded.offset, decoded.length, decoded.rawLen, decoded.crc = meta.offset, meta.length, meta.length, meta.crc
	return decoded, nil
}
