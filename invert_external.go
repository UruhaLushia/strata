package strata

import (
	"encoding/binary"
	"os"
	"slices"

	"github.com/klauspost/compress/zstd"
)

const (
	invertPairDiskSize    = 12
	invertChunkTargetSize = 16 << 10
	invertMemChunkLimit   = 512 << 10
	invertSentinelDense   = ^uint32(0)
)

type invertPair struct {
	key   uint64
	block uint32
}

type invertRunSpan struct {
	offset int64
	count  int
}

type invertExternalBuilder struct {
	tmpPath   string
	chunkPath string
	maxPairs  int
	runBuf    []invertPair
	runFile   *osFile
	runPos    int64
	runs      []invertRunSpan
	seen      map[uint64]struct{}
}

func newInvertExternalBuilder(dbPath string, memBytes int) *invertExternalBuilder {
	maxPairs := max(memBytes/16, 32)
	return &invertExternalBuilder{
		tmpPath:   dbPath + ".invtmp",
		chunkPath: dbPath + ".invchk",
		maxPairs:  maxPairs,
		runBuf:    make([]invertPair, 0, min(maxPairs, 4096)),
	}
}

func (b *invertExternalBuilder) close() {
	if b.runFile != nil {
		_ = b.runFile.Close()
		b.runFile = nil
	}
	if b.tmpPath != "" {
		_ = os.Remove(b.tmpPath)
	}
	if b.chunkPath != "" {
		_ = os.Remove(b.chunkPath)
	}
	b.runBuf = nil
	b.runs = nil
	b.seen = nil
}

func (b *invertExternalBuilder) addPair(key uint64, writeID uint32) error {
	b.runBuf = append(b.runBuf, invertPair{key: key, block: writeID})
	if len(b.runBuf) >= b.maxPairs {
		return b.spillRun()
	}
	return nil
}

func (b *invertExternalBuilder) addBlock(series uint16, writeID uint32, rows []idRow) error {
	if len(rows) == 0 {
		return nil
	}
	if b.seen == nil {
		b.seen = make(map[uint64]struct{}, 64)
	} else {
		clear(b.seen)
	}
	for _, row := range rows {
		for dim, tag := range row.tags {
			if tag == 0 {
				continue
			}
			k := encodeInvertKey(invertKey{series: series, dim: uint8(dim), tag: tag})
			if _, ok := b.seen[k]; ok {
				continue
			}
			b.seen[k] = struct{}{}
			if err := b.addPair(k, writeID); err != nil {
				return err
			}
		}
	}
	if len(b.seen) > 8192 {
		b.seen = nil
	}
	return nil
}

func sortAndDedupePairs(pairs []invertPair) []invertPair {
	slices.SortFunc(pairs, func(a, b invertPair) int {
		if a.key != b.key {
			if a.key < b.key {
				return -1
			}
			return 1
		}
		if a.block < b.block {
			return -1
		}
		if a.block > b.block {
			return 1
		}
		return 0
	})
	if len(pairs) <= 1 {
		return pairs
	}
	out := pairs[:1]
	for _, p := range pairs[1:] {
		if last := out[len(out)-1]; last.key != p.key || last.block != p.block {
			out = append(out, p)
		}
	}
	return out
}

func (b *invertExternalBuilder) spillRun() error {
	if len(b.runBuf) == 0 {
		return nil
	}
	pairs := sortAndDedupePairs(b.runBuf)
	if b.runFile == nil {
		_ = os.Remove(b.tmpPath)
		f, err := openData(b.tmpPath, false)
		if err != nil {
			return err
		}
		b.runFile = f
	}
	startOff := b.runPos
	buf := make([]byte, 0, min(len(pairs)*invertPairDiskSize, 64<<10))
	for _, p := range pairs {
		if len(buf)+invertPairDiskSize > cap(buf) {
			if _, err := b.runFile.WriteAt(buf, b.runPos); err != nil {
				return err
			}
			b.runPos += int64(len(buf))
			buf = buf[:0]
		}
		buf = binary.LittleEndian.AppendUint64(buf, p.key)
		buf = binary.LittleEndian.AppendUint32(buf, p.block)
	}
	if len(buf) > 0 {
		if _, err := b.runFile.WriteAt(buf, b.runPos); err != nil {
			return err
		}
		b.runPos += int64(len(buf))
	}
	b.runs = append(b.runs, invertRunSpan{offset: startOff, count: len(pairs)})
	b.runBuf = b.runBuf[:0]
	return nil
}

func (b *invertExternalBuilder) addPassthroughFromInvert(src *osFile, invertDesc sectionDesc, mapped []byte, chunks []invertChunkMeta, passOldToWrite []int32, passSeriesCount map[uint16]uint32) error {
	stickyAdded := make(map[uint64]struct{})
	for _, chunk := range chunks {
		plain, err := readInvertChunkFrom(src, invertDesc, mapped, chunk)
		if err != nil {
			return err
		}
		var cbErr error
		err = decodeInvertChunkScan(plain, nil, func(k uint64, oldID uint32) {
			if cbErr != nil || int(oldID) >= len(passOldToWrite) {
				return
			}
			if wID := passOldToWrite[oldID]; wID >= 0 {
				cbErr = b.addPair(k, uint32(wID))
			}
		}, func(k uint64) {
			if cbErr != nil {
				return
			}
			if passSeriesCount[decodeInvertKey(k).series] > 0 {
				if _, seen := stickyAdded[k]; !seen {
					stickyAdded[k] = struct{}{}
					cbErr = b.addPair(k, invertSentinelDense)
				}
			}
		})
		if err != nil {
			return err
		}
		if cbErr != nil {
			return cbErr
		}
	}
	return nil
}

func (b *invertExternalBuilder) writeSection(tf *osFile, pos int64, writeToFinal []uint32, seriesBlocks map[uint16]uint32, enc *zstd.Encoder) (int64, sectionDesc, error) {
	sink := &invertChunkSink{chunkPath: b.chunkPath, rawChunk: make([]byte, 0, invertChunkTargetSize), enc: enc}
	defer sink.close()

	var sc invertRecordScratch
	var keyIDs []uint32
	curKey := uint64(0)
	hasKey := false
	isDense := false

	emitCur := func() error {
		if !hasKey {
			return nil
		}
		if !isDense {
			keyIDs = dedupeUint32(keyIDs)
			if len(keyIDs) == 0 {
				hasKey = false
				return nil
			}
			sid := decodeInvertKey(curKey).series
			thresh := max(seriesBlocks[sid]/4, 64)
			if uint32(len(keyIDs)) >= thresh {
				isDense = true
			}
		}
		rec := encodeInvertRecordAdaptive(curKey, keyIDs, isDense, &sc)
		keyIDs = keyIDs[:0]
		hasKey = false
		isDense = false
		return sink.addRecord(curKey, rec)
	}
	feed := func(p invertPair) error {
		if hasKey && p.key != curKey {
			if err := emitCur(); err != nil {
				return err
			}
		}
		curKey = p.key
		hasKey = true
		if p.block == invertSentinelDense {
			isDense = true
			return nil
		}
		if !isDense && int(p.block) < len(writeToFinal) {
			keyIDs = append(keyIDs, writeToFinal[p.block])
		}
		return nil
	}

	if len(b.runs) == 0 {
		pairs := sortAndDedupePairs(b.runBuf)
		for _, p := range pairs {
			if err := feed(p); err != nil {
				return 0, sectionDesc{}, err
			}
		}
	} else {
		if err := b.spillRun(); err != nil {
			return 0, sectionDesc{}, err
		}
		b.runBuf = nil
		readers := make([]invertRunReader, len(b.runs))
		heap := make([]int, 0, len(b.runs))
		for i, sp := range b.runs {
			bufCap := min(max(sp.count*invertPairDiskSize, invertPairDiskSize), 8<<10)
			readers[i] = invertRunReader{
				f: b.runFile, off: sp.offset, rem: sp.count,
				buf: make([]byte, 0, bufCap),
			}
			if err := readers[i].next(); err != nil {
				return 0, sectionDesc{}, err
			}
			if readers[i].hasCur {
				heap = append(heap, i)
			}
		}
		for i := len(heap)/2 - 1; i >= 0; i-- {
			runHeapSiftDown(heap, readers, i)
		}
		for len(heap) > 0 {
			top := heap[0]
			if err := feed(readers[top].cur); err != nil {
				return 0, sectionDesc{}, err
			}
			if err := readers[top].next(); err != nil {
				return 0, sectionDesc{}, err
			}
			if readers[top].hasCur {
				runHeapSiftDown(heap, readers, 0)
			} else {
				heap[0] = heap[len(heap)-1]
				heap = heap[:len(heap)-1]
				if len(heap) > 0 {
					runHeapSiftDown(heap, readers, 0)
				}
			}
		}
	}
	if err := emitCur(); err != nil {
		return 0, sectionDesc{}, err
	}
	return sink.writeTo(tf, pos)
}
