package strata

import (
	"encoding/binary"
	"hash/crc32"
	"os"

	"github.com/klauspost/compress/zstd"
)

type invertChunkDir struct {
	first, last uint64
	length      uint32
	crc         uint32
}

type invertChunkSink struct {
	chunkPath  string
	chunkFile  *osFile
	chunkPos   int64
	memChunks  []byte
	dir        []invertChunkDir
	rawChunk   []byte
	chunkFirst uint64
	chunkLast  uint64
	hasKeys    bool
	recCount   uint32
	totalRaw   uint32
	enc        *zstd.Encoder
}

func (s *invertChunkSink) close() {
	if s.chunkFile != nil {
		_ = s.chunkFile.Close()
		s.chunkFile = nil
	}
}

func (s *invertChunkSink) addRecord(key uint64, rec []byte) error {
	if len(s.rawChunk) > 0 && len(s.rawChunk)+len(rec) > invertChunkTargetSize {
		if err := s.flushChunk(); err != nil {
			return err
		}
	}
	if !s.hasKeys {
		s.chunkFirst = key
		s.hasKeys = true
	}
	s.chunkLast = key
	s.rawChunk = append(s.rawChunk, rec...)
	s.recCount++
	s.totalRaw += uint32(len(rec))
	return nil
}

func (s *invertChunkSink) flushChunk() error {
	if len(s.rawChunk) == 0 {
		return nil
	}
	comp := s.enc.EncodeAll(s.rawChunk, nil)
	s.dir = append(s.dir, invertChunkDir{
		first: s.chunkFirst, last: s.chunkLast,
		length: uint32(len(comp)), crc: crc(comp),
	})
	if s.chunkFile == nil && len(s.memChunks)+len(comp) > invertMemChunkLimit {
		_ = os.Remove(s.chunkPath)
		f, err := openData(s.chunkPath, false)
		if err != nil {
			return err
		}
		s.chunkFile = f
		if len(s.memChunks) > 0 {
			if _, err := f.WriteAt(s.memChunks, 0); err != nil {
				return err
			}
			s.chunkPos = int64(len(s.memChunks))
			s.memChunks = nil
		}
	}
	if s.chunkFile != nil {
		if _, err := s.chunkFile.WriteAt(comp, s.chunkPos); err != nil {
			return err
		}
		s.chunkPos += int64(len(comp))
	} else {
		s.memChunks = append(s.memChunks, comp...)
	}
	s.rawChunk = s.rawChunk[:0]
	s.hasKeys = false
	return nil
}

func (s *invertChunkSink) writeTo(tf *osFile, pos int64) (int64, sectionDesc, error) {
	if err := s.flushChunk(); err != nil {
		return 0, sectionDesc{}, err
	}
	if len(s.dir) == 0 {
		return pos, sectionDesc{}, nil
	}
	header := binary.AppendUvarint(nil, uint64(len(s.dir)))
	headerLen := len(header) + len(s.dir)*32
	chunkOff := uint64(pos + int64(headerLen))
	for _, d := range s.dir {
		header = binary.LittleEndian.AppendUint64(header, d.first)
		header = binary.LittleEndian.AppendUint64(header, d.last)
		header = binary.LittleEndian.AppendUint64(header, chunkOff)
		header = binary.LittleEndian.AppendUint32(header, d.length)
		header = binary.LittleEndian.AppendUint32(header, d.crc)
		chunkOff += uint64(d.length)
	}
	if _, err := tf.WriteAt(header, pos); err != nil {
		return 0, sectionDesc{}, err
	}
	hash := crc32.New(castagnoli)
	_, _ = hash.Write(header)
	writePos := pos + int64(len(header))

	if s.chunkFile != nil {
		copyBuf := make([]byte, 64<<10)
		var off int64
		for off < s.chunkPos {
			n := min(s.chunkPos-off, int64(len(copyBuf)))
			chunk := copyBuf[:n]
			if _, err := s.chunkFile.ReadAt(chunk, off); err != nil {
				return 0, sectionDesc{}, err
			}
			if _, err := tf.WriteAt(chunk, writePos); err != nil {
				return 0, sectionDesc{}, err
			}
			_, _ = hash.Write(chunk)
			off += n
			writePos += n
		}
	} else if len(s.memChunks) > 0 {
		if _, err := tf.WriteAt(s.memChunks, writePos); err != nil {
			return 0, sectionDesc{}, err
		}
		_, _ = hash.Write(s.memChunks)
		writePos += int64(len(s.memChunks))
	}
	desc := sectionDesc{
		kind: secTSInvert, codec: 1, count: s.recCount,
		offset: uint64(pos), length: uint64(writePos - pos),
		rawLen: s.totalRaw, crc: hash.Sum32(),
	}
	return writePos, desc, nil
}

type invertRunReader struct {
	f      *osFile
	off    int64
	rem    int
	buf    []byte
	pos    int
	cur    invertPair
	hasCur bool
}

func (r *invertRunReader) next() error {
	if r.pos >= len(r.buf) {
		if r.rem == 0 {
			r.hasCur = false
			return nil
		}
		batch := min(r.rem, cap(r.buf)/invertPairDiskSize)
		nBytes := batch * invertPairDiskSize
		r.buf = r.buf[:nBytes]
		if _, err := r.f.ReadAt(r.buf, r.off); err != nil {
			return err
		}
		r.off += int64(nBytes)
		r.rem -= batch
		r.pos = 0
	}
	r.cur = invertPair{
		key:   binary.LittleEndian.Uint64(r.buf[r.pos : r.pos+8]),
		block: binary.LittleEndian.Uint32(r.buf[r.pos+8 : r.pos+12]),
	}
	r.pos += invertPairDiskSize
	r.hasCur = true
	return nil
}

func runLess(readers []invertRunReader, i, j int) bool {
	a, b := readers[i].cur, readers[j].cur
	if a.key != b.key {
		return a.key < b.key
	}
	return a.block < b.block
}

func runHeapSiftDown(heap []int, readers []invertRunReader, i int) {
	n := len(heap)
	for {
		l := 2*i + 1
		if l >= n {
			break
		}
		s := l
		if r := l + 1; r < n && runLess(readers, heap[r], heap[l]) {
			s = r
		}
		if !runLess(readers, heap[s], heap[i]) {
			break
		}
		heap[i], heap[s] = heap[s], heap[i]
		i = s
	}
}
