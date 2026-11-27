package eventlog

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type shard struct {
	id   uint64
	size int64
}

func (l *Log) shardPath(id uint64) string {
	var name [26]byte
	copy(name[:], "shard-0000000000000000.evl")
	for i := 21; i >= 6; i-- {
		name[i] = "0123456789abcdef"[id&15]
		id >>= 4
	}
	return filepath.Join(l.dir, string(name[:]))
}

func shardID(name string) uint64 {
	if len(name) != len("shard-0000000000000000.evl") || !strings.HasPrefix(name, "shard-") || !strings.HasSuffix(name, ".evl") {
		return 0
	}
	hex := name[len("shard-") : len(name)-len(".evl")]
	if strings.ToLower(hex) != hex {
		return 0
	}
	id, _ := strconv.ParseUint(hex, 16, 64)
	return id
}

func (l *Log) loadShards() error {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		id := shardID(entry.Name())
		if id == 0 {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return ErrCorrupt
		}
		l.total += info.Size()
		l.nextID = id + 1
		l.shards = append(l.shards, shard{id, info.Size()})
	}
	n := len(l.shards)
	shortNewest := n != 0 && l.shards[n-1].size < fileHeaderSize
	if shortNewest {
		n--
		s := l.shards[n]
		if err := os.Remove(l.shardPath(s.id)); err != nil {
			return err
		}
		l.total -= s.size
		l.shards = l.shards[:n]
		l.dirtyDir = true
	}
	for i := range n {
		s := &l.shards[i]
		recoverable := !shortNewest && i == n-1
		flag := os.O_RDONLY
		if recoverable {
			flag = os.O_RDWR
		}
		f, err := openFile(l.shardPath(s.id), flag)
		if err != nil {
			return err
		}
		err = checkHeader(f, s.id)
		if err == nil && recoverable {
			var end int64
			end, err = recoverTail(f, s.size, l.codecs)
			if err == nil {
				l.total -= s.size - end
				s.size = end
				l.active = f
				l.dirtyDir = true
				continue
			}
		}
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func checkHeader(f *os.File, id uint64) error {
	var header [fileHeaderSize]byte
	if err := readAt(f, header[:], 0); err != nil {
		return err
	}
	if !validHeader(header[:], id) {
		return ErrCorrupt
	}
	return nil
}

func validHeader(header []byte, id uint64) bool {
	return string(header[:8]) == fileMagic && binary.LittleEndian.Uint64(header[8:]) == id
}

func (l *Log) closeActive() error {
	if l.active == nil {
		return nil
	}
	f := l.active
	l.active = nil
	return f.Close()
}

func (l *Log) writeFrame(raw []byte) (bool, error) {
	if l.overBudget(0, 0) && len(l.shards) > 1 {
		if err := l.syncStorage(); err != nil {
			return false, err
		}
		if err := l.evictToBudget(); err != nil {
			return false, err
		}
	}
	size := int64(len(raw))
	if l.active == nil {
		return l.rotate(raw)
	}
	s := &l.shards[len(l.shards)-1]
	if (s.size > fileHeaderSize && s.size > l.opts.ShardMaxBytes-size) || s.size > l.opts.MaxTotalBytes-size {
		return l.rotate(raw)
	}
	err := writeAt(l.active, raw, s.size)
	if err == nil && l.overBudget(size, 0) {
		err = l.syncStorage()
	}
	if err != nil {
		if rollback := l.active.Truncate(s.size); rollback != nil {
			err = errors.Join(err, rollback)
			l.mu.Lock()
			l.writeErr = err
			l.mu.Unlock()
		}
		return false, err
	}
	l.mu.Lock()
	s.size += size
	l.mu.Unlock()
	l.total += size
	return true, l.evictToBudget()
}

func (l *Log) rotate(raw []byte) (bool, error) {
	size := fileHeaderSize + int64(len(raw))
	if l.nextID == 0 {
		return false, ErrFull
	}
	if l.active != nil {
		if err := l.active.Sync(); err != nil {
			return false, err
		}
	}
	id := l.nextID
	path := l.shardPath(id)
	f, err := openFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR)
	if err != nil {
		return false, err
	}
	l.nextID++
	l.dirtyDir = true
	var header [fileHeaderSize]byte
	copy(header[:], fileMagic)
	binary.LittleEndian.PutUint64(header[8:], id)
	err = writeAt(f, header[:], 0)
	if err == nil {
		err = writeAt(f, raw, fileHeaderSize)
	}
	evict := l.overBudget(size, 1)
	if err == nil && evict {
		err = f.Sync()
	}
	if err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return false, err
	}
	closeErr := l.closeActive()
	l.mu.Lock()
	l.shards = append(l.shards, shard{id, size})
	l.mu.Unlock()
	l.active = f
	l.total += size
	if evict {
		err = syncParent(l.dir)
		if err == nil {
			l.dirtyDir = false
			err = l.evictToBudget()
		}
	}
	if closeErr != nil {
		err = errors.Join(closeErr, err)
	}
	return true, err
}

func (l *Log) overBudget(bytes int64, newShards int) bool {
	return bytes > l.opts.MaxTotalBytes-l.total ||
		newShards > l.opts.MaxShards-len(l.shards)
}

func (l *Log) evictToBudget() error {
	for l.overBudget(0, 0) {
		if len(l.shards) <= 1 {
			return ErrFull
		}
		s := l.shards[0]
		if err := os.Remove(l.shardPath(s.id)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		l.mu.Lock()
		l.shards = l.shards[1:]
		l.total -= s.size
		l.dirtyDir = true
		l.mu.Unlock()
	}
	return nil
}
