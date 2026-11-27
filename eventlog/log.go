// Package eventlog stores a local, bounded history of structured events.
// Free-text log formatting and message delivery are outside its API.
package eventlog

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/UruhaLushia/strata/codec"
)

var (
	ErrClosed         = errors.New("eventlog: log is closed")
	ErrLocked         = errors.New("eventlog: directory is locked")
	ErrFull           = errors.New("eventlog: buffer or storage budget is full")
	ErrRecordTooLarge = errors.New("eventlog: record exceeds size limit")
	ErrInvalidOption  = errors.New("eventlog: invalid options")
	ErrCorrupt        = errors.New("eventlog: corrupt shard or frame")
	ErrStopScan       = errors.New("eventlog: stop scan")
)

type Record struct {
	TimestampNS int64 // Event time in UTC nanoseconds.
	StreamID    uint16
	Payload     []byte // A Scan payload is borrowed until that callback returns.
}

// Options uses defaults for zero values. ShardMaxBytes is a soft rotation
// threshold. A shard's first frame can exceed it if it fits the batch and
// storage budgets.
type Options struct {
	MaxRecordBytes     int
	MaxBatchBytes      int
	MaxBatchCount      int
	ShardMaxBytes      int64
	MaxTotalBytes      int64
	MaxShards          int
	MaxConcurrentScans int
	Codec              codec.Codec   // Writes use this codec when it shrinks a frame. Nil writes raw frames.
	Codecs             []codec.Codec // Additional codecs for reading older frames. IDs must be distinct.
}

// Log is a directory-backed event log opened by Open.
type Log struct {
	dir    string
	opts   Options
	lock   *os.File
	codecs map[uint8]codec.Codec

	mu            sync.Mutex
	closed        bool
	writeErr      error
	closing, done chan struct{}
	scans         sync.WaitGroup
	scanSem       chan *scanSlot
	pending       []byte
	count         int
	inFlightBytes int
	inFlightCount int
	shards        []shard // Changes hold both ioMu and mu.

	ioMu     sync.Mutex
	spare    []byte
	enc      codec.Encoder
	encID    uint8
	compBuf  []byte
	active   *os.File
	nextID   uint64
	total    int64
	dirtyDir bool
}

// Open acquires the directory lock and recovers the newest shard.
func Open(dir string, opts Options) (*Log, error) {
	if dir == "" || opts.MaxRecordBytes < 0 || opts.MaxRecordBytes > maxRecordBytes ||
		opts.MaxBatchBytes < 0 || opts.MaxBatchBytes > maxFrameBytes || opts.MaxBatchCount < 0 ||
		opts.ShardMaxBytes < 0 || opts.MaxTotalBytes < 0 || opts.MaxShards < 0 || opts.MaxConcurrentScans < 0 {
		return nil, ErrInvalidOption
	}
	const defaultRecordBytes = 4 << 10
	if opts.MaxBatchBytes == 0 {
		rec := opts.MaxRecordBytes
		if rec == 0 {
			rec = defaultRecordBytes
		}
		opts.MaxBatchBytes = max(64<<10, minRawFrameSize+rec)
		if room := opts.MaxTotalBytes - fileHeaderSize; opts.MaxTotalBytes != 0 && room < int64(opts.MaxBatchBytes) {
			opts.MaxBatchBytes = int(room)
		}
	}
	if opts.MaxRecordBytes == 0 && opts.MaxBatchBytes >= minRawFrameSize {
		opts.MaxRecordBytes = min(defaultRecordBytes, opts.MaxBatchBytes-minRawFrameSize)
	}
	if opts.MaxTotalBytes == 0 {
		opts.MaxTotalBytes = max(32<<20, opts.ShardMaxBytes, int64(fileHeaderSize+opts.MaxBatchBytes))
	}
	if opts.ShardMaxBytes == 0 {
		opts.ShardMaxBytes = min(4<<20, opts.MaxTotalBytes)
	}
	if opts.MaxBatchCount == 0 {
		opts.MaxBatchCount = 1024
	}
	if opts.MaxShards == 0 {
		opts.MaxShards = 16
	}
	if opts.MaxConcurrentScans == 0 {
		opts.MaxConcurrentScans = 2
	}
	if opts.MaxBatchBytes < minRawFrameSize+opts.MaxRecordBytes ||
		opts.ShardMaxBytes < fileHeaderSize+minRawFrameSize || opts.ShardMaxBytes > opts.MaxTotalBytes ||
		opts.MaxTotalBytes < int64(fileHeaderSize+opts.MaxBatchBytes) {
		return nil, ErrInvalidOption
	}
	codecs, encID, err := codecFactories(opts)
	if err != nil {
		return nil, err
	}
	writer := opts.Codec
	opts.Codec, opts.Codecs = nil, nil
	dir, err = filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := lockFile(filepath.Join(dir, "LOCK"))
	if err != nil {
		return nil, err
	}
	l := &Log{
		dir: dir, opts: opts, lock: lock, nextID: 1,
		codecs: codecs, encID: encID,
		closing: make(chan struct{}), done: make(chan struct{}),
		scanSem: make(chan *scanSlot, opts.MaxConcurrentScans),
	}
	if writer != nil {
		l.enc, err = writer.NewEncoder(opts.MaxBatchBytes)
	}
	if err == nil {
		err = l.loadShards()
	}
	if err != nil {
		if l.enc != nil {
			_ = l.enc.Close()
		}
		_ = lock.Close()
		return nil, err
	}
	for range cap(l.scanSem) {
		l.scanSem <- nil
	}
	return l, nil
}

func codecFactories(opts Options) (map[uint8]codec.Codec, uint8, error) {
	if opts.Codec == nil && len(opts.Codecs) == 0 {
		return nil, codec.IDNone, nil
	}
	factories := make(map[uint8]codec.Codec, len(opts.Codecs)+1)
	add := func(c codec.Codec) uint8 {
		if c == nil {
			return codec.IDNone
		}
		id := c.ID()
		if id == codec.IDNone || id > codec.IDFlate && id < 128 || factories[id] != nil {
			return codec.IDNone
		}
		factories[id] = c
		return id
	}
	encID := add(opts.Codec)
	if opts.Codec != nil && encID == codec.IDNone {
		return nil, 0, ErrInvalidOption
	}
	for _, c := range opts.Codecs {
		if add(c) == codec.IDNone {
			return nil, 0, ErrInvalidOption
		}
	}
	return factories, encID, nil
}

// Append copies an entire batch into the pending buffer or rejects it.
func (l *Log) Append(records ...Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if l.writeErr != nil {
		return l.writeErr
	}
	if len(records) == 0 {
		return nil
	}
	if len(records) > l.opts.MaxBatchCount {
		return ErrRecordTooLarge
	}
	size := frameOverhead
	for _, r := range records {
		if len(r.Payload) > l.opts.MaxRecordBytes || size > l.opts.MaxBatchBytes-recordHeaderSize-len(r.Payload) {
			return ErrRecordTooLarge
		}
		size += recordHeaderSize + len(r.Payload)
	}
	if size+max(0, len(l.pending)-frameHeaderSize)+l.inFlightBytes > l.opts.MaxBatchBytes ||
		len(records)+l.count+l.inFlightCount > l.opts.MaxBatchCount {
		return ErrFull
	}
	if l.pending == nil {
		l.pending = make([]byte, 0, l.opts.MaxBatchBytes)
	}
	if l.count == 0 {
		l.pending = l.pending[:frameHeaderSize]
	}
	for _, r := range records {
		l.pending = appendRecord(l.pending, r)
	}
	l.count += len(records)
	return nil
}

// Flush publishes the pending batch for subsequent scans. An eviction error
// leaves the published frame and any shards that could not be removed intact.
func (l *Log) Flush() error {
	l.ioMu.Lock()
	defer l.ioMu.Unlock()
	return l.flush(false)
}

// Sync flushes the pending batch and syncs active shard data. It also syncs
// directory changes on Unix. On Windows, directory metadata durability is
// governed by the operating system.
func (l *Log) Sync() error {
	l.ioMu.Lock()
	defer l.ioMu.Unlock()
	return l.sync(false)
}

// Close syncs accepted records and waits for scans before releasing the lock.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		<-l.done
		return nil
	}
	l.closed = true
	close(l.closing)
	l.mu.Unlock()

	l.ioMu.Lock()
	err := errors.Join(l.sync(true), l.closeActive())
	if l.enc != nil {
		err = errors.Join(err, l.enc.Close())
		l.enc, l.compBuf = nil, nil
	}
	l.ioMu.Unlock()
	l.scans.Wait()
	close(l.scanSem)
	for slot := range l.scanSem {
		if slot != nil {
			if closeErr := slot.closeDecoder(); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
		}
	}
	err = errors.Join(err, l.lock.Close())
	close(l.done)
	return err
}

func (l *Log) flush(closing bool) error {
	l.mu.Lock()
	if l.closed && !closing {
		l.mu.Unlock()
		return ErrClosed
	}
	if l.writeErr != nil {
		err := l.writeErr
		l.mu.Unlock()
		return err
	}
	if l.count == 0 {
		l.mu.Unlock()
		return nil
	}
	raw, count := l.pending, l.count
	l.pending, l.spare, l.count = l.spare, nil, 0
	l.inFlightBytes, l.inFlightCount = len(raw)-frameHeaderSize, count
	l.mu.Unlock()

	committed, err := l.writeBatch(raw, count)

	l.mu.Lock()
	if !committed {
		// Keep the detached records before appends accepted during the write.
		if l.count != 0 {
			raw = append(raw, l.pending[frameHeaderSize:]...)
		}
		l.spare, l.pending, l.count = l.pending[:0], raw, count+l.count
	} else if l.pending == nil {
		l.pending = raw[:0]
	} else {
		l.spare = raw[:0]
	}
	l.inFlightBytes, l.inFlightCount = 0, 0
	l.mu.Unlock()
	return err
}

func (l *Log) writeBatch(raw []byte, count int) (bool, error) {
	body := raw[frameHeaderSize:]
	if l.enc != nil {
		if l.compBuf == nil {
			l.compBuf = make([]byte, frameHeaderSize, l.opts.MaxBatchBytes)
		}
		packed, err := l.enc.Encode(l.compBuf[frameHeaderSize:frameHeaderSize], body)
		if err != nil {
			return false, err
		}
		if len(packed) > 0 && len(packed) < len(body) {
			l.compBuf = finishFrame(append(l.compBuf[:frameHeaderSize], packed...), len(body), count, l.encID)
			return l.writeFrame(l.compBuf)
		}
	}
	return l.writeFrame(finishFrame(raw, len(body), count, codec.IDNone))
}

func (l *Log) sync(closing bool) error {
	if err := l.flush(closing); err != nil {
		return err
	}
	return l.syncStorage()
}

func (l *Log) syncStorage() error {
	if l.active != nil {
		if err := l.active.Sync(); err != nil {
			return err
		}
	}
	if l.dirtyDir {
		if err := syncParent(l.dir); err != nil {
			return err
		}
		l.dirtyDir = false
	}
	return nil
}
