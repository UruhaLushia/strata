package strata

import (
	"errors"
	"math"
	"os"
	"path/filepath"
)

// Open opens or creates a Strata database at path.
func Open(path string, opts Options) (*DB, error) {
	opts, policy := defaultOptions(opts)
	codecs, err := newCodecTable(opts)
	if err != nil {
		return nil, err
	}
	if path == "" {
		return nil, errors.New("strata: empty path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	var lk fileLock
	if !opts.ReadOnly {
		lk, err = openLock(path + ".lock")
		if err != nil {
			return nil, err
		}
		_ = os.Remove(path + ".tmp")
		_ = os.Remove(path + ".invtmp")
		_ = os.Remove(path + ".invchk")
	}
	f, err := openData(path, opts.ReadOnly)
	if err != nil && !opts.ReadOnly && os.IsNotExist(err) {
		f, err = openData(path, false)
	}
	if err != nil {
		if lk != nil {
			_ = lk.Close()
		}
		return nil, err
	}
	db := &DB{
		path:           path,
		opts:           opts,
		policy:         policy,
		codecs:         codecs,
		querySem:       make(chan struct{}, policy.maxConcurrentQueries),
		querySlots:     make(chan *querySlot, policy.maxConcurrentQueries),
		lightReadSem:   make(chan struct{}, 1),
		lightReadSlots: make(chan *querySlot, 1),
		writer:         !opts.ReadOnly,
		loopClosed:     opts.ReadOnly,
		lock:           lk,
		data:           f,
		kvNext:         0x100,
	}
	if st, stErr := f.Stat(); stErr != nil {
		_ = db.Close()
		return nil, stErr
	} else if st.Size() < headerSize {
		db.head = initialSuper()
		db.head.created = opts.Now().Unix()
		if err = writeSupers(f, db.head); err == nil {
			err = f.Sync()
		}
		if err != nil {
			_ = db.Close()
			return nil, err
		}
	} else if db.head, err = readSuper(f); err != nil {
		_ = db.Close()
		return nil, err
	}
	if db.head.roCompat&^knownReadOnly != 0 && !opts.ReadOnly {
		_ = db.Close()
		return nil, ErrReadOnlyCompat
	}
	if err = db.codecs.checkSections(db.head); err != nil {
		_ = db.Close()
		return nil, err
	}
	if db.mapped, err = mapPrefix(f, db.head.walOffset, db.shouldMmap()); err != nil {
		_ = db.Close()
		return nil, err
	}
	db.view = newFileView(db.data, db.mapped, db.head.sections[secTSBlocks], db.codecs)
	if err = db.loadLocked(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) loadLocked() error {
	oldSeries := db.series
	db.series = make(map[uint16]*Series)
	db.dict = newDictWithPolicy(db.policy)
	db.live = make(map[bucketKey]*bucket)
	db.pending = make(map[bucketKey]*bucket)
	db.pendingDict = nil
	db.pendingBytes = 0
	db.invert = nil
	db.kvCatalog = make(map[string]uint16)
	db.kvRegions = make(map[uint16]*kvRegion)
	db.kvOverlay = make(map[kvKey]kvValue)
	db.kvNext = 0x100
	db.sealed = nil
	db.sealedByRes = nil
	db.order = 0
	mapped := db.mappedBytes()
	var err error
	var catalogRaw []byte
	if catalogRaw, err = db.loadKVIndex(); err != nil {
		return err
	}
	if len(catalogRaw) > 0 {
		if err = decodeKVCatalog(catalogRaw, db); err != nil {
			return err
		}
	}
	if d := db.head.sections[5]; d.length > 0 {
		if err := decodeDict(db.data, d, mapped, db.view, &db.dict, db.series); err != nil {
			return err
		}
	}
	for id, fresh := range db.series {
		if old := oldSeries[id]; old != nil {
			if old.configInferred && !fresh.configInferred {
				old.cfg = fresh.cfg
				old.configInferred = false
			}
			old.resetEpoch = fresh.resetEpoch
			old.rules = fresh.rules
			db.series[id] = old
			continue
		}
		fresh.db = db
	}
	for id, old := range oldSeries {
		if _, ok := db.series[id]; !ok {
			old.resetEpoch = 0
			old.rules = nil
			db.series[id] = old
		}
	}
	var blocks []sealedMeta
	if blocks, err = decodeBlocks(db.data, db.head, mapped, db.codecs); err != nil {
		return err
	}
	db.sealed = blocks
	sortSealedMetas(db.sealed)
	counts := make(map[sealedResKey]int, 8)
	for i := range db.sealed {
		if db.sealed[i].order > db.order {
			db.order = db.sealed[i].order
		}
		counts[sealedResKey{series: db.sealed[i].series, res: db.sealed[i].res}]++
	}
	db.sealedByRes = make(map[sealedResKey][]uint32, len(counts))
	for key, n := range counts {
		db.sealedByRes[key] = make([]uint32, 0, n)
	}
	for i := range db.sealed {
		key := sealedResKey{series: db.sealed[i].series, res: db.sealed[i].res}
		db.sealedByRes[key] = append(db.sealedByRes[key], uint32(i))
	}
	if err = db.loadInvertIndex(); err != nil {
		return err
	}
	var optional []optionalSection
	if optional, err = validateSections(db.data, db.head); err != nil {
		return err
	}
	db.optional = optional
	db.appendSeq = db.head.sealedWalSeq
	db.allocatedSeq = db.appendSeq
	db.durableSeq = db.appendSeq
	db.kvRev = db.head.sealedKVRev
	pos, maxSeq, err := scanWAL(db.data, int64(db.head.walOffset), db.writer, db.applyFrame)
	if err != nil {
		return err
	}
	db.walPos = pos
	if maxSeq > db.appendSeq {
		db.appendSeq = maxSeq
		db.allocatedSeq = maxSeq
		db.durableSeq = maxSeq
	}
	return nil
}

func (db *DB) mappedBytes() []byte {
	if db.mapped == nil {
		return nil
	}
	return db.mapped.bytes()
}

func (db *DB) applyFrame(fr walFrame) error {
	if fr.seq <= db.head.sealedWalSeq {
		return nil
	}
	switch fr.typ {
	case frameDelta:
		ds, bs, err := decodeDelta(fr.payload)
		if err != nil {
			return err
		}
		for _, d := range ds {
			if err := db.dict.apply(d); err != nil {
				return err
			}
		}
		for _, b := range bs {
			s := db.series[b.series]
			if s == nil {
				arity := int(b.arity)
				if arity < 1 || arity > 8 {
					arity = 1
					for _, row := range b.rows {
						for i := 7; i >= 0; i-- {
							if row.tags[i] != 0 {
								arity = i + 1
								break
							}
						}
						if arity > 1 {
							break
						}
					}
				}
				cfg := defaultSeriesConfig(arity)
				cfg.SummaryOps, cfg.RowOps = b.summaryOps, b.rowOps
				s = &Series{db: db, id: b.series, cfg: cfg, configInferred: true}
				db.series[b.series] = s
			} else if s.configInferred {
				s.cfg.SummaryOps, s.cfg.RowOps = b.summaryOps, b.rowOps
			}
			if b.order > db.order {
				db.order = b.order
			}
			db.mergeLive(b, true)
		}
	case frameReset:
		x, err := decodeReset(fr.payload)
		if err != nil {
			return err
		}
		s := db.series[x.series]
		if s == nil {
			s = &Series{db: db, id: x.series, cfg: defaultSeriesConfig(1), configInferred: true}
			db.series[x.series] = s
		}
		if s.resetEpoch == x.oldEpoch || s.resetEpoch < x.newEpoch {
			s.resetEpoch = x.newEpoch
			s.rules = append(s.rules, resetRule{oldEpoch: x.oldEpoch, from: x.from, to: x.to})
		}
		if x.from == math.MinInt64 && x.to == math.MaxInt64 {
			db.resetSeriesDictAndLiveLocked(x.series, x.oldEpoch)
		}
	case frameKVBatch:
		rev, ops, err := decodeKVBatch(fr.payload)
		if err != nil {
			return err
		}
		db.applyKVBatch(rev, ops)
	}
	return nil
}

func (db *DB) Series(id uint16, cfg SeriesConfig) (*Series, error) {
	if !cfg.valid() {
		return nil, errors.New("strata: series arity must be 1..8")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, errors.New("strata: database is closed")
	}
	if s := db.series[id]; s != nil {
		if s.cfg.Arity != cfg.Arity {
			return nil, errors.New("strata: series config mismatch")
		}
		if s.configInferred {
			s.cfg.SummaryOps, s.cfg.RowOps = cfg.SummaryOps, cfg.RowOps
			s.configInferred = false
			return s, nil
		}
		if s.cfg.SummaryOps == ([4]MetricOp{}) && cfg.SummaryOps != ([4]MetricOp{}) {
			s.cfg.SummaryOps = cfg.SummaryOps
		}
		if s.cfg.RowOps == ([4]MetricOp{}) && cfg.RowOps != ([4]MetricOp{}) {
			s.cfg.RowOps = cfg.RowOps
		}
		return s, nil
	}
	if db.opts.ReadOnly {
		return nil, ErrReadOnly
	}
	s := &Series{db: db, id: id, cfg: cfg}
	db.series[id] = s
	return s, nil
}
