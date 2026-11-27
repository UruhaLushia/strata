package strata

import (
	"context"
	"errors"
	"iter"
	"sync"
)

var (
	errClientClosed       = errors.New("strata: client is closed")
	errSeriesUnregistered = errors.New("strata: series is not registered")
)

// Client manages handle caching, default window/page guardrails, scoped queries, and buffer reclamation.
type Client struct {
	mu      sync.RWMutex
	db      *DB
	opts    ClientOptions
	ownsDB  bool
	closed  bool
	series  map[uint16]*Series
	regions map[string]*Region
}

// OpenClient opens a database and returns a Client that owns its lifecycle.
func OpenClient(path string, opts ClientOptions) (*Client, error) {
	opts = defaultClientOptions(opts)
	db, err := Open(path, opts.DB)
	if err != nil {
		return nil, err
	}
	return newClient(db, opts, true), nil
}

// NewClient creates a lightweight Client view over an existing DB without taking ownership of db.Close.
func NewClient(db *DB, opts ClientOptions) *Client {
	opts = defaultClientOptions(opts)
	return newClient(db, opts, false)
}

func newClient(db *DB, opts ClientOptions, ownsDB bool) *Client {
	return &Client{
		db:      db,
		opts:    opts,
		ownsDB:  ownsDB,
		series:  make(map[uint16]*Series),
		regions: make(map[string]*Region),
	}
}

func (c *Client) DB() *DB {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil
	}
	return c.db
}

func (c *Client) RegisterSeries(id uint16, cfg SeriesConfig) (*Series, error) {
	if c == nil {
		return nil, errClientClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.db == nil {
		return nil, errClientClosed
	}
	s, err := c.db.Series(id, cfg)
	if err != nil {
		return nil, err
	}
	c.series[id] = s
	return s, nil
}

func (c *Client) Series(id uint16, cfg SeriesConfig) (*Series, error) {
	if c == nil {
		return nil, errClientClosed
	}
	c.mu.RLock()
	if c.closed || c.db == nil {
		c.mu.RUnlock()
		return nil, errClientClosed
	}
	db := c.db
	s := c.series[id]
	c.mu.RUnlock()
	if s != nil {
		db.mu.RLock()
		scfg, inferred := s.cfg, s.configInferred
		db.mu.RUnlock()
		if !inferred && scfg.Arity == cfg.Arity &&
			(cfg.SummaryOps == scfg.SummaryOps || cfg.SummaryOps == [4]MetricOp{}) &&
			(cfg.RowOps == scfg.RowOps || cfg.RowOps == [4]MetricOp{}) {
			return s, nil
		}
	}
	return c.RegisterSeries(id, cfg)
}

func (c *Client) Region(name string) (*Region, error) {
	if c == nil {
		return nil, errClientClosed
	}
	c.mu.RLock()
	if !c.closed && c.db != nil && c.regions[name] != nil {
		r := c.regions[name]
		c.mu.RUnlock()
		return r, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.db == nil {
		return nil, errClientClosed
	}
	if r := c.regions[name]; r != nil {
		return r, nil
	}
	r, err := c.db.Region(name)
	if err != nil {
		return nil, err
	}
	c.regions[name] = r
	return r, nil
}

func (c *Client) NewBatch() *Batch {
	db, err := c.activeDB()
	if err != nil {
		return &Batch{err: err}
	}
	return db.NewBatch()
}

func (c *Client) Refresh() error {
	db, err := c.activeDB()
	if err != nil {
		return err
	}
	return db.Refresh()
}

func (c *Client) Flush() error {
	db, err := c.activeDB()
	if err != nil {
		return err
	}
	return db.Flush()
}

func (c *Client) Sync() error {
	db, err := c.activeDB()
	if err != nil {
		return err
	}
	return db.Sync()
}

func (c *Client) Compact() error {
	db, err := c.activeDB()
	if err != nil {
		return err
	}
	return db.Compact()
}

func (c *Client) Read(ctx context.Context, seriesID uint16, req ReadRequest) (*PageResult, error) {
	s, opts, err := c.resolveSeriesAndOpts(seriesID)
	if err != nil {
		return nil, err
	}
	return s.readWithOptions(ctx, req, opts.DefaultWindow, opts.DefaultPageLimit, opts.MaxPageLimit)
}

func (c *Client) WithPage(ctx context.Context, seriesID uint16, req ReadRequest, fn func(*PageResult) error) error {
	page, err := c.Read(ctx, seriesID, req)
	if err != nil {
		return err
	}
	defer page.Release()
	if fn == nil {
		return nil
	}
	return fn(page)
}

func (c *Client) Scan(ctx context.Context, seriesID uint16, q ScanQuery, fn func(ScanBatch) error) error {
	s, opts, err := c.resolveSeriesAndOpts(seriesID)
	if err != nil {
		return err
	}
	return s.scanWithDefaultWindow(ctx, q, opts.DefaultWindow, fn)
}

func (c *Client) ScanSeq(ctx context.Context, seriesID uint16, q ScanQuery) iter.Seq2[ScanBatch, error] {
	return func(yield func(ScanBatch, error) bool) {
		stopped := false
		err := c.Scan(ctx, seriesID, q, func(batch ScanBatch) error {
			if !yield(batch, nil) {
				stopped = true
				return ErrStopScan
			}
			return nil
		})
		if err != nil && !stopped {
			yield(ScanBatch{}, err)
		}
	}
}

func (c *Client) Aggregate(ctx context.Context, seriesID uint16, q AggregateQuery) (*AggregateResult, error) {
	s, opts, err := c.resolveSeriesAndOpts(seriesID)
	if err != nil {
		return nil, err
	}
	return s.aggregateWithDefaultWindow(ctx, q, opts.DefaultWindow)
}

func (c *Client) WithAggregate(ctx context.Context, seriesID uint16, q AggregateQuery, fn func(*AggregateResult) error) error {
	res, err := c.Aggregate(ctx, seriesID, q)
	if err != nil {
		return err
	}
	defer res.Release()
	if fn == nil {
		return nil
	}
	return fn(res)
}

func (c *Client) DictStats(seriesID uint16) ([]DictDimStats, error) {
	s, _, err := c.resolveSeriesAndOpts(seriesID)
	if err != nil {
		return nil, err
	}
	return s.DictStats()
}

func (c *Client) Reclaim() {
	if db, err := c.activeDB(); err == nil {
		db.reclaim()
	}
	reclaimPools()
}

func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	db, owns := c.db, c.ownsDB
	c.db, c.series, c.regions = nil, nil, nil
	c.mu.Unlock()

	if db != nil {
		db.reclaimSlots()
	}
	reclaimPools()
	if owns && db != nil {
		return db.Close()
	}
	return nil
}

func (c *Client) activeDB() (*DB, error) {
	if c == nil {
		return nil, errClientClosed
	}
	c.mu.RLock()
	db, closed := c.db, c.closed
	c.mu.RUnlock()
	if closed || db == nil {
		return nil, errClientClosed
	}
	return db, nil
}

func (c *Client) resolveSeriesAndOpts(id uint16) (*Series, ClientOptions, error) {
	if c == nil {
		return nil, ClientOptions{}, errClientClosed
	}
	c.mu.RLock()
	if c.closed || c.db == nil {
		c.mu.RUnlock()
		return nil, ClientOptions{}, errClientClosed
	}
	opts := c.opts
	if s := c.series[id]; s != nil {
		c.mu.RUnlock()
		return s, opts, nil
	}
	db := c.db
	c.mu.RUnlock()

	db.mu.RLock()
	s := db.series[id]
	db.mu.RUnlock()
	if s == nil {
		return nil, ClientOptions{}, errSeriesUnregistered
	}
	c.mu.Lock()
	if !c.closed && c.db == db && c.series[id] == nil {
		c.series[id] = s
	}
	c.mu.Unlock()
	return s, opts, nil
}
