package strata

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrReadOnly              = errors.New("strata: database is read-only")
	ErrReadOnlyCompat        = errors.New("strata: format requires newer writer (read-only compatible)")
	ErrIncompatFormat        = errors.New("strata: incompatible database format")
	ErrResolutionTooCoarse   = errors.New("strata: fine-grained data expired for exact range")
	ErrResetRequiresFineData = errors.New("strata: coarse block intersects active reset rule")
	ErrInvalidRegion         = errors.New("strata: invalid KV region")
	ErrInvalidTTL            = errors.New("strata: TTL must be positive")
	ErrWindowRequired        = errors.New("strata: time window (From/To, Window, or AllTime) is required")
	ErrInvalidWindow         = errors.New("strata: invalid or conflicting time window")
	ErrPageLimitExceeded     = errors.New("strata: page limit out of bounds")
	ErrInvalidCursor         = errors.New("strata: invalid or mismatched pagination cursor")
	ErrStopScan              = errors.New("strata: stop scan")
)

type MetricOp uint8

const (
	OpSum MetricOp = iota
	OpMin
	OpMax
	OpLast
)

type CompressionLevel uint8

const (
	CompressionFastest CompressionLevel = iota
	CompressionDefault
	CompressionBetter
	CompressionBest
)

type MemoryProfile uint8

const (
	MemoryBalanced MemoryProfile = iota
	MemoryConstrained
)

type MemoryBudget struct {
	QueryBytes     int64
	CompactBytes   int64
	PendingBytes   int64
	QueryWorkers   int
	CompactWorkers int
	QueryBatchRows int
}

type Options struct {
	ReadOnly         bool
	UseMmap          bool
	MemoryProfile    MemoryProfile
	MemoryBudget     MemoryBudget
	CompressionLevel CompressionLevel
	WALCompactBytes  int64
	FlushInterval    time.Duration
	SyncInterval     time.Duration
	Retention1m      time.Duration
	Retention1h      time.Duration
	Retention1d      time.Duration
	Now              func() time.Time
}

const (
	DefaultPageLimit = 100
	MaxPageLimit     = 1000
)

type ClientOptions struct {
	DB               Options
	DefaultWindow    time.Duration
	DefaultPageLimit int
	MaxPageLimit     int
}

type SeriesConfig struct {
	Arity      int
	SummaryOps [4]MetricOp
	RowOps     [4]MetricOp
}

type Summary struct {
	Values [4]int64
	Mask   uint8
}

type GroupRow struct {
	Tag     string
	Summary Summary
}

type DictDimStats struct {
	Dim           int
	MaxID         uint32
	LiveCount     uint32
	DeadCount     uint32
	InlineBytes   uint64
	ExternalBytes uint64
	ForwardPages  uint32
	ReverseBlocks uint32
}

type SegmentMeta struct {
	RequestedFrom time.Time
	RequestedTo   time.Time
	CoveredFrom   time.Time
	CoveredTo     time.Time
	Resolution    time.Duration
	Exact         bool
}

type SegmentSummary struct {
	Meta    SegmentMeta
	Summary Summary
}

type AggregateQuery struct {
	From         time.Time
	To           time.Time
	Window       time.Duration
	AllTime      bool
	Filters      []Filter
	GroupDims    []int
	TopK         int
	PreferFine   bool
	RequireExact bool
}

type AggregateResult struct {
	RequestedFrom time.Time
	RequestedTo   time.Time
	EffectiveFrom time.Time
	EffectiveTo   time.Time
	Exact         bool
	Segments      []SegmentSummary
	Summary       Summary
	Groups        [][]GroupRow
	released      bool
}

type Row struct {
	Tags   []string
	Values [4]int64
	Mask   uint8
}

type ReadRequest struct {
	From         time.Time
	To           time.Time
	Window       time.Duration
	AllTime      bool
	Filters      []Filter
	Limit        int
	Offset       int
	Cursor       Cursor
	PreferFine   bool
	RequireExact bool
}

type PageRow struct {
	From       time.Time
	Resolution time.Duration
	Exact      bool
	Tags       []string
	Values     [4]int64
	Mask       uint8
}

type PageResult struct {
	RequestedFrom     time.Time
	RequestedTo       time.Time
	Exact             bool
	ResolutionShifted bool
	Rows              []PageRow
	HasMore           bool
	NextCursor        Cursor
	released          bool
	tagBuf            []string
}

type ScanQuery struct {
	From           time.Time
	To             time.Time
	Window         time.Duration
	AllTime        bool
	Filters        []Filter
	ProjectDims    uint8
	ProjectMetrics uint8
	BatchSize      int
	PreferFine     bool
	RequireExact   bool
}

type LocalHourBucket struct {
	Date          string
	Hour          int
	OffsetSeconds int
	Summary       Summary
}

type queryOptions struct {
	RequireExact bool
	PreferFine   bool
}

type queryResult struct {
	RequestedFrom time.Time
	RequestedTo   time.Time
	EffectiveFrom time.Time
	EffectiveTo   time.Time
	Exact         bool
	Summary       Summary
	sparse        bool
}

type Region struct {
	db   *DB
	name string
}

type Batch struct {
	db  *DB
	cur *Region
	ops []kvOp
	err error
}

type KVScan struct {
	Prefix   string
	Start    string
	End      string
	Limit    int
	KeysOnly bool
}

type KVItem struct {
	Key   string
	Value []byte
}

type DB struct {
	path                  string
	opts                  Options
	policy                memPolicy
	mu                    sync.RWMutex
	compactMu             sync.Mutex
	compacting            atomic.Bool
	querySem              chan struct{}
	querySlots            chan *querySlot
	lightReadSem          chan struct{}
	lightReadSlots        chan *querySlot
	closed                bool
	writer                bool
	lock                  fileLock
	data                  *osFile
	view                  *fileView
	head                  superblock
	series                map[uint16]*Series
	dict                  dictionary
	sealed                []sealedMeta
	sealedByRes           map[sealedResKey][]uint32
	optional              []optionalSection
	mapped                mapView
	live                  map[bucketKey]*bucket
	appendSeq             uint64
	allocatedSeq          uint64
	durableSeq            uint64
	fenceSeq              uint64
	sealedSeq             uint64
	order                 uint64
	walPos                int64
	pending               map[bucketKey]*bucket
	pendingDict           []dictEntry
	pendingBytes          int64
	lastRefreshSize       int64
	loopMu                sync.Mutex
	loopCancel            context.CancelFunc
	loopDone              chan struct{}
	syncHook              func() error
	compactHook           func()
	compactInvertRunBytes int
	kvCatalog             map[string]uint16
	kvRegions             map[uint16]*kvRegion
	kvOverlay             map[kvKey]kvValue
	kvSST                 []kvSSTMeta
	kvRev                 uint64
	kvNext                uint16
	invert                []invertChunkMeta
}

type sealedResKey struct {
	series uint16
	res    byte
}

type Series struct {
	db             *DB
	id             uint16
	cfg            SeriesConfig
	configInferred bool
	resetEpoch     uint64
	rules          []resetRule
}

const (
	resHour     byte = 2
	resDay      byte = 3
	resMinuteHR byte = 4
)
