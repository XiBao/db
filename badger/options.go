package badger

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/rs/zerolog"
)

type option struct {
	enableTracing bool
	enableMetric  bool
}

type Option = func(opt *option)

func WithTracing(enabled bool) Option {
	return func(opt *option) {
		opt.enableTracing = enabled
	}
}

func WithMetric(enabled bool) Option {
	return func(opt *option) {
		opt.enableMetric = enabled
	}
}

func DefaultOptions(ctx context.Context, filePath string) badger.Options {
	opts := badger.DefaultOptions(filePath).WithLogger(NewBadgerLogger(ctx, 5))
	opts.NumVersionsToKeep = 1
	opts.Compression = options.Snappy
	opts.MetricsEnabled = false
	opts.InMemory = false

	// To allow writes at a faster speed, we create a new memtable as soon as
	// an existing memtable is filled up. This option determines how many
	// memtables should be kept in memory.
	// opts.NumMemtables = 2 << 20

	opts.MemTableSize = 64 << 20
	opts.ValueLogFileSize = 64 << 20

	// We don't compact L0 on close as this can greatly delay shutdown time.
	opts.CompactL0OnClose = false

	// This value specifies how much memory should be used by table indices. These
	// indices include the block offsets and the bloomfilters. Badger uses bloom
	// filters to speed up lookups. Each table has its own bloom
	// filter and each bloom filter is approximately of 5 MB. This defaults
	// to an unlimited size (and quickly balloons to GB with a large DB).
	opts.IndexCacheSize = 2000 << 20

	// Don't cache blocks in memory. All reads should go to disk.
	// opts.BlockCacheSize = 0

	// The NumLevelZeroTables and NumLevelZeroTableStall will not have any
	// effect on the memory if `KeepL0InMemory` is set to false.
	// Don't keep multiple memtables in memory. With larger
	// memtable size, this explodes memory usage.
	opts.NumLevelZeroTables = 1
	opts.NumLevelZeroTablesStall = 2

	// SyncWrites=false has significant effect on write performance. When sync
	// writes is set to true, badger ensures the data is flushed to the disk after a
	// write call. For normal usage, such a high level of consistency is not required.
	opts.SyncWrites = false

	return opts
}

func LowMemOptions(ctx context.Context, filePath string) badger.Options {
	opts := DefaultOptions(ctx, filePath)
	// To allow writes at a faster speed, we create a new memtable as soon as
	// an existing memtable is filled up. This option determines how many
	// memtables should be kept in memory.
	opts.NumMemtables = 1
	opts.IndexCacheSize = 100 << 20
	opts.Compression = options.None
	opts.DetectConflicts = false
	// // Don't cache blocks in memory. All reads should go to disk.
	opts.BlockCacheSize = 256 << 20
	return opts
}

func LowMemLowCPUBadgerOptions(ctx context.Context, filePath string) badger.Options {
	opts := LowMemOptions(ctx, filePath)
	opts.NumCompactors = 2
	return opts
}

type BadgerGCOptions struct {
	GCDiscardRatio float64
	GCInterval     time.Duration
	GCSleep        time.Duration
}

var DefaultBadgerGCOptions = BadgerGCOptions{
	GCDiscardRatio: 0.5,
	GCInterval:     time.Minute * 5,
	GCSleep:        time.Second * 15,
}

func BadgerGC(ctx context.Context, extendedOptions *BadgerGCOptions, db *badger.DB) {
	t := time.NewTicker(extendedOptions.GCInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			switch err := db.RunValueLogGC(extendedOptions.GCDiscardRatio); err {
			case badger.ErrNoRewrite, badger.ErrRejected:
				// 没写入 被拒绝
				t.Reset(extendedOptions.GCInterval)
			case nil:
				// 无错误
				t.Reset(extendedOptions.GCSleep)
			case badger.ErrDBClosed:
				// 被关闭 返回
				return
			default:
				// 其他错误
				db.Opts().Logger.Errorf("error during a GC cycle %s", err)
				// Not much we can do on a random error but log it and continue.
				t.Reset(extendedOptions.GCInterval)
			}
		case <-ctx.Done():
			return
		}
	}
}

type BadgerLogger struct {
	logger zerolog.Logger
}

func NewBadgerLogger(ctx context.Context, callerSkip int) *BadgerLogger {
	return &BadgerLogger{
		logger: zerolog.Ctx(ctx).With().CallerWithSkipFrameCount(callerSkip).Logger(),
	}
}

func (l *BadgerLogger) Errorf(format string, v ...interface{}) {
	l.logger.Error().Msgf(format, v...)
}

func (l *BadgerLogger) Infof(format string, v ...interface{}) {
	l.logger.Info().Msgf(format, v...)
}

func (l *BadgerLogger) Warningf(format string, v ...interface{}) {
	l.logger.Warn().Msgf(format, v...)
}

func (l *BadgerLogger) Debugf(format string, v ...interface{}) {
	l.logger.Debug().Msgf(format, v...)
}
