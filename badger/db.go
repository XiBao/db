package badger

import (
	"context"
	"encoding/hex"
	"time"
	"unicode/utf8"

	"github.com/XiBao/db/model"
	"github.com/XiBao/goutil"
	"github.com/dgraph-io/badger/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv10 "go.opentelemetry.io/otel/semconv/v1.10.0"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/XiBao/db"
)

var instrumName = goutil.StringsJoin(db.InstrumName, "/badger")

type DB struct {
	db             *badger.DB
	option         *option
	traceProvider  trace.TracerProvider
	tracer         trace.Tracer //nolint:structcheck
	meterProvider  metric.MeterProvider
	meter          metric.Meter
	queryHistogram metric.Int64Histogram
	attrs          []attribute.KeyValue
}

func New(ctx context.Context, options badger.Options, dbOptions ...Option) (*DB, error) {
	ret := &DB{
		option:        new(option),
		traceProvider: otel.GetTracerProvider(),
		meterProvider: otel.GetMeterProvider(),
		attrs: []attribute.KeyValue{
			semconv.DBSystemKey.String("badger"),
			semconv.DBNamespace(options.Dir),
		},
	}
	for _, opt := range dbOptions {
		opt(ret.option)
	}
	ret.tracer = ret.traceProvider.Tracer(instrumName)
	ret.meter = ret.meterProvider.Meter(instrumName)
	if histogram, err := ret.meter.Int64Histogram(
		semconv.DBClientOperationDurationName,
		metric.WithDescription(semconv.DBClientOperationDurationDescription),
		metric.WithUnit(semconv.DBClientOperationDurationUnit),
	); err != nil {
		return nil, err
	} else {
		ret.queryHistogram = histogram
	}
	if err := ret.withSpan(ctx, "db.connect", "connect", nil,
		func(ctx context.Context) error {
			if conn, err := badger.Open(options); err != nil {
				return err
			} else {
				ret.db = conn
			}
			return nil
		}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (t *DB) DB() *badger.DB {
	return t.db
}

func (t *DB) TracingEnabled() bool {
	return t.option != nil && t.option.enableTracing
}

func (t *DB) MetricEnabled() bool {
	return t.option != nil && t.option.enableMetric
}

func (t *DB) withSpan(
	ctx context.Context,
	spanName string,
	operation string,
	key []byte,
	fn func(ctx context.Context) error,
) error {
	if !t.TracingEnabled() && !t.MetricEnabled() {
		return fn(ctx)
	}
	var (
		span      trace.Span
		startTime time.Time
	)
	if key != nil {
		startTime = time.Now()
	}

	if t.TracingEnabled() {
		attrs := make([]attribute.KeyValue, 0, len(t.attrs)+1)
		attrs = append(attrs, t.attrs...)
		if key != nil {
			attrs = append(attrs, semconv10.DBStatementKey.String(safeString(key)))
			attrs = append(attrs, semconv.DBQueryText(safeString(key)))
		}
		if operation != "" {
			attrs = append(attrs, semconv.DBOperationName(operation))
		}

		ctx, span = t.tracer.Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attrs...))
		defer span.End()
	}
	err := fn(ctx)

	if span != nil && span.IsRecording() && err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	if key != nil && t.MetricEnabled() {
		t.queryHistogram.Record(ctx, time.Since(startTime).Milliseconds(), metric.WithAttributes(t.attrs...))
	}

	return err
}

func (t *DB) GC(ctx context.Context, options *BadgerGCOptions) error {
	return t.withSpan(ctx, "db.gc", "gc", nil,
		func(ctx context.Context) error {
			BadgerGC(ctx, options, t.db)
			return nil
		})
}

func (t *DB) Close(ctx context.Context) error {
	return t.withSpan(ctx, "db.close", "close", nil,
		func(ctx context.Context) error {
			return t.db.Close()
		})
}

func (t *DB) Commit(ctx context.Context, txn *badger.Txn) error {
	return t.withSpan(ctx, "db.commit", "commit", nil,
		func(ctx context.Context) error {
			return txn.Commit()
		})
}

func (t *DB) Update(ctx context.Context, entry *badger.Entry) error {
	return t.withSpan(ctx, "db.update", "update", entry.Key,
		func(ctx context.Context) error {
			return t.db.Update(func(txn *badger.Txn) error {
				return txn.SetEntry(entry)
			})
		})
}

func (t *DB) Delete(ctx context.Context, key []byte) error {
	return t.withSpan(ctx, "db.delete", "delete", key,
		func(ctx context.Context) error {
			return t.db.Update(func(txn *badger.Txn) error {
				return txn.Delete(key)
			})
		})
}

func (t *DB) View(ctx context.Context, key []byte, callback func([]byte) error) error {
	return t.withSpan(ctx, "db.view", "view", key,
		func(ctx context.Context) error {
			return t.db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(key)
				if err != nil {
					if err == badger.ErrKeyNotFound {
						return model.ErrNotFound
					} else if err == badger.ErrEmptyKey {
						return model.ErrEmptyKey
					}
					return err
				}
				return item.Value(callback)
			})
		})
}

func (t *DB) Get(ctx context.Context, txn *badger.Txn, key []byte) (value []byte, err error) {
	err = t.withSpan(ctx, "db.get", "get", key,
		func(ctx context.Context) error {
			item, err := txn.Get(key)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					return model.ErrNotFound
				} else if err == badger.ErrEmptyKey {
					return model.ErrEmptyKey
				}
				return err
			}
			if val, err := item.ValueCopy(nil); err != nil {
				return err
			} else {
				value = val
			}
			return nil
		})
	return
}

func (t *DB) NewTransaction(update bool) *badger.Txn {
	return t.db.NewTransaction(update)
}

func safeString(bs []byte) string {
	if bs == nil {
		return ""
	}
	if utf8.Valid(bs) {
		return string(bs)
	}
	return goutil.StringsJoin("0x", hex.EncodeToString(bs))
}
