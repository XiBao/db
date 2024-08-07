package badger

import (
	"context"
	"time"

	"github.com/XiBao/goutil"
	"github.com/dgraph-io/badger/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/XiBao/db"
)

var instrumName = goutil.StringsJoin(db.InstrumName, "/badger")

type DB struct {
	db             *badger.DB
	traceProvider  trace.TracerProvider
	tracer         trace.Tracer //nolint:structcheck
	meterProvider  metric.MeterProvider
	meter          metric.Meter
	queryHistogram metric.Int64Histogram
	attrs          []attribute.KeyValue
}

func New(ctx context.Context, options badger.Options) (*DB, error) {
	ret := &DB{
		traceProvider: otel.GetTracerProvider(),
		meterProvider: otel.GetMeterProvider(),
		attrs: []attribute.KeyValue{
			semconv.DBSystemKey.String("badger"),
			semconv.DBNameKey.String(options.Dir),
		},
	}
	ret.tracer = ret.traceProvider.Tracer(instrumName)
	ret.meter = ret.meterProvider.Meter(instrumName)
	var err error
	ret.queryHistogram, err = ret.meter.Int64Histogram(
		"go.badger.query_timing",
		metric.WithDescription("Timing of processed queries"),
		metric.WithUnit("milliseconds"),
	)
	if err != nil {
		return nil, err
	}
	if err := ret.withSpan(ctx, "db.connect", "",
		func(ctx context.Context, span trace.Span) error {
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

func (t *DB) withSpan(
	ctx context.Context,
	spanName string,
	key string,
	fn func(ctx context.Context, span trace.Span) error,
) error {
	var startTime time.Time
	if key != "" {
		startTime = time.Now()
	}

	attrs := make([]attribute.KeyValue, 0, len(t.attrs)+1)
	attrs = append(attrs, t.attrs...)
	if key != "" {
		attrs = append(attrs, semconv.DBStatementKey.String(key))
	}

	ctx, span := t.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...))
	err := fn(ctx, span)
	defer span.End()

	if key != "" {
		t.queryHistogram.Record(ctx, time.Since(startTime).Milliseconds(), metric.WithAttributes(t.attrs...))
	}

	if !span.IsRecording() {
		return err
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (t *DB) GC(ctx context.Context, options *BadgerGCOptions) error {
	return t.withSpan(ctx, "db.gc", "",
		func(ctx context.Context, span trace.Span) error {
			BadgerGC(ctx, options, t.db)
			return nil
		})
}

func (t *DB) Close(ctx context.Context) error {
	return t.withSpan(ctx, "db.close", "",
		func(ctx context.Context, span trace.Span) error {
			return t.db.Close()
		})
}

func (t *DB) Commit(ctx context.Context, txn *badger.Txn) error {
	return t.withSpan(ctx, "db.commit", "",
		func(ctx context.Context, span trace.Span) error {
			return txn.Commit()
		})
}

func (t *DB) Update(ctx context.Context, entry *badger.Entry) error {
	return t.withSpan(ctx, "db.update", string(entry.Key),
		func(ctx context.Context, span trace.Span) error {
			return t.db.Update(func(txn *badger.Txn) error {
				return txn.SetEntry(entry)
			})
		})
}

func (t *DB) Delete(ctx context.Context, key []byte) error {
	return t.withSpan(ctx, "db.delete", string(key),
		func(ctx context.Context, span trace.Span) error {
			return t.db.Update(func(txn *badger.Txn) error {
				return txn.Delete(key)
			})
		})
}

func (t *DB) View(ctx context.Context, key []byte, callback func([]byte) error) error {
	return t.withSpan(ctx, "db.view", string(key),
		func(ctx context.Context, span trace.Span) error {
			return t.db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(key)
				if err != nil {
					return err
				}
				return item.Value(callback)
			})
		})
}

func (t *DB) Get(ctx context.Context, txn *badger.Txn, key []byte) (value []byte, err error) {
	err = t.withSpan(ctx, "db.get", string(key),
		func(ctx context.Context, span trace.Span) error {
			item, err := txn.Get(key)
			if err != nil {
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
