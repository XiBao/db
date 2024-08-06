package mysql

import (
	"context"
	"time"

	"github.com/XiBao/goutil"
	"github.com/ziutek/mymysql/autorc"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/thrsafe"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/XiBao/db"
)

var instrumName = goutil.StringsJoin(db.InstrumName, "/mysql")

type DB struct {
	db             *autorc.Conn
	traceProvider  trace.TracerProvider
	tracer         trace.Tracer //nolint:structcheck
	meterProvider  metric.MeterProvider
	meter          metric.Meter
	queryHistogram metric.Int64Histogram
	queryFormatter func(query string) string
	attrs          []attribute.KeyValue
}

func New(ctx context.Context, host, user, passwd, db string) (*DB, error) {
	ret := &DB{
		traceProvider: otel.GetTracerProvider(),
		meterProvider: otel.GetMeterProvider(),
		attrs: []attribute.KeyValue{
			semconv.DBSystemMySQL,
			semconv.DBNameKey.String(db),
		},
	}
	ret.tracer = ret.traceProvider.Tracer(instrumName)
	ret.meter = ret.meterProvider.Meter(instrumName)
	var err error
	ret.queryHistogram, err = ret.meter.Int64Histogram(
		"go.sql.query_timing",
		metric.WithDescription("Timing of processed queries"),
		metric.WithUnit("milliseconds"),
	)
	if err != nil {
		return nil, err
	}
	mysql := autorc.New("tcp", "", host, user, passwd, db)
	mysql.Register("set names utf8mb4")
	ret.db = mysql
	if err = ret.withSpan(ctx, "db.Connect", "",
		func(ctx context.Context, span trace.Span) error {
			return mysql.Reconnect()
		}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (t *DB) EnableFingerprint() {
	t.queryFormatter = Fingerprint
}

func (t *DB) formatQuery(query string) string {
	if t.queryFormatter != nil {
		return t.queryFormatter(query)
	}
	return query
}

func (t *DB) withSpan(
	ctx context.Context,
	spanName string,
	query string,
	fn func(ctx context.Context, span trace.Span) error,
) error {
	var startTime time.Time
	if query != "" {
		startTime = time.Now()
	}

	attrs := make([]attribute.KeyValue, 0, len(t.attrs)+1)
	attrs = append(attrs, t.attrs...)
	if query != "" {
		attrs = append(attrs, semconv.DBStatementKey.String(t.formatQuery(query)))
	}

	ctx, span := t.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...))
	err := fn(ctx, span)
	defer span.End()

	if query != "" {
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

func (t *DB) Query(sql string, params ...interface{}) (rows []mysql.Row, res mysql.Result, err error) {
	err = t.withSpan(context.TODO(), "db.Query", sql,
		func(ctx context.Context, span trace.Span) error {
			rows, res, err = t.db.Query(sql, params...)
			if err != nil {
				return err
			}
			if span.IsRecording() {
				span.SetAttributes(db.RowsAffected.Int64(int64(res.AffectedRows())))
			}
			return nil
		})
	return
}

func (t *DB) QueryCtx(ctx context.Context, sql string, params ...interface{}) (rows []mysql.Row, res mysql.Result, err error) {
	err = t.withSpan(ctx, "db.Query", sql,
		func(ctx context.Context, span trace.Span) error {
			rows, res, err = t.db.Query(sql, params...)
			if err != nil {
				return err
			}
			if span.IsRecording() {
				span.SetAttributes(db.RowsAffected.Int64(int64(res.AffectedRows())))
			}
			return nil
		})
	return
}

func (t *DB) QueryFirst(sql string, params ...interface{}) (row mysql.Row, res mysql.Result, err error) {
	err = t.withSpan(context.TODO(), "db.Query", sql,
		func(ctx context.Context, span trace.Span) error {
			row, res, err = t.db.QueryFirst(sql, params...)
			if err != nil {
				return err
			}
			if span.IsRecording() {
				span.SetAttributes(db.RowsAffected.Int64(int64(res.AffectedRows())))
			}
			return nil
		})
	return
}

func (t *DB) QueryFirstCtx(ctx context.Context, sql string, params ...interface{}) (row mysql.Row, res mysql.Result, err error) {
	err = t.withSpan(ctx, "db.Query", sql,
		func(ctx context.Context, span trace.Span) error {
			row, res, err = t.db.QueryFirst(sql, params...)
			if err != nil {
				return err
			}
			if span.IsRecording() {
				span.SetAttributes(db.RowsAffected.Int64(int64(res.AffectedRows())))
			}
			return nil
		})
	return
}

func (t *DB) Quote(str string) string {
	return goutil.DBQuote(str, t.db)
}

func (t *DB) Escape(str string) string {
	return t.db.Escape(str)
}
