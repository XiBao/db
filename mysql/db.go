package mysql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/XiBao/goutil"
	"github.com/ziutek/mymysql/autorc"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/thrsafe"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv10 "go.opentelemetry.io/otel/semconv/v1.10.0"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/XiBao/db"
	"github.com/XiBao/db/query"
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
			semconv.DBNamespace(db),
		},
	}
	ret.tracer = ret.traceProvider.Tracer(instrumName)
	ret.meter = ret.meterProvider.Meter(instrumName)
	var err error
	ret.queryHistogram, err = ret.meter.Int64Histogram(
		semconv.DBClientOperationDurationName,
		metric.WithDescription(semconv.DBClientOperationDurationDescription),
		metric.WithUnit(semconv.DBClientOperationDurationUnit),
	)
	if err != nil {
		return nil, err
	}
	mysql := autorc.New("tcp", "", host, user, passwd, db)
	mysql.Register("set names utf8mb4")
	ret.db = mysql
	if err = ret.withSpan(ctx, "db.Connect", "", nil,
		func(ctx context.Context, span trace.Span) error {
			return mysql.Reconnect()
		}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (t *DB) EnableFingerprint() {
	t.queryFormatter = query.Fingerprint
}

func (t *DB) formatQuery(query string) string {
	if t.queryFormatter != nil {
		return strings.ToValidUTF8(t.queryFormatter(query), " ")
	}
	return strings.ToValidUTF8(query, " ")
}

func (t *DB) withSpan(
	ctx context.Context,
	spanName string,
	sql string,
	params []interface{},
	fn func(ctx context.Context, span trace.Span) error,
) error {
	var startTime time.Time
	if sql != "" {
		startTime = time.Now()
	}

	attrs := make([]attribute.KeyValue, 0, len(t.attrs)+1)
	attrs = append(attrs, t.attrs...)
	if sql != "" {
		attrs = append(attrs, semconv10.DBStatementKey.String(t.formatQuery(sql)))
		query := sql
		if len(params) > 0 {
			query = fmt.Sprintf(sql, params...)
		}
		attrs = append(attrs, semconv.DBQueryText(query))
	}

	ctx, span := t.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attrs...))
	err := fn(ctx, span)
	defer span.End()

	if sql != "" {
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
	err = t.withSpan(context.TODO(), "db.Query", sql, params,
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
	err = t.withSpan(ctx, "db.Query", sql, params,
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
	err = t.withSpan(context.TODO(), "db.Query", sql, params,
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
	err = t.withSpan(ctx, "db.Query", sql, params,
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
