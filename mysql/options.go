package mysql

import "github.com/XiBao/db/query"

type option struct {
	enableTracing  bool
	enableMetric   bool
	queryFormatter func(query string) string
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

func WithQueryFormator(queryFormatter func(query string) string) Option {
	return func(opt *option) {
		opt.queryFormatter = queryFormatter
	}
}

func EnableFingerprint() Option {
	return func(opt *option) {
		opt.queryFormatter = query.Fingerprint
	}
}
