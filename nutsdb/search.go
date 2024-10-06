package nutsdb

import (
	"context"
	"errors"

	"github.com/nutsdb/nutsdb"
)

type searchOption struct {
	prefix   []byte
	reg      string
	startKey []byte
	endKey   []byte
	offset   int
	limit    int
}

type SearchOption = func(opt *searchOption)

func WithPrefix(prefix []byte) SearchOption {
	return func(opt *searchOption) {
		opt.prefix = prefix
	}
}

func WithReg(reg string) SearchOption {
	return func(opt *searchOption) {
		opt.reg = reg
	}
}

func WithStartKey(startKey []byte) SearchOption {
	return func(opt *searchOption) {
		opt.startKey = startKey
	}
}

func WithEndKey(endKey []byte) SearchOption {
	return func(opt *searchOption) {
		opt.endKey = endKey
	}
}

func WithOffset(offset int) SearchOption {
	return func(opt *searchOption) {
		opt.offset = offset
	}
}

func WithLimit(limit int) SearchOption {
	return func(opt *searchOption) {
		opt.offset = limit
	}
}

func NewSearchOption(opts ...SearchOption) *searchOption {
	ret := new(searchOption)
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

func (tb *Table) Search(ctx context.Context, cb func(key []byte, value []byte) error, opts ...SearchOption) error {
	option := NewSearchOption(opts...)
	var fn func(*nutsdb.Tx) error
	if option.prefix != nil {
		if option.reg != "" {
			fn = func(tx *nutsdb.Tx) error {
				values, err := tx.PrefixSearchScan(tb.name, option.prefix, option.reg, option.offset, option.limit)
				if err != nil {
					return err
				}
				for _, value := range values {
					if e := cb(nil, value); e != nil {
						if err == nil {
							err = e
						} else {
							err = errors.Join(err, e)
						}
					}
				}
				return err
			}
		} else {
			fn = func(tx *nutsdb.Tx) error {
				values, err := tx.PrefixScan(tb.name, option.prefix, option.offset, option.limit)
				if err != nil {
					return err
				}
				for _, value := range values {
					if e := cb(nil, value); e != nil {
						if err == nil {
							err = e
						} else {
							err = errors.Join(err, e)
						}
					}
				}
				return err
			}
		}
	} else if option.startKey != nil && option.endKey != nil {
		fn = func(tx *nutsdb.Tx) error {
			values, err := tx.RangeScan(tb.name, option.startKey, option.endKey)
			if err != nil {
				return err
			}
			for _, value := range values {
				if e := cb(nil, value); e != nil {
					if err == nil {
						err = e
					} else {
						err = errors.Join(err, e)
					}
				}
			}
			return err
		}
	} else {
		fn = func(tx *nutsdb.Tx) error {
			keys, values, err := tx.GetAll(tb.name)
			if err != nil {
				return err
			}
			for idx, key := range keys {
				if e := cb(key, values[idx]); e != nil {
					if err == nil {
						err = e
					} else {
						err = errors.Join(err, e)
					}
				}
			}
			return err
		}
	}
	return tb.db.View(fn)
}
