package nutsdb

import (
	"context"
	"errors"

	"github.com/XiBao/db/model"
	"github.com/nutsdb/nutsdb"
)

type Table struct {
	db   *nutsdb.DB
	name string
	ttl  uint32
}

func NewTable(db *nutsdb.DB, name string, ttl uint32) (*Table, error) {
	if err := db.Update(func(tx *nutsdb.Tx) error {
		return tx.NewBucket(nutsdb.DataStructureBTree, name)
	}); err != nil {
		return nil, err
	}
	return &Table{
		db:   db,
		name: name,
		ttl:  ttl,
	}, nil
}

func (tb Table) Name() string {
	return tb.name
}

func (tb Table) TTL() uint32 {
	return tb.ttl
}

func (tb *Table) Set(ctx context.Context, key []byte, val []byte) error {
	return tb.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(tb.name, key, val, tb.ttl)
	})
}

func (tb *Table) Get(ctx context.Context, key []byte, fn func(val []byte) error) error {
	if err := tb.db.View(func(tx *nutsdb.Tx) error {
		if val, err := tx.Get(tb.name, key); err != nil {
			return err
		} else {
			return fn(val)
		}
	}); err != nil {
		if errors.Is(err, nutsdb.ErrNotFoundKey) {
			return model.ErrNotFound
		}
		return err
	}
	return nil
}

func (tb *Table) Delete(ctx context.Context, key []byte) error {
	return tb.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Delete(tb.name, key)
	})
}

func (tb *Table) MSet(ctx context.Context, args ...[]byte) error {
	return tb.db.Update(func(tx *nutsdb.Tx) error {
		return tx.MSet(tb.name, tb.ttl, args...)
	})
}

func (tb *Table) MGet(ctx context.Context, keys [][]byte, fn func(val []byte) error) error {
	return tb.db.View(func(tx *nutsdb.Tx) error {
		if values, err := tx.MGet(tb.name, keys...); err != nil {
			return err
		} else {
			var err error
			for _, key := range values {
				if e := fn(key); e != nil {
					if err == nil {
						err = e
					} else {
						err = errors.Join(err, e)
					}
				}
			}
			return err
		}
	})
}
