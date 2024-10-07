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
		err := tx.NewBucket(nutsdb.DataStructureBTree, name)
		if errors.Is(err, nutsdb.ErrBucketAlreadyExist) {
			return nil
		}
		return err
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

func (tb *Table) Persist(ctx context.Context, key []byte, val []byte) error {
	return tb.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(tb.name, key, val, nutsdb.Persistent)
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
		if errors.Is(err, nutsdb.ErrNotFoundKey) || errors.Is(err, nutsdb.ErrKeyNotFound) {
			return model.ErrNotFound
		}
		return err
	}
	return nil
}

func (tb *Table) GetTTL(ctx context.Context, key []byte) (int64, error) {
	var ttl int64
	if err := tb.db.View(func(tx *nutsdb.Tx) error {
		if val, err := tx.GetTTL(tb.name, key); err != nil {
			return err
		} else {
			ttl = val
			return nil
		}
	}); err != nil {
		if errors.Is(err, nutsdb.ErrNotFoundKey) || errors.Is(err, nutsdb.ErrKeyNotFound) {
			return 0, model.ErrNotFound
		}
		return 0, err
	}
	return ttl, nil
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

func (tb *Table) BatchSet(ctx context.Context, args ...[]byte) error {
	return tb.db.Update(func(tx *nutsdb.Tx) error {
		if len(args) == 0 {
			return nil
		}

		if len(args)%2 != 0 {
			return nutsdb.ErrKVArgsLenNotEven
		}

		var err error
		for i := 0; i < len(args); i += 2 {
			if e := tx.Put(tb.name, args[i], args[i+1], tb.ttl); e != nil {
				if err == nil {
					err = e
				} else {
					err = errors.Join(err, e)
				}
			}
		}
		return err
	})
}

func (tb *Table) MGet(ctx context.Context, keys [][]byte, fn func(val []byte) error) error {
	return tb.db.View(func(tx *nutsdb.Tx) error {
		if values, err := tx.MGet(tb.name, keys...); err != nil {
			if errors.Is(err, nutsdb.ErrNotFoundKey) || errors.Is(err, nutsdb.ErrKeyNotFound) {
				return model.ErrNotFound
			}
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

func (tb *Table) Iterate(ctx context.Context, opt *nutsdb.IteratorOptions, fn func(key []byte, val []byte) error) error {
	tx, err := tb.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Commit()
	iter := nutsdb.NewIterator(tx, tb.name, *opt)
	iter.Rewind()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		if value, err := iter.Value(); err != nil {
			continue
		} else if e := fn(iter.Key(), value); e != nil {
			if err == nil {
				err = e
			} else {
				err = errors.Join(err, e)
			}
		}
	}
	return err
}
