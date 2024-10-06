package migrate

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/nutsdb/nutsdb"
	"github.com/rs/zerolog"
)

func Badger2Nuts(ctx context.Context, from *badger.DB, to *nutsdb.DB, table string, convertKey func(from []byte) []byte) error {
	logger := zerolog.Ctx(ctx).With().Str("migrate", "badger2nuts").Str("table", table).Logger()
	opts := badger.DefaultIteratorOptions
	txn := from.NewTransaction(false)
	defer txn.Discard()
	iter := txn.NewIterator(opts)
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		var (
			item    = iter.Item()
			now     = time.Now().Unix()
			ttl     uint32
			expired bool
		)
		if item.ExpiresAt() == 0 {
			ttl = nutsdb.Persistent
		} else {
			if item.IsDeletedOrExpired() {
				expired = true
			} else if diff := int64(item.ExpiresAt()) - now; diff <= 0 {
				expired = true
			} else {
				ttl = uint32(diff)
			}
		}
		if !expired {
			oriKey := item.Key()
			var key []byte
			if convertKey != nil {
				key = convertKey(oriKey)
			}
			if err := item.Value(func(val []byte) error {
				return to.Update(func(tx *nutsdb.Tx) error {
					return tx.Put(table, key, val, ttl)
				})
			}); err != nil {
				return err
			}
		} else {
			logger.Warn().Time("expires_at", time.Unix(int64(item.ExpiresAt()), 0)).Msg("skipped")
		}
	}
	return nil
}
