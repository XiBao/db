package migrate

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/nutsdb/nutsdb"
	"github.com/rs/zerolog"
)

func Badger2Nuts(ctx context.Context, from *badger.DB, to *nutsdb.DB, table string) error {
	logger := zerolog.Ctx(ctx).With().Str("migrate", "badger2nuts").Str("table", table).Logger()
	opts := badger.DefaultIteratorOptions
	txn := from.NewTransaction(false)
	defer txn.Discard()
	iter := txn.NewIterator(opts)
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		now := uint64(time.Now().Unix())
		ttl := item.ExpiresAt() - now
		if ttl > 0 {
			key := item.KeyCopy(nil)
			value, _ := item.ValueCopy(nil)
			if err := to.Update(func(tx *nutsdb.Tx) error {
				return tx.Put(table, key, value, uint32(ttl))
			}); err != nil {
				return err
			}
			logger.Info().Hex("key", key).Hex("value", value).Uint64("ttl", ttl).Msg("transfered")
		} else {
			logger.Warn().Uint64("expires_in", ttl).Msg("skipped")
		}
	}
	return nil
}
