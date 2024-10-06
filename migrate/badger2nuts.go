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
		expiresIn := now - item.ExpiresAt()
		if expiresIn > 0 {
			key := item.KeyCopy(nil)
			value, _ := item.ValueCopy(nil)
			if err := to.Update(func(tx *nutsdb.Tx) error {
				return tx.Put(table, key, value, uint32(expiresIn))
			}); err != nil {
				return err
			}
			logger.Info().Bytes("key", key).Bytes("value", value).Msg("transfered")
		} else {
			logger.Warn().Uint64("expires_in", expiresIn).Msg("skipped")
		}
	}
	return nil
}
