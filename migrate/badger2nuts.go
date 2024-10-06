package migrate

import (
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/nutsdb/nutsdb"
)

func Badger2Nuts(from *badger.DB, to *nutsdb.DB, table string) error {
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
		}
	}
	return nil
}
