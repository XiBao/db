package migrate

import (
	"context"
	"errors"

	"github.com/nutsdb/nutsdb"
	"github.com/rs/zerolog"
)

func ChangeNutsIndex(ctx context.Context, from *nutsdb.DB, to *nutsdb.DB) error {
	logger := zerolog.Ctx(ctx).With().Str("migrate", "nutsdb index").Logger()
	var buckets []string
	if err := from.View(
		func(tx *nutsdb.Tx) error {
			return tx.IterateBuckets(nutsdb.DataStructureBTree, "*", func(bucket string) bool {
				buckets = append(buckets, bucket)
				return true
			})
		}); err != nil {
		return err
	}
	for _, bucket := range buckets {
		logger.Info().Str("bucket", bucket).Msg("start migrating")
		if err := iterateNutsBucket(ctx, bucket, from, to); err != nil {
			logger.Error().Ctx(ctx).Err(err).Send()
		} else {
			logger.Info().Str("bucket", bucket).Msg("finish migrating")
		}
	}
	return nil
}

func iterateNutsBucket(ctx context.Context, bucket string, from *nutsdb.DB, to *nutsdb.DB) error {
	if err := to.Update(func(tx *nutsdb.Tx) error {
		err := tx.NewBucket(nutsdb.DataStructureBTree, bucket)
		if errors.Is(err, nutsdb.ErrBucketAlreadyExist) {
			return nil
		}
		return err
	}); err != nil {
		return err
	}
	tx, err := from.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Commit()
	logger := zerolog.Ctx(ctx)
	iter := nutsdb.NewIterator(tx, bucket, nutsdb.IteratorOptions{Reverse: false})
	for iter.Rewind(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value, err := iter.Value()
		if err != nil {
			continue
		}
		ttl, err := tx.GetTTL(bucket, key)
		if err != nil {
			continue
		}
		if err := to.Update(func(toTx *nutsdb.Tx) error {
			return toTx.Put(bucket, key, value, uint32(ttl))
		}); err != nil {
			logger.Error().Ctx(ctx).Err(err).Send()
		}
	}
	return nil
}
