package mojura

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/mojura/backend"
)

type membershipWriteBackend struct {
	backend.Backend
	writes *int
	cancel context.CancelFunc
}

func (b membershipWriteBackend) Transaction(fn func(backend.Transaction) error) error {
	return b.Backend.Transaction(func(tx backend.Transaction) error {
		return fn(membershipWriteTx{Transaction: tx, writes: b.writes, cancel: b.cancel})
	})
}

type membershipWriteTx struct {
	backend.Transaction
	writes *int
	cancel context.CancelFunc
}

func (t membershipWriteTx) GetOrCreateBucket(key []byte) (backend.Bucket, error) {
	bucket, err := t.Transaction.GetOrCreateBucket(key)
	if err == nil && bytes.Equal(key, relationshipAlternateKey) {
		return membershipWriteBucket{Bucket: bucket, depth: 1, writes: t.writes, cancel: t.cancel}, nil
	}

	return bucket, err
}

type membershipWriteBucket struct {
	backend.Bucket
	depth  int
	writes *int
	cancel context.CancelFunc
}

func (b membershipWriteBucket) GetOrCreateBucket(key []byte) (backend.Bucket, error) {
	bucket, err := b.Bucket.GetOrCreateBucket(key)
	if err != nil {
		return nil, err
	}

	return membershipWriteBucket{Bucket: bucket, depth: b.depth + 1, writes: b.writes, cancel: b.cancel}, nil
}

func (b membershipWriteBucket) GetBucket(key []byte) backend.Bucket {
	bucket := b.Bucket.GetBucket(key)
	if bucket == nil {
		return nil
	}

	return membershipWriteBucket{Bucket: bucket, depth: b.depth + 1, writes: b.writes, cancel: b.cancel}
}

func (b membershipWriteBucket) Put(key, value []byte) error {
	err := b.Bucket.Put(key, value)
	if err == nil && b.depth == 3 {
		*b.writes++
		if *b.writes == 1 && b.cancel != nil {
			b.cancel()
		}
	}

	return err
}

func TestACancelledRebuildStopsMembershipWrites(t *testing.T) {
	for _, stop := range []string{"after-decode", "after-first-write", "uncancelled"} {
		t.Run(stop, func(t *testing.T) {
			dir := t.TempDir()
			legacySortedStore(t, dir)
			db := reopenSortedStore(t, dir)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var writes int
			wrapped := membershipWriteBackend{Backend: db.db, writes: &writes}
			want := 2
			switch stop {
			case "after-decode":
				db.opts.Encoder = cancelDecodeEncoder{Encoder: db.opts.Encoder, cancel: cancel}
				want = 0
			case "after-first-write":
				wrapped.cancel = cancel
				want = 1
			}

			db.db = wrapped
			page, err := db.RebuildRelationships(ctx, 1)
			if writes != want {
				t.Errorf("membership writes after cancellation boundary: got %d want %d", writes, want)
			}

			if stop == "uncancelled" {
				if err != nil || page.Scanned != 1 {
					t.Fatalf("ordinary fanout failed: %+v %v", page, err)
				}

				return
			}

			if !errors.Is(err, context.Canceled) || page != (RelationshipRebuildPage{}) {
				t.Fatalf("cancelled page reported progress: %+v %v", page, err)
			}

			if err := db.db.ReadTransaction(func(tx backend.Transaction) error {
				layout, err := loadRelationshipLayout(tx)
				if err != nil {
					return err
				}

				if layout.Build != nil || layout.Slot != 0 || tx.GetBucket(relationshipAlternateKey) != nil {
					return errors.New("cancelled membership work escaped transaction rollback")
				}

				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
