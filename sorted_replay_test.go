package mojura

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/mojura/enkodo"
	"github.com/mojura/kiroku"
	"github.com/mojura/mojura/action"
)

func TestSortedDeleteReplayRemovesMemberships(t *testing.T) {
	db := openSortedRecords(t)
	seedSortedRecords(t, db)
	var encoded bytes.Buffer
	if err := enkodo.NewWriter(&encoded).Encode(&action.Action{Type: action.TypeDelete, Key: []byte("a")}); err != nil {
		t.Fatal(err)
	}

	for replay := 0; replay < 2; replay++ {
		if err := db.importTransaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
			return tx.processBlock(kiroku.Block(encoded.Bytes()))
		}); err != nil {
			t.Fatal(err)
		}

		if got := orderedIDs(t, db, OrderedReadOptions{}); !slices.Equal(got, []string{"d", "b", "c"}) {
			t.Fatalf("replayed deletion left a stale sorted membership: %v", got)
		}
	}
}

func TestSortedSnapshotPurgeUsesTheRequestedLayout(t *testing.T) {
	for _, converted := range []bool{false, true} {
		name := "fresh"
		if converted {
			name = "converted"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if converted {
				legacySortedStore(t, dir)
			}
			db := reopenSortedStore(t, dir)
			if converted {
				finishRelationshipRebuild(t, db, 2)
			} else {
				seedSortedRecords(t, db)
			}
			if err := db.importTransaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
				if err := db.purge(tx.txn); err != nil {
					return err
				}
				for _, row := range []struct {
					id string
					at int64
				}{{"z", 5}, {"x", 10}} {
					if _, err := tx.Put(row.id, &sortedRecord{Entry: Entry{CreatedAt: row.at}, Room: "one", Actor: "one"}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if got := orderedIDs(t, db, OrderedReadOptions{}); !slices.Equal(got, []string{"z", "x"}) {
				t.Fatalf("purge/replay layout: %v", got)
			}
		})
	}
}
