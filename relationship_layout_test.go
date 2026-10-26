package mojura

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/mojura/backend"
	"github.com/mojura/mojura/filters"
)

type legacySortedRecord sortedRecord

func (r *legacySortedRecord) GetRelationships() Relationships {
	return Relationships{{r.Room}, {r.Actor}}
}

func legacySortedStore(t *testing.T, dir string) {
	t.Helper()
	db, err := New[*legacySortedRecord](MakeOpts("conversion", dir), "room", "actor")
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Transaction(context.Background(), func(tx *Transaction[*legacySortedRecord]) error {
		for i, at := range []int64{30, 20, 20, 10} {
			if _, err := tx.Put(fmt.Sprintf("id-%d", i), &legacySortedRecord{Entry: Entry{CreatedAt: at}, Room: "one", Actor: "one"}); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func reopenSortedStore(t *testing.T, dir string) *Mojura[*sortedRecord] {
	t.Helper()
	db, err := New[*sortedRecord](MakeOpts("conversion", dir), "room", "actor")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if !db.closed {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	return db
}

func rawEntries(t *testing.T, db *Mojura[*sortedRecord]) map[string]string {
	t.Helper()
	rows := make(map[string]string)
	if err := db.db.ReadTransaction(func(tx backend.Transaction) error {
		return tx.GetBucket(entriesBktKey).ForEach(func(id, raw []byte) error {
			rows[string(id)] = string(raw)
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}

	return rows
}

func finishRelationshipRebuild(t *testing.T, db *Mojura[*sortedRecord], limit int) {
	t.Helper()
	for call := 0; call < 100; call++ {
		page, err := db.RebuildRelationships(context.Background(), limit)
		if err != nil {
			t.Fatal(err)
		}

		if page.Scanned > limit || page.Removed > limit || page.Scanned+page.Removed > limit {
			t.Fatalf("unbounded conversion batch: %+v", page)
		}

		if page.Done {
			return
		}
	}

	t.Fatal("bounded fixture did not finish its conversion and retirement")
}

func TestTheRebuildConvertsSetsInBoundedBatches(t *testing.T) {
	dir := t.TempDir()
	legacySortedStore(t, dir)
	db := reopenSortedStore(t, dir)
	before := rawEntries(t, db)
	read := func() error {
		return db.ReadTransaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
			return tx.ForEachOrderedID(func(OrderedID) error { t.Error("partial layout yielded"); return nil }, OrderedReadOptions{}, filters.Match("room", "one"))
		})
	}
	if err := read(); !errors.Is(err, ErrRelationshipRebuildRequired) {
		t.Fatalf("opening legacy data silently adopted sorted layout: %v", err)
	}

	page, err := db.RebuildRelationships(context.Background(), 1)
	if err != nil || page.Scanned != 1 || page.AfterID != "id-0" || page.Done {
		t.Fatalf("first bounded batch: page=%+v err=%v", page, err)
	}

	if err := read(); !errors.Is(err, ErrRelationshipRebuildInProgress) {
		t.Fatalf("partially built set was readable: %v", err)
	}

	if _, err := db.Put("new", &sortedRecord{Entry: Entry{CreatedAt: 40}, Room: "one", Actor: "one"}); !errors.Is(err, ErrRelationshipRebuildInProgress) {
		t.Fatalf("write during stopped conversion = %v", err)
	}

	if _, err := db.Get("id-3"); err != nil {
		t.Fatalf("record lookup unavailable during index conversion: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = reopenSortedStore(t, dir)
	page, err = db.RebuildRelationships(context.Background(), 1)
	if err != nil || page.Scanned != 1 || page.AfterID != "id-1" || page.Done {
		t.Fatalf("restart did not resume after committed entry: page=%+v err=%v", page, err)
	}

	finishRelationshipRebuild(t, db, 1)
	if got := orderedIDs(t, db, OrderedReadOptions{}); !slices.Equal(got, []string{"id-3", "id-1", "id-2", "id-0"}) {
		t.Fatalf("converted order/ties = %v", got)
	}

	after := rawEntries(t, db)
	if len(before) != len(after) {
		t.Fatalf("conversion rewrote record set: %d -> %d", len(before), len(after))
	}

	for id, raw := range before {
		if after[id] != raw {
			t.Fatalf("conversion rewrote record %s", id)
		}
	}

	if err := db.db.ReadTransaction(func(tx backend.Transaction) error {
		layout, err := loadRelationshipLayout(tx)
		if err != nil {
			return err
		}

		if layout.Build != nil || layout.Retiring || tx.GetBucket(layout.otherRoot()) != nil {
			return fmt.Errorf("completed rebuild retained a duplicate generation: %+v", layout)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	page, err = db.RebuildRelationships(context.Background(), 1)
	if err != nil || !page.Done || page.Scanned != 0 || page.Removed != 0 {
		t.Fatalf("completed rebuild was not idempotent: %+v %v", page, err)
	}

	if _, err := db.Update("id-0", func(record *sortedRecord) error { record.CreatedAt = 1; return nil }); err != nil {
		t.Fatal(err)
	}

	if got := orderedIDs(t, db, OrderedReadOptions{}); !slices.Equal(got, []string{"id-0", "id-3", "id-1", "id-2"}) {
		t.Fatalf("converted generation did not maintain new writes: %v", got)
	}
}

type cancelDecodeEncoder struct {
	Encoder
	cancel context.CancelFunc
}

func (e cancelDecodeEncoder) Unmarshal(raw []byte, value interface{}) error {
	if err := e.Encoder.Unmarshal(raw, value); err != nil {
		return err
	}

	e.cancel()
	return nil
}

func TestACancelledRebuildCommitsNeitherKeysNorCheckpoint(t *testing.T) {
	dir := t.TempDir()
	legacySortedStore(t, dir)
	db := reopenSortedStore(t, dir)
	var before []byte
	if err := db.db.ReadTransaction(func(tx backend.Transaction) error {
		before = bytes.Clone(tx.GetBucket(metaBktKey).Get(relationshipLayoutKey))
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	encoder := db.opts.Encoder
	db.opts.Encoder = cancelDecodeEncoder{Encoder: encoder, cancel: cancel}
	page, err := db.RebuildRelationships(ctx, 2)
	db.opts.Encoder = encoder
	if !errors.Is(err, context.Canceled) || page != (RelationshipRebuildPage{}) {
		t.Fatalf("cancelled batch reported committed progress: %+v %v", page, err)
	}

	if err := db.db.ReadTransaction(func(tx backend.Transaction) error {
		if !bytes.Equal(before, tx.GetBucket(metaBktKey).Get(relationshipLayoutKey)) || tx.GetBucket(relationshipAlternateKey) != nil {
			return errors.New("cancelled batch leaked index keys or advanced its checkpoint")
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	finishRelationshipRebuild(t, db, 2)
	if got := orderedIDs(t, db, OrderedReadOptions{}); !slices.Equal(got, []string{"id-3", "id-1", "id-2", "id-0"}) {
		t.Fatalf("resume after cancelled batch changed the page: %v", got)
	}
}

func TestAnUnsortedValueKeepsEntryIDKeys(t *testing.T) {
	db := openTempMojura(t)
	seedTagged(t, db, 4, func(int) []string { return []string{"all"} })
	if err := db.ReadTransaction(context.Background(), func(tx *Transaction[*testStruct]) error {
		var ids []string
		if err := tx.ForEachIntersectedID(func(id string) error { ids = append(ids, id); return nil }, filters.Match("tags", "all")); err != nil {
			return err
		}

		if !slices.Equal(ids, []string{"00000000", "00000001", "00000002", "00000003"}) {
			return fmt.Errorf("unsorted IDs changed: %v", ids)
		}

		bucket, err := tx.getRelationshipBucket([]byte("tags"))
		if err != nil {
			return err
		}

		var keys []string
		if err := bucket.GetBucket([]byte("all")).ForEach(func(key, _ []byte) error { keys = append(keys, string(key)); return nil }); err != nil {
			return err
		}

		if !slices.Equal(keys, ids) {
			return fmt.Errorf("unsorted storage keys changed: %v", keys)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

type extraSortedRecord struct{ sortedRecord }

func (r *extraSortedRecord) GetRelationships() Relationships {
	return Relationships{{r.Room}, {r.Actor}, {"all"}}
}

func TestANewSortedIndexRequiresExplicitRebuild(t *testing.T) {
	dir := t.TempDir()
	db := reopenSortedStore(t, dir)
	seedSortedRecords(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	extended, err := New[*extraSortedRecord](MakeOpts("conversion", dir), "room", "actor", "added")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := extended.Close(); err != nil {
			t.Error(err)
		}
	})
	read := func() ([]string, error) {
		var ids []string
		err := extended.ReadTransaction(context.Background(), func(tx *Transaction[*extraSortedRecord]) error {
			return tx.ForEachOrderedID(func(position OrderedID) error { ids = append(ids, position.EntryID); return nil }, OrderedReadOptions{}, filters.Match("added", "all"), filters.Match("actor", "one"))
		})
		return ids, err
	}
	if ids, err := read(); !errors.Is(err, ErrRelationshipRebuildRequired) || len(ids) != 0 {
		t.Fatalf("unbuilt new index looked like an authoritative empty page: ids=%v err=%v", ids, err)
	}

	complete := false
	for call := 0; call < 100; call++ {
		page, err := extended.RebuildRelationships(context.Background(), 2)
		if err != nil {
			t.Fatal(err)
		}
		if page.Done {
			complete = true
			break
		}
	}
	if !complete {
		t.Fatal("new index rebuild did not finish")
	}
	ids, err := read()
	if err != nil || !slices.Equal(ids, []string{"d", "b", "a", "c"}) {
		t.Fatalf("new index did not cover old records: ids=%v err=%v", ids, err)
	}
}

type variableSortedRecord struct {
	sortedRecord
	Bad bool
}

func (r *variableSortedRecord) GetSortKey() []byte {
	if r.Bad {
		return []byte{1}
	}
	return r.sortedRecord.GetSortKey()
}

func TestASortWidthMismatchRollsBackTheWrite(t *testing.T) {
	db, err := New[*variableSortedRecord](MakeOpts("width", t.TempDir()), "room", "actor")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	good := &variableSortedRecord{sortedRecord: sortedRecord{Entry: Entry{CreatedAt: 10}, Room: "one", Actor: "one"}}
	if _, err := db.Put("good", good); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Update("good", func(value *variableSortedRecord) error { value.Bad = true; return nil }); !errors.Is(err, ErrInvalidSortKey) {
		t.Fatalf("invalid update returned %v", err)
	}
	if _, err := db.Put("bad", &variableSortedRecord{sortedRecord: good.sortedRecord, Bad: true}); !errors.Is(err, ErrInvalidSortKey) {
		t.Fatalf("invalid insert returned %v", err)
	}
	value, err := db.Get("good")
	if err != nil || value.Bad {
		t.Fatalf("failed update changed stored value: %v %v", value, err)
	}
	if exists, err := db.Exists("bad"); err != nil || exists {
		t.Fatalf("failed insert persisted: %v %v", exists, err)
	}
	if err := db.ReadTransaction(context.Background(), func(tx *Transaction[*variableSortedRecord]) error {
		var ids []string
		err := tx.ForEachOrderedID(func(position OrderedID) error { ids = append(ids, position.EntryID); return nil }, OrderedReadOptions{}, filters.Match("room", "one"))
		if err != nil {
			return err
		}
		if !slices.Equal(ids, []string{"good"}) {
			return fmt.Errorf("failed write changed memberships: %v", ids)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestAnInterruptedRebuildCanBeAbortedInBoundedBatches(t *testing.T) {
	dir := t.TempDir()
	legacySortedStore(t, dir)
	db := reopenSortedStore(t, dir)
	before := rawEntries(t, db)
	if _, err := db.RebuildRelationships(context.Background(), 1); err != nil {
		t.Fatal(err)
	}

	aborter, ok := any(db).(interface {
		AbortRelationshipRebuild(context.Context, int) (RelationshipRebuildPage, error)
	})
	if !ok {
		t.Fatal("explicit conversion has no bounded pre-cutover abort")
	}

	done := false
	for call := 0; call < 30; call++ {
		page, err := aborter.AbortRelationshipRebuild(context.Background(), 1)
		if err != nil || page.Scanned != 0 || page.Removed > 1 {
			t.Fatalf("bounded abort: %+v %v", page, err)
		}

		if page.Done {
			done = true
			break
		}

		if _, err := db.RebuildRelationships(context.Background(), 1); !errors.Is(err, ErrRelationshipRebuildAborting) {
			t.Fatalf("build resumed a partially aborted generation: %v", err)
		}
	}

	if !done {
		t.Fatal("abort did not retire its temporary generation")
	}

	if err := db.db.ReadTransaction(func(tx backend.Transaction) error {
		layout, err := loadRelationshipLayout(tx)
		if err != nil {
			return err
		}
		if layout.Width != 0 || layout.Slot != 0 || layout.Build != nil || layout.Retiring || tx.GetBucket(relationshipAlternateKey) != nil {
			return fmt.Errorf("abort changed active layout or retained replacement: %+v", layout)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for id, raw := range rawEntries(t, db) {
		if before[id] != raw {
			t.Fatalf("abort rewrote %s", id)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	legacy, err := New[*legacySortedRecord](MakeOpts("conversion", dir), "room", "actor")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := legacy.Close(); err != nil {
			t.Error(err)
		}
	})
	rows, _, err := legacy.GetFiltered(NewFilteringOpts(filters.Match("room", "one")))
	if err != nil || len(rows) != 4 {
		t.Fatalf("legacy read after abort: rows=%d err=%v", len(rows), err)
	}
}

func TestRebuildPreservesUnownedStagingAndChangedConfiguration(t *testing.T) {
	for _, foreign := range []bool{true, false} {
		t.Run(fmt.Sprintf("foreign=%t", foreign), func(t *testing.T) {
			dir := t.TempDir()
			legacySortedStore(t, dir)
			db := reopenSortedStore(t, dir)
			if foreign {
				if err := db.db.Transaction(func(tx backend.Transaction) error {
					bucket, err := tx.GetOrCreateBucket(relationshipAlternateKey)
					if err != nil {
						return err
					}

					return bucket.Put([]byte("foreign"), []byte("preserve"))
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := db.RebuildRelationships(context.Background(), 1); err != nil {
					t.Fatal(err)
				}

				db.relationships = [][]byte{[]byte("actor"), []byte("room")}
			}

			var before []byte
			if err := db.db.ReadTransaction(func(tx backend.Transaction) error {
				before = bytes.Clone(tx.GetBucket(metaBktKey).Get(relationshipLayoutKey))
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			page, err := db.RebuildRelationships(context.Background(), 1)
			if err == nil || page != (RelationshipRebuildPage{}) {
				t.Fatalf("conflicting generation accepted: %+v %v", page, err)
			}

			if err := db.db.ReadTransaction(func(tx backend.Transaction) error {
				if !bytes.Equal(before, tx.GetBucket(metaBktKey).Get(relationshipLayoutKey)) {
					return errors.New("refusal changed layout/checkpoint")
				}

				if foreign && string(tx.GetBucket(relationshipAlternateKey).Get([]byte("foreign"))) != "preserve" {
					return errors.New("refusal changed unowned staging")
				}

				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRebuildCutoverCannotBeAborted(t *testing.T) {
	dir := t.TempDir()
	legacySortedStore(t, dir)
	db := reopenSortedStore(t, dir)
	page, err := db.RebuildRelationships(context.Background(), 4)
	if err != nil || page.Scanned != 4 || page.Phase != "retiring" {
		t.Fatalf("cutover: %+v %v", page, err)
	}

	if page, err := db.AbortRelationshipRebuild(context.Background(), 1); err == nil || page != (RelationshipRebuildPage{}) {
		t.Fatalf("committed generation was aborted: %+v %v", page, err)
	}

	if got := orderedIDs(t, db, OrderedReadOptions{}); !slices.Equal(got, []string{"id-3", "id-1", "id-2", "id-0"}) {
		t.Fatalf("abort refusal damaged active generation: %v", got)
	}

	finishRelationshipRebuild(t, db, 1)
}
