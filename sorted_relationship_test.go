package mojura

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/mojura/kiroku"
	"github.com/mojura/mojura/filters"
)

type sortedRecord struct {
	Entry
	Room  string
	Actor string
}

func (r *sortedRecord) GetRelationships() Relationships {
	return Relationships{{r.Room}, {r.Actor}}
}

func (r *sortedRecord) GetSortKey() []byte {
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], uint64(r.CreatedAt))
	return key[:]
}

func openSortedRecords(t *testing.T) *Mojura[*sortedRecord] {
	t.Helper()
	dir := t.TempDir()
	opts := MakeOpts("sorted", dir)
	source, err := kiroku.NewIOSource(dir)
	if err != nil {
		t.Fatal(err)
	}

	opts.Source = source
	db, err := New[*sortedRecord](opts, "room", "actor")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func seedSortedRecords(t *testing.T, db *Mojura[*sortedRecord]) {
	t.Helper()
	rows := []struct {
		id    string
		time  int64
		actor string
	}{
		{"c", 20, "one"},
		{"a", 20, "one"},
		{"d", 5, "one"},
		{"b", 10, "one"},
		{"decoy", 1, "other"},
	}
	if err := db.Transaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
		for _, row := range rows {
			record := &sortedRecord{Entry: Entry{CreatedAt: row.time}, Room: "one", Actor: row.actor}
			if _, err := tx.Put(row.id, record); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestASortedSetWalksInSortKeyOrder(t *testing.T) {
	db := openSortedRecords(t)
	seedSortedRecords(t, db)
	fs := []Filter{filters.Match("room", "one"), filters.Match("actor", "one")}
	if err := db.ReadTransaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
		var ids []string
		if err := tx.ForEachIntersectedID(func(id string) error {
			ids = append(ids, id)
			return nil
		}, fs...); err != nil {
			return err
		}

		if want := []string{"d", "b", "a", "c"}; !slices.Equal(ids, want) {
			return fmt.Errorf("sorted membership order = %v, want %v (created_at, entry ID)", ids, want)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestASortedSetStopsAtItsVisitorBound(t *testing.T) {
	db := openSortedRecords(t)
	seedSortedRecords(t, db)
	if err := db.ReadTransaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
		var ids []string
		if err := tx.ForEachIntersectedID(func(id string) error {
			ids = append(ids, id)
			if len(ids) == 2 {
				return Break
			}

			return nil
		}, filters.Match("room", "one"), filters.Match("actor", "one")); err != nil {
			return err
		}

		if want := []string{"d", "b"}; !slices.Equal(ids, want) {
			return fmt.Errorf("bounded sorted page = %v, want %v; filters must precede the bound", ids, want)
		}

		_, err := tx.Get("c")
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
