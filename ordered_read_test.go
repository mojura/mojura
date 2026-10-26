package mojura

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/mojura/backend"
	"github.com/mojura/mojura/filters"
)

func orderedIDs(t *testing.T, db *Mojura[*sortedRecord], opts OrderedReadOptions) []string {
	t.Helper()
	var ids []string
	if err := db.ReadTransaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
		return tx.ForEachOrderedID(func(position OrderedID) error {
			ids = append(ids, position.EntryID)
			return nil
		}, opts, filters.Match("room", "one"), filters.Match("actor", "one"))
	}); err != nil {
		t.Fatal(err)
	}

	return ids
}

func orderedPosition(at int64, id string) *OrderedID {
	return &OrderedID{SortKey: (&sortedRecord{Entry: Entry{CreatedAt: at}}).GetSortKey(), EntryID: id}
}

func TestASortedSetSeeksToAPair(t *testing.T) {
	db := openSortedRecords(t)
	seedSortedRecords(t, db)
	for _, tc := range []struct {
		name string
		opts OrderedReadOptions
		want []string
	}{
		{"forward", OrderedReadOptions{}, []string{"d", "b", "a", "c"}},
		{"reverse", OrderedReadOptions{Reverse: true}, []string{"c", "a", "b", "d"}},
		{"after tie", OrderedReadOptions{After: orderedPosition(20, "a")}, []string{"c"}},
		{"before tie", OrderedReadOptions{Reverse: true, Before: orderedPosition(20, "c")}, []string{"a", "b", "d"}},
		{"before beyond last", OrderedReadOptions{Reverse: true, Before: orderedPosition(30, "z")}, []string{"c", "a", "b", "d"}},
		{"both bounds", OrderedReadOptions{After: orderedPosition(10, "b"), Before: orderedPosition(20, "c")}, []string{"a"}},
		{"limit", OrderedReadOptions{Reverse: true, Limit: 2}, []string{"c", "a"}},
		{"after last", OrderedReadOptions{After: orderedPosition(30, "z")}, nil},
		{"before first", OrderedReadOptions{Reverse: true, Before: orderedPosition(1, "a")}, nil},
		{"empty range", OrderedReadOptions{After: orderedPosition(20, "c"), Before: orderedPosition(10, "b")}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := orderedIDs(t, db, tc.opts); !slices.Equal(got, tc.want) {
				t.Fatalf("IDs = %v, want %v", got, tc.want)
			}
		})
	}

	if _, err := db.Delete("a"); err != nil {
		t.Fatal(err)
	}

	if got := orderedIDs(t, db, OrderedReadOptions{After: orderedPosition(20, "a")}); !slices.Equal(got, []string{"c"}) {
		t.Fatalf("deleted lower cursor skipped its successor: %v", got)
	}

	if got := orderedIDs(t, db, OrderedReadOptions{Reverse: true, Before: orderedPosition(20, "a")}); !slices.Equal(got, []string{"b", "d"}) {
		t.Fatalf("deleted upper cursor skipped its predecessor: %v", got)
	}
}

func TestAnUpdateMovesItsSortedMemberships(t *testing.T) {
	db := openSortedRecords(t)
	seedSortedRecords(t, db)
	if _, err := db.Update("a", func(record *sortedRecord) error { record.CreatedAt = 2; return nil }); err != nil {
		t.Fatal(err)
	}

	if got := orderedIDs(t, db, OrderedReadOptions{}); !slices.Equal(got, []string{"a", "d", "b", "c"}) {
		t.Fatalf("in-place key update left missing/stale memberships: %v", got)
	}

	if _, err := db.Update("a", func(record *sortedRecord) error { record.Actor = "other"; return nil }); err != nil {
		t.Fatal(err)
	}

	if got := orderedIDs(t, db, OrderedReadOptions{}); !slices.Equal(got, []string{"d", "b", "c"}) {
		t.Fatalf("membership update left a stale match: %v", got)
	}

	opts := NewFilteringOpts(filters.Match("room", "one"), filters.Match("actor", "one"))
	opts.Reverse = true
	values, _, err := db.GetFiltered(opts)
	if err != nil {
		t.Fatal(err)
	}

	var ids []string
	for _, value := range values {
		ids = append(ids, value.ID)
	}

	if !slices.Equal(ids, []string{"c", "b", "d"}) {
		t.Fatalf("legacy value cursor did not resolve sorted membership IDs: %v", ids)
	}
}

func TestSortedIDOnlyWalkDoesNotCopyUnusedSortKeys(t *testing.T) {
	db := openSortedRecords(t)
	seedSortedRecords(t, db)
	want := []string{"d", "b", "a", "c"}
	fs := []Filter{filters.Match("room", "one"), filters.Match("actor", "one")}
	var idAllocations, tupleAllocations float64
	err := db.ReadTransaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
		ids := make([]string, 0, len(want))
		var walkErr error
		idAllocations = testing.AllocsPerRun(5, func() {
			ids = ids[:0]
			walkErr = tx.ForEachIntersectedID(func(id string) error { ids = append(ids, id); return nil }, fs...)
		})
		if walkErr != nil {
			return walkErr
		}

		if !slices.Equal(ids, want) {
			return fmt.Errorf("ID-only result = %v, want %v", ids, want)
		}

		tupleAllocations = testing.AllocsPerRun(5, func() {
			ids = ids[:0]
			walkErr = tx.ForEachOrderedID(func(position OrderedID) error { ids = append(ids, position.EntryID); return nil }, OrderedReadOptions{}, fs...)
		})
		if walkErr != nil {
			return walkErr
		}

		if !slices.Equal(ids, want) {
			return fmt.Errorf("tuple result = %v, want %v", ids, want)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("ID-only allocations=%v tuple allocations=%v yielded=%d", idAllocations, tupleAllocations, len(want))
	if tupleAllocations-idAllocations != float64(len(want)) {
		t.Fatalf("ID-only route must omit exactly one unused sort-key allocation per yield: ID=%v tuple=%v yielded=%d", idAllocations, tupleAllocations, len(want))
	}
}

func TestOrderedPositionsOwnTheirSortKeys(t *testing.T) {
	db := openSortedRecords(t)
	seedSortedRecords(t, db)
	err := db.ReadTransaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
		parent, err := tx.getRelationshipBucket([]byte("room"))
		if err != nil {
			return err
		}

		cursor := parent.GetBucket([]byte("one")).Cursor()
		return tx.ForEachOrderedID(func(position OrderedID) error {
			key := append(slices.Clone(position.SortKey), position.EntryID...)
			stored, _ := cursor.Seek(key)
			if len(position.SortKey) != tx.m.sortKeyWidth || len(stored) < len(position.SortKey) {
				return fmt.Errorf("invalid position for %q", position.EntryID)
			}

			if &stored[0] == &position.SortKey[0] {
				return fmt.Errorf("position %q borrows the backend sort key", position.EntryID)
			}

			return nil
		}, OrderedReadOptions{}, filters.Match("room", "one"), filters.Match("actor", "one"))
	})
	if err != nil {
		t.Fatal(err)
	}
}

type orderedCounts struct{ steps, seeks, gets int }
type orderedCountBackend struct {
	backend.Backend
	counts *orderedCounts
}
type orderedCountTx struct {
	backend.Transaction
	counts *orderedCounts
}
type orderedCountBucket struct {
	backend.Bucket
	counts  *orderedCounts
	entries bool
}
type orderedCountCursor struct {
	backend.Cursor
	counts *orderedCounts
}

func (b orderedCountBackend) ReadTransaction(fn func(backend.Transaction) error) error {
	return b.Backend.ReadTransaction(func(tx backend.Transaction) error { return fn(orderedCountTx{tx, b.counts}) })
}

func (tx orderedCountTx) GetBucket(key []byte) backend.Bucket {
	bucket := tx.Transaction.GetBucket(key)
	if bucket == nil {
		return nil
	}
	return orderedCountBucket{bucket, tx.counts, string(key) == string(entriesBktKey)}
}

func (b orderedCountBucket) GetBucket(key []byte) backend.Bucket {
	bucket := b.Bucket.GetBucket(key)
	if bucket == nil {
		return nil
	}
	return orderedCountBucket{bucket, b.counts, false}
}

func (b orderedCountBucket) Get(key []byte) []byte {
	if b.entries {
		b.counts.gets++
	}
	return b.Bucket.Get(key)
}

func (b orderedCountBucket) Cursor() backend.Cursor {
	return orderedCountCursor{b.Bucket.Cursor(), b.counts}
}
func (c orderedCountCursor) First() ([]byte, []byte) { c.counts.steps++; return c.Cursor.First() }
func (c orderedCountCursor) Last() ([]byte, []byte)  { c.counts.steps++; return c.Cursor.Last() }
func (c orderedCountCursor) Next() ([]byte, []byte)  { c.counts.steps++; return c.Cursor.Next() }
func (c orderedCountCursor) Prev() ([]byte, []byte)  { c.counts.steps++; return c.Cursor.Prev() }
func (c orderedCountCursor) Seek(key []byte) ([]byte, []byte) {
	c.counts.seeks++
	return c.Cursor.Seek(key)
}

func TestSortedDensePageDoesNotReadOlderMatches(t *testing.T) {
	for _, population := range []int{80, 160, 400} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			db := openSortedRecords(t)
			if err := db.Transaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
				for i := 0; i < population; i++ {
					id, at := fmt.Sprintf("old-%04d", i), int64(i+1)
					if i < 80 {
						id, at = fmt.Sprintf("page-%02d", i), int64(10000+i/2)
					}
					if _, err := tx.Put(id, &sortedRecord{Entry: Entry{CreatedAt: at}, Room: "one", Actor: "one"}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			var counts orderedCounts
			db.db = orderedCountBackend{db.db, &counts}
			got := orderedIDs(t, db, OrderedReadOptions{Reverse: true, Limit: 80})
			want := make([]string, 80)
			for i := range want {
				want[i] = fmt.Sprintf("page-%02d", 79-i)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("fixed last page changed: %v", got)
			}
			if counts.gets != 0 || counts.steps != 160 || counts.seeks != 0 {
				t.Fatalf("population=%d: Gets=%d steps=%d seeks=%d, want 0/160/0", population, counts.gets, counts.steps, counts.seeks)
			}
			t.Logf("population=%d limit=80 entries_gets=%d cursor_steps=%d seeks=%d", population, counts.gets, counts.steps, counts.seeks)
		})
	}
}

func TestSortedSparsePageSeeksPastUnrelatedGrowth(t *testing.T) {
	for _, population := range []int{80, 400} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			db := openSortedRecords(t)
			if err := db.Transaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
				for i := 0; i < population; i++ {
					if _, err := tx.Put(fmt.Sprintf("noise-%04d", i), &sortedRecord{Entry: Entry{CreatedAt: int64(200 + i*10)}, Room: "one", Actor: "other"}); err != nil {
						return err
					}
				}
				for i, at := range []int64{5, 100, 2000, 10000} {
					if _, err := tx.Put(fmt.Sprintf("rare-%d", i), &sortedRecord{Entry: Entry{CreatedAt: at}, Room: "one", Actor: "one"}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			var counts orderedCounts
			db.db = orderedCountBackend{db.db, &counts}
			if got := orderedIDs(t, db, OrderedReadOptions{Limit: 4}); !slices.Equal(got, []string{"rare-0", "rare-1", "rare-2", "rare-3"}) {
				t.Fatalf("selective IDs changed: %v", got)
			}
			if counts.gets != 0 || counts.steps+counts.seeks > 24 || counts.seeks == 0 {
				t.Fatalf("population=%d: Gets=%d steps=%d seeks=%d; want point seeks proportional to four matches", population, counts.gets, counts.steps, counts.seeks)
			}
			t.Logf("population=%d limit=4 entries_gets=%d cursor_steps=%d seeks=%d", population, counts.gets, counts.steps, counts.seeks)
		})
	}
}

func TestSortedReadRejectsInvalidFiltersAndBounds(t *testing.T) {
	db := openSortedRecords(t)
	seedSortedRecords(t, db)
	if err := db.ReadTransaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
		visitor := func(OrderedID) error { t.Error("invalid request yielded a record"); return nil }
		for _, fs := range [][]Filter{nil, {filters.InverseMatch("actor", "one")}, {filters.Match("actor", "missing"), (*filters.MatchFilter)(nil)}} {
			if err := tx.ForEachOrderedID(visitor, OrderedReadOptions{}, fs...); !errors.Is(err, ErrIntersectionNeedsMatchFilters) {
				return fmt.Errorf("invalid filters returned %v", err)
			}
		}

		for _, opts := range []OrderedReadOptions{
			{After: &OrderedID{SortKey: []byte{1}, EntryID: "a"}},
			{Before: &OrderedID{SortKey: []byte{1}, EntryID: "a"}},
		} {
			if err := tx.ForEachOrderedID(visitor, opts, filters.Match("room", "one")); !errors.Is(err, ErrInvalidSortKey) {
				return fmt.Errorf("invalid cursor returned %v", err)
			}
		}

		nilVisitorErr := func() (result error) {
			defer func() {
				if recovered := recover(); recovered != nil {
					result = fmt.Errorf("nil visitor panicked instead of refusing: %v", recovered)
				}
			}()
			return tx.ForEachOrderedID(nil, OrderedReadOptions{}, filters.Match("room", "one"))
		}()
		if nilVisitorErr == nil || nilVisitorErr.Error() != "ordered read requires a visitor" {
			return fmt.Errorf("nil visitor returned %v", nilVisitorErr)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSortedReadChecksEveryIndexBeforeAnEmptyResult(t *testing.T) {
	db := openSortedRecords(t)
	seedSortedRecords(t, db)
	for _, tc := range []struct {
		name, index string
		wantError   error
	}{
		{"known empty", "actor", nil},
		{"unknown second index", "unknown-index", ErrRelationshipNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			visits := 0
			err := db.ReadTransaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
				return tx.ForEachOrderedID(func(OrderedID) error { visits++; return nil }, OrderedReadOptions{}, filters.Match("room", "absent"), filters.Match(tc.index, "one"))
			})
			if !errors.Is(err, tc.wantError) || visits != 0 {
				t.Fatalf("empty first set: err=%v want=%v visits=%d", err, tc.wantError, visits)
			}
		})
	}
}

func TestSortedReadRefusesACanceledCallerBeforeVisiting(t *testing.T) {
	db := openSortedRecords(t)
	seedSortedRecords(t, db)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	visits := 0
	// Exercise the reader's cancellation arm without the public transaction
	// scheduler rejecting the context before this callback can enter.
	err := db.db.ReadTransaction(func(raw backend.Transaction) error {
		tx := newTransaction(ctx, db, raw, nil)
		return tx.ForEachOrderedID(func(OrderedID) error { visits++; return nil }, OrderedReadOptions{}, filters.Match("room", "one"))
	})
	if !errors.Is(err, context.Canceled) || visits != 0 {
		t.Fatalf("canceled caller: err=%v visits=%d; want canceled with no visitor", err, visits)
	}
}

func TestOrderedReadRefusesAnUnsortedValue(t *testing.T) {
	db := openTempMojura(t)
	visits := 0
	err := db.ReadTransaction(context.Background(), func(tx *Transaction[*testStruct]) error {
		return tx.ForEachOrderedID(func(OrderedID) error { visits++; return nil }, OrderedReadOptions{}, filters.Match("tags", "one"))
	})
	if !errors.Is(err, ErrInvalidSortKey) || visits != 0 {
		t.Fatalf("unsorted ordered read: err=%v visits=%d", err, visits)
	}
}

func TestSortedReverseIntersectionSeeksPastNonmembers(t *testing.T) {
	db := openSortedRecords(t)
	err := db.Transaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
		for _, row := range []struct {
			id    string
			at    int64
			room  string
			actor string
		}{
			{"room-100", 100, "one", "other"}, {"room-90", 90, "one", "other"},
			{"room-60", 60, "one", "other"}, {"actor-80", 80, "other", "one"},
			{"actor-70", 70, "other", "one"}, {"actor-40", 40, "other", "one"},
			{"shared-30", 30, "one", "one"}, {"shared-10", 10, "one", "one"},
		} {
			if _, putErr := tx.Put(row.id, &sortedRecord{Entry: Entry{CreatedAt: row.at}, Room: row.room, Actor: row.actor}); putErr != nil {
				return putErr
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	var counts orderedCounts
	db.db = orderedCountBackend{db.db, &counts}
	got := orderedIDs(t, db, OrderedReadOptions{Reverse: true})
	if !slices.Equal(got, []string{"shared-30", "shared-10"}) || counts.seeks == 0 || counts.gets != 0 {
		t.Fatalf("reverse sparse intersection: IDs=%v seeks=%d entryGets=%d", got, counts.seeks, counts.gets)
	}
}

func TestSortedReadCancellationDoesNotAdvanceCursors(t *testing.T) {
	db := openSortedRecords(t)
	seedSortedRecords(t, db)
	var counts orderedCounts
	db.db = orderedCountBackend{db.db, &counts}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	visits, atCancel := 0, 0
	// Count this reader's own operations, not the legacy public transaction
	// callback scheduler's cancellation/teardown behavior.
	err := db.db.ReadTransaction(func(raw backend.Transaction) error {
		tx := newTransaction(ctx, db, raw, nil)
		return tx.ForEachOrderedID(func(OrderedID) error {
			visits++
			atCancel = counts.steps + counts.seeks
			cancel()
			return nil
		}, OrderedReadOptions{}, filters.Match("room", "one"), filters.Match("actor", "one"))
	})
	if !errors.Is(err, context.Canceled) || visits != 1 || counts.steps+counts.seeks != atCancel {
		t.Fatalf("cancelled visitor: err=%v visits=%d moves=%d at_cancel=%d", err, visits, counts.steps+counts.seeks, atCancel)
	}
}

func TestSortedReadPreservesVisitorFailure(t *testing.T) {
	db := openSortedRecords(t)
	seedSortedRecords(t, db)
	refusal := errors.New("visitor refusal")
	visits := 0
	err := db.ReadTransaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
		return tx.ForEachOrderedID(func(OrderedID) error { visits++; return refusal }, OrderedReadOptions{}, filters.Match("room", "one"))
	})
	if err != refusal || visits != 1 {
		t.Fatalf("visitor error changed: err=%v visits=%d", err, visits)
	}
}
