package mojura

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/mojura/mojura/filters"
)

func TestASortedCursorSeeksRelationshipBoundaryWithoutEntryID(t *testing.T) {
	db := openSortedRecords(t)
	seedSortedRecords(t, db)
	for _, tc := range []struct {
		name    string
		seek    string
		want    string
		err     error
		reverse bool
	}{
		{"same-relationship", "one::", "d", nil, false},
		{"next-relationship", "one\x00::", "decoy", nil, false},
		{"past-last-relationship", "other\x00::", "", Break, false},
		{"existing-entry", "one::b", "b", nil, false},
		{"missing-nonempty-entry", "one::missing", "", ErrEntryNotFound, false},
		{"reverse-existing-entry", "one::b", "b", nil, true},
		{"reverse-missing-nonempty-entry", "one::missing", "", ErrEntryNotFound, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := db.ReadTransaction(context.Background(), func(tx *Transaction[*sortedRecord]) error {
				cursor, err := tx.IDCursor(filters.Comparison("actor", func(string) (bool, error) { return true, nil }))
				if err != nil {
					return err
				}

				seek := cursor.Seek
				if tc.reverse {
					seek = cursor.SeekReverse
				}

				got, err := seek(tc.seek)
				if got != tc.want || !errors.Is(err, tc.err) {
					return fmt.Errorf("relationship-boundary seek %q = %q, %v; want %q, %v", tc.seek, got, err, tc.want, tc.err)
				}

				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
