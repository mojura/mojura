package mojura

import (
	"bytes"

	"github.com/mojura/backend"
)

// OrderedID is a stable (sort key, entry ID) position, independent of whether
// its record still exists. Returned positions own their bytes.
type OrderedID struct {
	SortKey []byte
	EntryID string
}

// OrderedReadOptions bounds an intersection before its limit. After and Before
// are exclusive tuple bounds in ascending order, also when Reverse is true.
// A nonpositive Limit is unlimited; a visitor may always stop with Break.
type OrderedReadOptions struct {
	After   *OrderedID
	Before  *OrderedID
	Reverse bool
	Limit   int
}

func orderedBehind(a, b []byte, reverse bool) bool {
	if reverse {
		return bytes.Compare(a, b) > 0
	}

	return bytes.Compare(a, b) < 0
}

func orderedNext(cursor backend.Cursor, reverse bool) []byte {
	if reverse {
		key, _ := cursor.Prev()
		return key
	}

	key, _ := cursor.Next()
	return key
}

func orderedStart(cursor backend.Cursor, bound []byte, reverse bool) []byte {
	if bound == nil {
		if reverse {
			key, _ := cursor.Last()
			return key
		}

		key, _ := cursor.First()
		return key
	}

	key, _ := cursor.Seek(bound)
	if reverse {
		if key == nil {
			key, _ = cursor.Last()
			return key
		}

		key, _ = cursor.Prev()
		return key
	}

	if bytes.Equal(key, bound) {
		key, _ = cursor.Next()
	}

	return key
}

func orderedStepTo(cursor backend.Cursor, target []byte, reverse bool) []byte {
	key := orderedNext(cursor, reverse)
	if key == nil || !orderedBehind(key, target, reverse) {
		return key
	}

	key, _ = cursor.Seek(target)
	if reverse {
		if key == nil {
			key, _ = cursor.Last()
		} else if bytes.Compare(key, target) > 0 {
			key, _ = cursor.Prev()
		}
	}

	return key
}
