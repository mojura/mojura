package mojura

import (
	"fmt"

	"github.com/mojura/mojura/filters"
)

func newFilterCursor[T Value](txn *Transaction[T], f Filter) (fc filterCursor, err error) {
	switch n := f.(type) {
	case *filters.MatchFilter:
		return newMatchCursor(txn, n)
	case *filters.InverseMatchFilter:
		return newInverseMatchCursor(txn, n)
	case *filters.ComparisonFilter:
		return newComparisonCursor(txn, n)
	default:
		err = fmt.Errorf("filter of %T is not supported", n)
		return
	}
}

type filterCursor interface {
	SeekForward(relationshipKey, seekID []byte) (entryID []byte, err error)
	SeekReverse(relationshipKey, seekID []byte) (entryID []byte, err error)

	First() (entryID []byte, err error)
	Last() (entryID []byte, err error)
	Next() (entryID []byte, err error)
	Prev() (entryID []byte, err error)

	HasForward(entryID []byte) (ok bool, err error)
	HasReverse(entryID []byte) (ok bool, err error)

	getCurrentRelationshipID() (relationshipID string)
	teardown()
}
