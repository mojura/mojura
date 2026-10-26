package mojura

import (
	"fmt"

	"github.com/mojura/mojura/filters"
)

func newFilterCursor[T Value](txn *Transaction[T], f Filter) (fc filterCursor, err error) {
	switch n := f.(type) {
	case *filters.MatchFilter:
		fc, err = newMatchCursor(txn, n)
	case *filters.InverseMatchFilter:
		fc, err = newInverseMatchCursor(txn, n)
	case *filters.ComparisonFilter:
		if n.RelationshipKey == "" {
			return newComparisonCursor(txn, n)
		}

		fc, err = newComparisonCursor(txn, n)
	default:
		err = fmt.Errorf("filter of %T is not supported", n)
		return
	}

	if err == nil && txn.m.sortKeyWidth > 0 {
		fc = &entryFilterCursor[T]{filterCursor: fc, txn: txn}
	}

	return
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
