package mojura

import (
	"bytes"

	"github.com/mojura/backend"
	"github.com/mojura/mojura/filters"
)

var _ filterCursor = &baseComparisonCursor[Entry, *Entry]{}

func newBaseComparisonCursor[T any, V Value[T]](txn *Transaction[T, V], f *filters.ComparisonFilter) (cur *baseComparisonCursor[T, V], err error) {
	var c baseComparisonCursor[T, V]
	parent := txn.txn.GetBucket(entriesBktKey)
	c.cur = parent.Cursor()

	c.txn = txn
	c.isMatch = filters.ComparisonFn(f.Comparison)

	c.rangeStart = []byte(f.RangeStart)
	c.rangeEnd = []byte(f.RangeEnd)
	cur = &c
	return
}

type baseComparisonCursor[T any, V Value[T]] struct {
	txn *Transaction[T, V]

	cur backend.Cursor

	rangeStart []byte
	rangeEnd   []byte

	isMatch filters.ComparisonFn
}

func (c *baseComparisonCursor[T, V]) rangeStartCheck(entryID []byte) (ok bool) {
	if len(c.rangeStart) == 0 {
		return true
	}

	return bytes.Compare(c.rangeStart, entryID) != 1
}

func (c *baseComparisonCursor[T, V]) rangeEndCheck(entryID []byte) (ok bool) {
	if len(c.rangeEnd) == 0 {
		return true
	}

	return bytes.Compare(c.rangeEnd, entryID) != -1
}

func (c *baseComparisonCursor[T, V]) getCurrentRelationshipID() (relationshipID string) {
	return string("")
}

func (c *baseComparisonCursor[T, V]) next() (entryID []byte, err error) {
	if c.cur == nil {
		err = Break
		return
	}

	// Set next entry ID
	entryID, _ = c.cur.Next()
	switch {
	// Ensure the entry exists
	case len(entryID) == 0:
		err = Break
		return
	// Ensure the current relationship ID does not exceed the start range
	case !c.rangeEndCheck(entryID):
		entryID = nil
		err = Break
		return
	}

	return
}

func (c *baseComparisonCursor[T, V]) nextUntilMatch(entryID []byte) (matchingEntryID []byte, err error) {
	var isMatch bool
	for err == nil {
		// This []byte -> string conversion should be non-existent after the compier pass
		isMatch, err = c.isMatch(string(entryID))
		switch {
		case err != nil:
			return
		case isMatch:
			matchingEntryID = entryID
			return
		default:
			entryID, err = c.next()
		}
	}

	return
}

func (c *baseComparisonCursor[T, V]) prev() (entryID []byte, err error) {
	if c.cur == nil {
		err = Break
		return
	}

	// Set previous entry ID
	entryID, _ = c.cur.Prev()
	switch {
	// Ensure the entry exists
	case len(entryID) == 0:
		err = Break
		return

	// Ensure the current relationship ID does not exceed the start range
	case !c.rangeStartCheck(entryID):
		entryID = nil
		err = Break
		return
	}

	return
}

func (c *baseComparisonCursor[T, V]) prevUntilMatch(entryID []byte) (matchingEntryID []byte, err error) {
	var isMatch bool
	for err == nil {
		// This []byte -> string conversion should be non-existent after the compier pass
		isMatch, err = c.isMatch(string(entryID))
		switch {
		case err != nil:
			return
		case isMatch:
			matchingEntryID = entryID
			return
		default:
			entryID, err = c.prev()
		}
	}

	return
}

func (c *baseComparisonCursor[T, V]) first() (entryID []byte, err error) {
	if entryID, _ = c.cur.First(); entryID == nil {
		err = Break
		return
	}

	return
}

func (c *baseComparisonCursor[T, V]) last() (entryID []byte, err error) {
	if entryID, _ = c.cur.Last(); entryID == nil {
		err = Break
		return
	}

	return
}

func (c *baseComparisonCursor[T, V]) hasForward(entryID []byte) (ok bool, err error) {
	var iteratingEntryID []byte
	if iteratingEntryID, err = c.first(); err != nil {
		return
	}

	if iteratingEntryID, err = c.nextUntilMatch(iteratingEntryID); err != nil {
		return
	}

	for {
		if bytes.Equal(entryID, iteratingEntryID) {
			ok = true
			return
		}

		if iteratingEntryID, err = c.next(); err != nil {
			return
		}

		if iteratingEntryID, err = c.nextUntilMatch(iteratingEntryID); err != nil {
			return
		}
	}
}

func (c *baseComparisonCursor[T, V]) hasReverse(entryID []byte) (ok bool, err error) {
	var iteratingEntryID []byte
	if iteratingEntryID, err = c.last(); err != nil {
		return
	}

	if iteratingEntryID, err = c.prevUntilMatch(iteratingEntryID); err != nil {
		return
	}

	for {
		if bytes.Equal(entryID, iteratingEntryID) {
			ok = true
			return
		}

		if iteratingEntryID, err = c.prev(); err != nil {
			return
		}

		if iteratingEntryID, err = c.prevUntilMatch(iteratingEntryID); err != nil {
			return
		}
	}
}

// SeekForward will seek the provided ID in a forward direction
func (c *baseComparisonCursor[T, V]) SeekForward(relationshipID, seekID []byte) (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	entryID, _ = c.cur.Seek([]byte(seekID))
	if entryID == nil {
		err = Break
		return
	}

	return c.nextUntilMatch(entryID)
}

// SeekReverse will seek the provided ID in a reverse direction
func (c *baseComparisonCursor[T, V]) SeekReverse(relationshipID, seekID []byte) (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	entryID, _ = c.cur.Seek([]byte(seekID))
	if entryID == nil {
		err = Break
		return
	}

	return c.prevUntilMatch(entryID)
}

// First will return the first entry
func (c *baseComparisonCursor[T, V]) First() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if entryID, err = c.first(); err != nil {
		return
	}

	return c.nextUntilMatch(entryID)

}

// Next will return the next entry
func (c *baseComparisonCursor[T, V]) Next() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if entryID, err = c.next(); err != nil {
		return
	}

	return c.nextUntilMatch(entryID)
}

// Prev will return the previous entry
func (c *baseComparisonCursor[T, V]) Prev() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if entryID, err = c.prev(); err != nil {
		return
	}

	return c.prevUntilMatch(entryID)
}

// Last will return the last entry
func (c *baseComparisonCursor[T, V]) Last() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if entryID, err = c.last(); err != nil {
		return
	}

	return c.prevUntilMatch(entryID)
}

// HasForward will determine if an entry exists in a forward direction
func (c *baseComparisonCursor[T, V]) HasForward(entryID []byte) (ok bool, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if ok, err = c.hasForward(entryID); err == Break {
		err = nil
	}

	return
}

// HasReverse will determine if an entry exists in a reverse direction
func (c *baseComparisonCursor[T, V]) HasReverse(entryID []byte) (ok bool, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if ok, err = c.hasReverse(entryID); err == Break {
		err = nil
	}

	return
}

func (c *baseComparisonCursor[T, V]) teardown() {

}
