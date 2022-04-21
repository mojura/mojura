package mojura

import (
	"bytes"

	"github.com/mojura/backend"
	"github.com/mojura/mojura/filters"
)

var _ filterCursor = &comparisonCursor[Entry, *Entry]{}

func newComparisonCursor[T any, V Value[T]](txn *Transaction[T, V], f *filters.ComparisonFilter) (fc filterCursor, err error) {
	if len(f.RelationshipKey) == 0 {
		return newBaseComparisonCursor(txn, f)
	}

	return newKeyComparisonCursor(txn, f)
}

func newKeyComparisonCursor[T any, V Value[T]](txn *Transaction[T, V], f *filters.ComparisonFilter) (cur *comparisonCursor[T, V], err error) {
	var c comparisonCursor[T, V]
	if c.parent, err = txn.getRelationshipBucket([]byte(f.RelationshipKey)); err != nil {
		return
	}

	c.txn = txn
	c.bktCur = c.parent.Cursor()
	c.isMatch = filters.ComparisonFn(f.Comparison)

	c.rangeStart = []byte(f.RangeStart)
	c.rangeEnd = []byte(f.RangeEnd)
	cur = &c
	return
}

type comparisonCursor[T any, V Value[T]] struct {
	txn *Transaction[T, V]

	parent backend.Bucket
	bktCur backend.Cursor
	cur    backend.Cursor

	rangeStart            []byte
	rangeEnd              []byte
	currentRelationshipID []byte

	isMatch filters.ComparisonFn
}

func (c *comparisonCursor[T, V]) rangeStartCheck() (ok bool) {
	if len(c.rangeStart) == 0 {
		return true
	}

	return bytes.Compare(c.rangeStart, c.currentRelationshipID) != 1
}

func (c *comparisonCursor[T, V]) rangeEndCheck() (ok bool) {
	if len(c.rangeEnd) == 0 {
		return true
	}

	return bytes.Compare(c.rangeEnd, c.currentRelationshipID) != -1
}

func (c *comparisonCursor[T, V]) getCurrentRelationshipID() (relationshipID string) {
	return string(c.currentRelationshipID)
}

func (c *comparisonCursor[T, V]) next() (entryID []byte, err error) {
	if c.cur == nil {
		err = Break
		return
	}

	// Set next entry ID
	entryID, _ = c.cur.Next()

	// While entry ID is unset
	for entryID == nil {
		// Set next cursor
		if err = c.setNextCursor(); err != nil {
			return
		}

		// Set entry ID as the first entry of the current cursor
		entryID, _ = c.cur.First()
	}

	// Ensure the current relationship ID does not exceed the end range
	if !c.rangeEndCheck() {
		entryID = nil
		err = Break
		return
	}

	return
}

func (c *comparisonCursor[T, V]) nextBucket() (bktKey []byte, err error) {
	fn := c.bktCur.Next
	if c.cur == nil {
		fn = c.bktCur.First
	}

	if bktKey, _ = fn(); bktKey == nil {
		err = Break
		return
	}

	return
}

func (c *comparisonCursor[T, V]) setNextCursor() (err error) {
	var bktKey []byte
	if bktKey, err = c.nextBucket(); err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt = c.parent.GetBucket(bktKey); bkt == nil {
		err = Break
		return
	}

	c.currentRelationshipID = bktKey
	c.cur = bkt.Cursor()
	return
}

func (c *comparisonCursor[T, V]) nextUntilMatch(entryID []byte) (matchingEntryID []byte, err error) {
	var isMatch bool
	for err == nil {
		// This []byte -> string conversion should be non-existent after the compier pass
		isMatch, err = c.isMatch(string(c.currentRelationshipID))
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

func (c *comparisonCursor[T, V]) prev() (entryID []byte, err error) {
	if c.cur == nil {
		err = Break
		return
	}

	// Set previous entry ID
	entryID, _ = c.cur.Prev()

	// While entry ID is unset
	for entryID == nil {
		// Set previous cursor
		if err = c.setPrevCursor(); err != nil {
			return
		}

		// Set entry ID as the last entry of the current cursor
		entryID, _ = c.cur.Last()
	}

	// Ensure the current relationship ID does not exceed the start range
	if !c.rangeStartCheck() {
		entryID = nil
		err = Break
		return
	}

	return
}

func (c *comparisonCursor[T, V]) prevBucket() (bktKey []byte, err error) {
	fn := c.bktCur.Prev
	if c.cur == nil {
		fn = c.bktCur.Last
	}

	if bktKey, _ = fn(); bktKey == nil {
		err = Break
		return
	}

	return
}

func (c *comparisonCursor[T, V]) setPrevCursor() (err error) {
	var bktKey []byte
	if bktKey, err = c.prevBucket(); err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt = c.parent.GetBucket(bktKey); bkt == nil {
		err = Break
		return
	}

	c.currentRelationshipID = bktKey
	c.cur = bkt.Cursor()
	return
}

func (c *comparisonCursor[T, V]) prevUntilMatch(entryID []byte) (matchingEntryID []byte, err error) {
	var isMatch bool
	for err == nil {
		// This []byte -> string conversion should be non-existent after the compier pass
		isMatch, err = c.isMatch(string(c.currentRelationshipID))
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

func (c *comparisonCursor[T, V]) teardown() {
	c.txn = nil
	c.cur = nil
}

func (c *comparisonCursor[T, V]) firstBktKey() (bktKey []byte, err error) {
	if bktKey = c.rangeStart; len(bktKey) > 0 {
		return
	}

	if bktKey, _ = c.bktCur.First(); bktKey == nil {
		err = Break
		return
	}

	return
}

func (c *comparisonCursor[T, V]) first() (entryID []byte, err error) {
	var bktKey []byte
	if bktKey, err = c.firstBktKey(); err != nil {
		return
	}

	if err = c.setCursor(bktKey); err != nil {
		return
	}

	if entryID, _ = c.cur.First(); entryID == nil {
		err = Break
		return
	}

	return
}

func (c *comparisonCursor[T, V]) lastBktKey() (bktKey []byte, err error) {
	if bktKey = c.rangeEnd; len(bktKey) > 0 {
		return
	}

	if bktKey, _ = c.bktCur.Last(); bktKey == nil {
		err = Break
		return
	}

	return
}

func (c *comparisonCursor[T, V]) last() (entryID []byte, err error) {
	var bktKey []byte
	if bktKey, err = c.lastBktKey(); err != nil {
		return
	}

	if err = c.setCursor(bktKey); err != nil {
		return
	}

	if entryID, _ = c.cur.Last(); entryID == nil {
		err = Break
		return
	}

	return
}

func (c *comparisonCursor[T, V]) setCursor(relationshipID []byte) (err error) {
	if bytes.Equal(relationshipID, c.currentRelationshipID) {
		return
	}

	bktKey, _ := c.bktCur.Seek(relationshipID)
	if bktKey == nil {
		err = Break
		return
	}

	bkt := c.parent.GetBucket(bktKey)
	if bkt == nil {
		err = Break
		return
	}

	c.cur = bkt.Cursor()
	c.currentRelationshipID = bktKey
	return
}

func (c *comparisonCursor[T, V]) hasForward(entryID []byte) (ok bool, err error) {
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

func (c *comparisonCursor[T, V]) hasReverse(entryID []byte) (ok bool, err error) {
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
func (c *comparisonCursor[T, V]) SeekForward(relationshipID, seekID []byte) (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if err = c.setCursor(relationshipID); err != nil {
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
func (c *comparisonCursor[T, V]) SeekReverse(relationshipID, seekID []byte) (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if err = c.setCursor(relationshipID); err != nil {
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
func (c *comparisonCursor[T, V]) First() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if entryID, err = c.first(); err != nil {
		return
	}

	return c.nextUntilMatch(entryID)

}

// Next will return the next entry
func (c *comparisonCursor[T, V]) Next() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if entryID, err = c.next(); err != nil {
		return
	}

	return c.nextUntilMatch(entryID)
}

// Prev will return the previous entry
func (c *comparisonCursor[T, V]) Prev() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if entryID, err = c.prev(); err != nil {
		return
	}

	return c.prevUntilMatch(entryID)
}

// Last will return the last entry
func (c *comparisonCursor[T, V]) Last() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if entryID, err = c.last(); err != nil {
		return
	}

	return c.prevUntilMatch(entryID)
}

// HasForward will determine if an entry exists in a forward direction
func (c *comparisonCursor[T, V]) HasForward(entryID []byte) (ok bool, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if ok, err = c.hasForward(entryID); err == Break {
		err = nil
	}

	return
}

// HasReverse will determine if an entry exists in a reverse direction
func (c *comparisonCursor[T, V]) HasReverse(entryID []byte) (ok bool, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if ok, err = c.hasReverse(entryID); err == Break {
		err = nil
	}

	return
}
