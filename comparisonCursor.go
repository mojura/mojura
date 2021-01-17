package mojura

import (
	"bytes"

	"github.com/mojura/backend"
	"github.com/mojura/mojura/filters"
)

var _ filterCursor = &comparisonCursor{}

func newComparisonCursor(txn *Transaction, f *filters.ComparisonFilter) (cur *comparisonCursor, err error) {
	var c comparisonCursor
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

type comparisonCursor struct {
	txn *Transaction

	parent backend.Bucket
	bktCur backend.Cursor
	cur    backend.Cursor

	rangeStart            []byte
	rangeEnd              []byte
	currentRelationshipID []byte

	isMatch filters.ComparisonFn
}

func (c *comparisonCursor) rangeStartCheck() (ok bool) {
	if len(c.rangeStart) == 0 {
		return true
	}

	return bytes.Compare(c.rangeStart, c.currentRelationshipID) != 1
}

func (c *comparisonCursor) rangeEndCheck() (ok bool) {
	if len(c.rangeEnd) == 0 {
		return true
	}

	return bytes.Compare(c.rangeEnd, c.currentRelationshipID) != -1
}

func (c *comparisonCursor) getCurrentRelationshipID() (relationshipID string) {
	return string(c.currentRelationshipID)
}

func (c *comparisonCursor) next() (entryID []byte, err error) {
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

func (c *comparisonCursor) nextBucket() (bktKey []byte, err error) {
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

func (c *comparisonCursor) setNextCursor() (err error) {
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

func (c *comparisonCursor) nextUntilMatch(entryID []byte) (matchingEntryID []byte, err error) {
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

func (c *comparisonCursor) prev() (entryID []byte, err error) {
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

func (c *comparisonCursor) prevBucket() (bktKey []byte, err error) {
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

func (c *comparisonCursor) setPrevCursor() (err error) {
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

func (c *comparisonCursor) prevUntilMatch(entryID []byte) (matchingEntryID []byte, err error) {
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

func (c *comparisonCursor) teardown() {
	c.txn = nil
	c.cur = nil
}

func (c *comparisonCursor) firstBktKey() (bktKey []byte, err error) {
	if bktKey = c.rangeStart; len(bktKey) > 0 {
		return
	}

	if bktKey, _ = c.bktCur.First(); bktKey == nil {
		err = Break
		return
	}

	return
}

func (c *comparisonCursor) first() (entryID []byte, err error) {
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

func (c *comparisonCursor) lastBktKey() (bktKey []byte, err error) {
	if bktKey = c.rangeEnd; len(bktKey) > 0 {
		return
	}

	if bktKey, _ = c.bktCur.Last(); bktKey == nil {
		err = Break
		return
	}

	return
}

func (c *comparisonCursor) last() (entryID []byte, err error) {
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

func (c *comparisonCursor) setCursor(relationshipID []byte) (err error) {
	if bytes.Compare(relationshipID, c.currentRelationshipID) == 0 {
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

func (c *comparisonCursor) hasForward(entryID []byte) (ok bool, err error) {
	var iteratingEntryID []byte
	if iteratingEntryID, err = c.first(); err != nil {
		return
	}

	if iteratingEntryID, err = c.nextUntilMatch(iteratingEntryID); err != nil {
		return
	}

	for {
		if bytes.Compare(entryID, iteratingEntryID) == 0 {
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

func (c *comparisonCursor) hasReverse(entryID []byte) (ok bool, err error) {
	var iteratingEntryID []byte
	if iteratingEntryID, err = c.last(); err != nil {
		return
	}

	if iteratingEntryID, err = c.prevUntilMatch(iteratingEntryID); err != nil {
		return
	}

	for {
		if bytes.Compare(entryID, iteratingEntryID) == 0 {
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
func (c *comparisonCursor) SeekForward(relationshipID, seekID []byte) (entryID []byte, err error) {
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
func (c *comparisonCursor) SeekReverse(relationshipID, seekID []byte) (entryID []byte, err error) {
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
func (c *comparisonCursor) First() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if entryID, err = c.first(); err != nil {
		return
	}

	return c.nextUntilMatch(entryID)

}

// Next will return the next entry
func (c *comparisonCursor) Next() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if entryID, err = c.next(); err != nil {
		return
	}

	return c.nextUntilMatch(entryID)
}

// Prev will return the previous entry
func (c *comparisonCursor) Prev() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if entryID, err = c.prev(); err != nil {
		return
	}

	return c.prevUntilMatch(entryID)
}

// Last will return the last entry
func (c *comparisonCursor) Last() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if entryID, err = c.last(); err != nil {
		return
	}

	return c.prevUntilMatch(entryID)
}

// HasForward will determine if an entry exists in a forward direction
func (c *comparisonCursor) HasForward(entryID []byte) (ok bool, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if ok, err = c.hasForward(entryID); err == Break {
		err = nil
	}

	return
}

// HasReverse will determine if an entry exists in a reverse direction
func (c *comparisonCursor) HasReverse(entryID []byte) (ok bool, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if ok, err = c.hasReverse(entryID); err == Break {
		err = nil
	}

	return
}
