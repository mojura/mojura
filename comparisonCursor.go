package mojura

import (
	"bytes"

	"github.com/mojura/backend"
)

var _ primaryCursor = &comparisonCursor{}

func newComparisonCursor(txn *Transaction, opts ComparisonOpts) (cur *comparisonCursor, err error) {
	var c comparisonCursor
	if c.parent, err = txn.getRelationshipBucket(opts.RelationshipKey); err != nil {
		return
	}

	c.txn = txn
	c.bktCur = c.parent.Cursor()
	c.isMatch = opts.Comparison

	c.rangeStart = opts.RangeStart
	c.rangeEnd = opts.RangeEnd
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

	isMatch ComparisonFn
}

func (c *comparisonCursor) next() (entryID []byte, err error) {
	if c.cur == nil {
		err = Break
		return
	}

	if entryID, _ = c.cur.Next(); entryID != nil {
		return
	}

	for entryID == nil {
		if err = c.setNextCursor(); err != nil {
			return
		}

		if entryID, _ = c.cur.First(); entryID != nil {
			return
		}
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
		isMatch, err = c.isMatch(c.currentRelationshipID)
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

	if entryID, _ = c.cur.Prev(); entryID != nil {
		return
	}

	for entryID == nil {
		if err = c.setPrevCursor(); err != nil {
			return
		}

		if entryID, _ = c.cur.Last(); entryID != nil {
			return
		}
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
		isMatch, err = c.isMatch(c.currentRelationshipID)
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

func (c *comparisonCursor) first() (entryID []byte, err error) {
	bktKey, _ := c.bktCur.First()
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

	if entryID, _ = c.cur.First(); entryID == nil {
		err = Break
		return
	}

	return
}

func (c *comparisonCursor) last() (entryID []byte, err error) {
	bktKey, _ := c.bktCur.Last()
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

	if entryID, _ = c.cur.Last(); entryID == nil {
		err = Break
		return
	}

	return
}

func (c *comparisonCursor) setBkt(relationshipID []byte) (err error) {
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

// SeekForward will seek the provided ID in a forward direction
func (c *comparisonCursor) SeekForward(relationshipID, seekID []byte) (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if err = c.setBkt(relationshipID); err != nil {
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

	if err = c.setBkt(relationshipID); err != nil {
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

// ComparisonFn is used for comparison filters
type ComparisonFn func(relationshipID []byte) (ok bool, err error)
