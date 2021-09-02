package mojura

import (
	"bytes"

	"github.com/mojura/backend"
	"github.com/mojura/mojura/filters"
)

var (
	_ filterCursor = &inverseMatchCursor{}
)

func newInverseMatchCursor(txn *Transaction, f *filters.InverseMatchFilter) (cur *inverseMatchCursor, err error) {
	var c inverseMatchCursor
	if c.parent, err = txn.getRelationshipBucket([]byte(f.RelationshipKey)); err != nil {
		return
	}

	c.txn = txn
	c.bktCur = c.parent.Cursor()
	c.targetRelationshipID = []byte(f.RelationshipID)

	if matchBkt := c.parent.GetBucket([]byte(f.RelationshipID)); matchBkt != nil {
		c.matchCur = matchBkt.Cursor()
	}

	cur = &c
	return
}

type inverseMatchCursor struct {
	txn *Transaction

	parent   backend.Bucket
	bktCur   backend.Cursor
	cur      backend.Cursor
	matchCur backend.Cursor

	targetRelationshipID  []byte
	currentRelationshipID []byte
}

func (c *inverseMatchCursor) has(entryID []byte) (ok bool, err error) {
	if c.matchCur == nil {
		// If match cursor does not exist, that means that the target bucket does not exist.
		// Because this cursor is an inverse cursor, this means that all values will not match
		// this relationship key and we can automatically return true
		return true, nil
	}

	// Get the first key matching entryID (will get next key if entryID does not exist)
	firstKey, _ := c.matchCur.Seek(entryID)
	// If the first key matches the entry ID, we have a match
	ok = !bytes.Equal(entryID, firstKey)
	return
}

func (c *inverseMatchCursor) getCurrentRelationshipID() (relationshipID string) {
	return string(c.currentRelationshipID)
}

func (c *inverseMatchCursor) next() (entryID []byte, err error) {
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

	return
}

func (c *inverseMatchCursor) nextBucket() (bktKey []byte, err error) {
	fn := c.bktCur.Next
	if c.cur == nil {
		fn = c.bktCur.First
	}

	for {
		bktKey, _ = fn()
		switch {
		case bytes.Equal(bktKey, c.targetRelationshipID):
		case bktKey == nil:
			err = Break
			return

		default:
			return
		}
	}
}

func (c *inverseMatchCursor) setNextAvailableCursor() (err error) {
	for {
		if err = c.setNextCursor(); err != nil {
			return
		}

		if !bytes.Equal(c.targetRelationshipID, c.currentRelationshipID) {
			return
		}
	}
}

func (c *inverseMatchCursor) setNextCursor() (err error) {
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

func (c *inverseMatchCursor) prev() (entryID []byte, err error) {
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

	return
}

func (c *inverseMatchCursor) prevBucket() (bktKey []byte, err error) {
	fn := c.bktCur.Prev
	if c.cur == nil {
		fn = c.bktCur.Last
	}

	for {
		bktKey, _ = fn()
		switch {
		case bytes.Equal(bktKey, c.targetRelationshipID):
		case bktKey == nil:
			err = Break
			return

		default:
			return
		}
	}
}

func (c *inverseMatchCursor) setPrevAvailableCursor() (err error) {
	for {
		if err = c.setPrevCursor(); err != nil {
			return
		}

		if !bytes.Equal(c.targetRelationshipID, c.currentRelationshipID) {
			return
		}
	}
}

func (c *inverseMatchCursor) setPrevCursor() (err error) {
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

func (c *inverseMatchCursor) teardown() {
	c.txn = nil
	c.cur = nil
}

func (c *inverseMatchCursor) firstBktKey() (bktKey []byte, err error) {
	bktKey, _ = c.bktCur.First()
	for {
		switch {
		case bktKey == nil:
			err = Break
			return
		case !bytes.Equal(c.targetRelationshipID, bktKey):
			return

		default:
			bktKey, _ = c.bktCur.Next()
		}
	}
}

func (c *inverseMatchCursor) first() (entryID []byte, err error) {
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

func (c *inverseMatchCursor) lastBktKey() (bktKey []byte, err error) {
	bktKey, _ = c.bktCur.Last()
	for {
		switch {
		case bktKey == nil:
			err = Break
			return
		case !bytes.Equal(c.targetRelationshipID, bktKey):
			return

		default:
			bktKey, _ = c.bktCur.Prev()
		}
	}
}

func (c *inverseMatchCursor) last() (entryID []byte, err error) {
	var bktKey []byte
	if bktKey, err = c.lastBktKey(); err != nil {
		return
	}

	if err = c.setCursor(bktKey); err != nil {
		return
	}

	if bytes.Equal(c.targetRelationshipID, c.currentRelationshipID) {
		if err = c.setPrevCursor(); err != nil {
			return
		}
	}

	if entryID, _ = c.cur.Last(); entryID == nil {
		err = Break
		return
	}

	return
}

func (c *inverseMatchCursor) setCursor(relationshipID []byte) (err error) {
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

// SeekForward will seek the provided ID in a forward direction
func (c *inverseMatchCursor) SeekForward(relationshipID, seekID []byte) (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if err = c.setCursor(relationshipID); err != nil {
		return
	}

	var matchFound bool
	for {
		if matchFound {
			entryID, _ = c.cur.First()
			return
		}

		entryID, _ = c.cur.Seek([]byte(seekID))
		switch {
		case entryID == nil:
		case bytes.Equal(c.targetRelationshipID, c.currentRelationshipID):
			entryID = nil
			matchFound = true

		default:
			return
		}

		if err = c.setNextCursor(); err != nil {
			return
		}
	}
}

// SeekReverse will seek the provided ID in a reverse direction
func (c *inverseMatchCursor) SeekReverse(relationshipID, seekID []byte) (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if err = c.setCursor(relationshipID); err != nil {
		return
	}

	var matchFound bool
	for {
		if matchFound {
			entryID, _ = c.cur.First()
			return
		}

		entryID, _ = c.cur.Seek([]byte(seekID))
		switch {
		case entryID == nil:
		case bytes.Equal(c.targetRelationshipID, c.currentRelationshipID):
			entryID = nil
			matchFound = true

		default:
			return
		}

		if err = c.setPrevCursor(); err != nil {
			return
		}
	}
}

// First will return the first entry
func (c *inverseMatchCursor) First() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	return c.first()
}

// Next will return the next entry
func (c *inverseMatchCursor) Next() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	return c.next()
}

// Prev will return the previous entry
func (c *inverseMatchCursor) Prev() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	return c.prev()
}

// Last will return the last entry
func (c *inverseMatchCursor) Last() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	return c.last()
}

// HasForward will determine if an entry exists in a forward direction
func (c *inverseMatchCursor) HasForward(entryID []byte) (ok bool, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	return c.has(entryID)
}

// HasReverse will determine if an entry exists in a reverse direction
func (c *inverseMatchCursor) HasReverse(entryID []byte) (ok bool, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	return c.has(entryID)
}
