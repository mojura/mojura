package mojura

import (
	"bytes"

	"github.com/mojura/backend"
	"github.com/mojura/mojura/filters"
)

var (
	_ filterCursor = &inverseMatchCursor{}
)

func newInverseMatchCursor(txn *Transaction, f *filters.InverseMatchFilter) (c filterCursor, err error) {
	var parentBkt backend.Bucket
	if parentBkt, err = txn.getRelationshipBucket([]byte(f.RelationshipKey)); err != nil {
		return
	}

	var match inverseMatchCursor
	bkt := parentBkt.GetBucket([]byte(f.RelationshipID))
	if bkt == nil {
		c = nopC
		return
	}

	match.txn = txn
	match.cur = bkt.Cursor()
	c = &match
	return
}

type inverseMatchCursor struct {
	txn *Transaction
	cur backend.Cursor
}

func (c *inverseMatchCursor) seek(id []byte) (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	entryID, _ = c.cur.Seek([]byte(id))
	if entryID == nil {
		err = Break
		return
	}

	return
}

func (c *inverseMatchCursor) has(entryID []byte) (ok bool, err error) {
	// Get the first key matching entryID (will get next key if entryID does not exist)
	firstKey, _ := c.cur.Seek(entryID)
	// If the first key matches the entry ID, we have a match
	ok = !bytes.Equal(entryID, firstKey)
	return
}

func (c *inverseMatchCursor) getCurrentRelationshipID() (relationshipID string) {
	return ""
}

func (c *inverseMatchCursor) teardown() {
	c.txn = nil
	c.cur = nil
}

// SeekForward will seek the provided ID
func (c *inverseMatchCursor) SeekForward(relationshipID, seekID []byte) (entryID []byte, err error) {
	return c.seek(seekID)
}

// SeekReverse will seek the provided ID
func (c *inverseMatchCursor) SeekReverse(relationshipID, seekID []byte) (entryID []byte, err error) {
	return c.seek(seekID)
}

// First will return the first entry
func (c *inverseMatchCursor) First() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	entryID, _ = c.cur.First()
	if entryID == nil {
		err = Break
		return
	}

	return
}

// Last will return the last entry
func (c *inverseMatchCursor) Last() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	entryID, _ = c.cur.Last()
	if entryID == nil {
		err = Break
		return
	}

	return
}

// Next will return the next entry
func (c *inverseMatchCursor) Next() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	entryID, _ = c.cur.Next()
	if entryID == nil {
		err = Break
		return
	}

	return
}

// Prev will return the previous entry
func (c *inverseMatchCursor) Prev() (entryID []byte, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	entryID, _ = c.cur.Prev()
	if entryID == nil {
		err = Break
		return
	}

	return
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
