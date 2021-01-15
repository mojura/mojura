package mojura

import (
	"bytes"

	"github.com/mojura/backend"
)

var (
	_ filterCursor = &matchCursor{}
)

func newMatchCursor(txn *Transaction, relationshipKey, relationshipID []byte) (m *matchCursor, err error) {
	var parentBkt backend.Bucket
	if parentBkt, err = txn.getRelationshipBucket(relationshipKey); err != nil {
		return
	}

	var match matchCursor
	bkt := parentBkt.GetBucket(relationshipID)
	match.txn = txn
	match.cur = bkt.Cursor()
	m = &match
	return
}

type matchCursor struct {
	txn *Transaction
	cur backend.Cursor
}

func (c *matchCursor) seek(id []byte) (entryID []byte, err error) {
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

func (c *matchCursor) has(entryID []byte) (ok bool, err error) {
	// Get the first key matching entryID (will get next key if entryID does not exist)
	firstKey, _ := c.cur.Seek(entryID)
	// If the first key matches the entry ID, we have a match
	ok = bytes.Compare(entryID, firstKey) == 0
	return
}

func (c *matchCursor) getCurrentRelationshipID() (relationshipID string) {
	return ""
}

func (c *matchCursor) teardown() {
	c.txn = nil
	c.cur = nil
}

// SeekForward will seek the provided ID
func (c *matchCursor) SeekForward(relationshipID, seekID []byte) (entryID []byte, err error) {
	return c.seek(seekID)
}

// SeekReverse will seek the provided ID
func (c *matchCursor) SeekReverse(relationshipID, seekID []byte) (entryID []byte, err error) {
	return c.seek(seekID)
}

// First will return the first entry
func (c *matchCursor) First() (entryID []byte, err error) {
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
func (c *matchCursor) Last() (entryID []byte, err error) {
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
func (c *matchCursor) Next() (entryID []byte, err error) {
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
func (c *matchCursor) Prev() (entryID []byte, err error) {
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
func (c *matchCursor) HasForward(entryID []byte) (ok bool, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	return c.has(entryID)
}

// HasReverse will determine if an entry exists in a reverse direction
func (c *matchCursor) HasReverse(entryID []byte) (ok bool, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	return c.has(entryID)
}
