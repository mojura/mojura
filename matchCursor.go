package mojura

import (
	"bytes"

	"github.com/mojura/backend"
	"github.com/mojura/mojura/filters"
)

var (
	_ filterCursor = &matchCursor[Entry, *Entry]{}
)

func newMatchCursor[T any, V Value[T]](txn *Transaction[T, V], f *filters.MatchFilter) (c filterCursor, err error) {
	var parentBkt backend.Bucket
	if parentBkt, err = txn.getRelationshipBucket([]byte(f.RelationshipKey)); err != nil {
		return
	}

	var match matchCursor[T, V]
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

type matchCursor[T any, V Value[T]] struct {
	txn *Transaction[T, V]
	cur backend.Cursor
}

func (c *matchCursor[T, V]) seek(id []byte) (entryID []byte, err error) {
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

func (c *matchCursor[T, V]) has(entryID []byte) (ok bool, err error) {
	// Get the first key matching entryID (will get next key if entryID does not exist)
	firstKey, _ := c.cur.Seek(entryID)
	// If the first key matches the entry ID, we have a match
	ok = bytes.Equal(entryID, firstKey)
	return
}

func (c *matchCursor[T, V]) getCurrentRelationshipID() (relationshipID string) {
	return ""
}

// SeekForward will seek the provided ID
func (c *matchCursor[T, V]) SeekForward(relationshipID, seekID []byte) (entryID []byte, err error) {
	return c.seek(seekID)
}

// SeekReverse will seek the provided ID
func (c *matchCursor[T, V]) SeekReverse(relationshipID, seekID []byte) (entryID []byte, err error) {
	return c.seek(seekID)
}

// First will return the first entry
func (c *matchCursor[T, V]) First() (entryID []byte, err error) {
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
func (c *matchCursor[T, V]) Last() (entryID []byte, err error) {
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
func (c *matchCursor[T, V]) Next() (entryID []byte, err error) {
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
func (c *matchCursor[T, V]) Prev() (entryID []byte, err error) {
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
func (c *matchCursor[T, V]) HasForward(entryID []byte) (ok bool, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	return c.has(entryID)
}

// HasReverse will determine if an entry exists in a reverse direction
func (c *matchCursor[T, V]) HasReverse(entryID []byte) (ok bool, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	return c.has(entryID)
}

func (c *matchCursor[T, V]) teardown() {
	c.txn = nil
}
