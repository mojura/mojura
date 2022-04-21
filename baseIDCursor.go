package mojura

import "github.com/mojura/backend"

var _ IDCursor = &baseIDCursor[Entry, *Entry]{}

func newBaseIDCursor[T any, V Value[T]](txn *Transaction[T, V]) (c IDCursor, err error) {
	var bkt backend.Bucket
	if bkt, err = txn.getEntriesBucket(); err != nil {
		return
	}

	var b baseIDCursor[T, V]
	b.txn = txn
	b.cur = bkt.Cursor()
	c = &b
	return
}

// baseIDCursor is an iterating structure
type baseIDCursor[T any, V Value[T]] struct {
	txn *Transaction[T, V]
	cur backend.Cursor
}

func (c *baseIDCursor[T, V]) getCurrentRelationshipID() (relationshipID string) {
	return string("")
}

func (c *baseIDCursor[T, V]) seek(seekID []byte) (entryID []byte, err error) {
	entryID, _ = c.cur.Seek(seekID)
	if entryID == nil {
		err = Break
		return
	}

	return
}

func (c *baseIDCursor[T, V]) seekReverse(seekID []byte) (entryID []byte, err error) {
	return c.seek(seekID)
}

// First will return the first entry
func (c *baseIDCursor[T, V]) first() (entryID []byte, err error) {
	entryID, _ = c.cur.First()
	if entryID == nil {
		err = Break
		return
	}

	return
}

func (c *baseIDCursor[T, V]) last() (entryID []byte, err error) {
	entryID, _ = c.cur.Last()
	if entryID == nil {
		err = Break
		return
	}

	return
}

func (c *baseIDCursor[T, V]) next() (entryID []byte, err error) {
	entryID, _ = c.cur.Next()
	if entryID == nil {
		err = Break
		return
	}

	return
}

func (c *baseIDCursor[T, V]) prev() (entryID []byte, err error) {
	entryID, _ = c.cur.Prev()
	if entryID == nil {
		err = Break
		return
	}

	return
}

// Seek will seek the provided ID
func (c *baseIDCursor[T, V]) Seek(seekID string) (entryID string, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	var eID []byte
	if eID, err = c.seek([]byte(seekID)); err != nil {
		return
	}

	entryID = string(eID)
	return
}

// SeekReverse will seek the provided ID
func (c *baseIDCursor[T, V]) SeekReverse(seekID string) (entryID string, err error) {
	return c.Seek(seekID)
}

// First will return the first entry
func (c *baseIDCursor[T, V]) First() (entryID string, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	var eID []byte
	if eID, err = c.first(); err != nil {
		return
	}

	entryID = string(eID)
	return
}

// Last will return the last entry
func (c *baseIDCursor[T, V]) Last() (entryID string, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	var eID []byte
	if eID, err = c.last(); err != nil {
		return
	}

	entryID = string(eID)
	return
}

// Next will return the next entry
func (c *baseIDCursor[T, V]) Next() (entryID string, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	var eID []byte
	if eID, err = c.next(); err != nil {
		return
	}

	entryID = string(eID)
	return
}

// Prev will return the previous entry
func (c *baseIDCursor[T, V]) Prev() (entryID string, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	var eID []byte
	if eID, err = c.prev(); err != nil {
		return
	}

	entryID = string(eID)
	return
}

func (c *baseIDCursor[T, V]) teardown() {
	c.txn = nil
	c.cur = nil
}
