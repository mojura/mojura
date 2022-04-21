package mojura

import "github.com/mojura/backend"

var _ Cursor[Entry, *Entry] = &baseCursor[Entry, *Entry]{}

func newBaseCursor[T any, V Value[T]](txn *Transaction[T, V]) (c Cursor[T, V], err error) {
	var bkt backend.Bucket
	if bkt, err = txn.getEntriesBucket(); err != nil {
		return
	}

	var b baseCursor[T, V]
	b.txn = txn
	b.cur = bkt.Cursor()
	c = &b
	return
}

// baseCursor is an iterating structure
type baseCursor[T any, V Value[T]] struct {
	txn *Transaction[T, V]
	cur backend.Cursor
}

func (c *baseCursor[T, V]) getCurrentRelationshipID() (relationshipID string) {
	return string("")
}

// Seek will seek the provided ID
func (c *baseCursor[T, V]) Seek(seekID string) (val *T, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	entryID, valueBytes := c.cur.Seek([]byte(seekID))
	if entryID == nil && valueBytes == nil {
		err = Break
		return
	}

	return c.get(entryID, valueBytes)
}

// Seek will seek the provided ID
func (c *baseCursor[T, V]) SeekReverse(seekID string) (val *T, err error) {
	return c.Seek(seekID)
}

// First will return the first entry
func (c *baseCursor[T, V]) First() (val *T, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	k, v := c.cur.First()
	if k == nil && v == nil {
		err = Break
		return
	}

	return c.get(k, v)
}

// Last will return the last entry
func (c *baseCursor[T, V]) Last() (val *T, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	k, v := c.cur.Last()
	if k == nil && v == nil {
		err = Break
		return
	}

	return c.get(k, v)
}

// Next will return the next entry
func (c *baseCursor[T, V]) Next() (val *T, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	k, v := c.cur.Next()
	if k == nil && v == nil {
		err = Break
		return
	}

	return c.get(k, v)
}

// Prev will return the previous entry
func (c *baseCursor[T, V]) Prev() (val *T, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	k, v := c.cur.Prev()
	if k == nil && v == nil {
		err = Break
		return
	}

	return c.get(k, v)
}

func (c *baseCursor[T, V]) get(key, bs []byte) (val *T, err error) {
	return c.txn.m.newValueFromBytes(bs)
}

func (c *baseCursor[T, V]) teardown() {
	c.txn = nil
	c.cur = nil
}
