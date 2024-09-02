package mojura

import "github.com/mojura/backend"

var _ Cursor[*Entry] = &baseCursor[*Entry]{}

func newBaseCursor[T Value](txn *Transaction[T]) (c Cursor[T], err error) {
	var bkt backend.Bucket
	if bkt, err = txn.getEntriesBucket(); err != nil {
		return
	}

	var b baseCursor[T]
	b.txn = txn
	b.cur = bkt.Cursor()
	c = &b
	return
}

// baseCursor is an iterating structure
type baseCursor[T Value] struct {
	txn *Transaction[T]
	cur backend.Cursor
}

func (c *baseCursor[T]) getCurrentRelationshipID() (relationshipID string) {
	return string("")
}

// Seek will seek the provided ID
func (c *baseCursor[T]) Seek(seekID string) (val T, err error) {
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
func (c *baseCursor[T]) SeekReverse(seekID string) (val T, err error) {
	return c.Seek(seekID)
}

// First will return the first entry
func (c *baseCursor[T]) First() (val T, err error) {
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
func (c *baseCursor[T]) Last() (val T, err error) {
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
func (c *baseCursor[T]) Next() (val T, err error) {
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
func (c *baseCursor[T]) Prev() (val T, err error) {
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

func (c *baseCursor[T]) get(_, bs []byte) (val T, err error) {
	return c.txn.m.newValueFromBytes(bs)
}

func (c *baseCursor[T]) teardown() {
	c.txn = nil
	c.cur = nil
}
