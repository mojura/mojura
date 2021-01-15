package mojura

import "github.com/mojura/backend"

var _ Cursor = &baseCursor{}

func newBaseCursor(txn *Transaction) (c Cursor, err error) {
	var bkt backend.Bucket
	if bkt, err = txn.getEntriesBucket(); err != nil {
		return
	}

	var b baseCursor
	b.txn = txn
	b.cur = bkt.Cursor()
	c = &b
	return
}

// baseCursor is an iterating structure
type baseCursor struct {
	txn *Transaction
	cur backend.Cursor
}

// Seek will seek the provided ID
func (c *baseCursor) Seek(seekID string) (val Value, err error) {
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
func (c *baseCursor) SeekReverse(seekID string) (val Value, err error) {
	return c.Seek(seekID)
}

// First will return the first entry
func (c *baseCursor) First() (val Value, err error) {
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
func (c *baseCursor) Last() (val Value, err error) {
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
func (c *baseCursor) Next() (val Value, err error) {
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
func (c *baseCursor) Prev() (val Value, err error) {
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

func (c *baseCursor) get(key, bs []byte) (val Value, err error) {
	return c.txn.m.newValueFromBytes(bs)
}

func (c *baseCursor) teardown() {
	c.txn = nil
	c.cur = nil
}
