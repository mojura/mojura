package mojura

import "github.com/mojura/backend"

var _ Cursor = &cursor{}

func newCursor(txn *Transaction, cur backend.Cursor, relationship bool) (c cursor) {
	c.txn = txn
	c.cur = cur
	c.relationship = relationship
	return
}

// cursor is an iterating structure
type cursor struct {
	txn *Transaction
	cur backend.Cursor

	relationship bool
}

func (c *cursor) get(key, bs []byte, val Value) (err error) {
	if !c.relationship {
		return c.txn.m.unmarshal(bs, val)
	}

	if err = c.txn.get(key, val); err != nil {
		val = nil
		return
	}

	return
}

func (c *cursor) teardown() {
	c.txn = nil
	c.cur = nil
}

// Seek will seek the provided ID
func (c *cursor) Seek(id string, val Value) (err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	k, v := c.cur.Seek([]byte(id))
	if k == nil && v == nil {
		err = Break
		return
	}

	if err = c.get(k, v, val); err != nil {
		return
	}

	return
}

// First will return the first entry
func (c *cursor) First(val Value) (err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	k, v := c.cur.First()
	if k == nil && v == nil {
		err = Break
		return
	}

	if err = c.get(k, v, val); err != nil {
		return
	}

	return
}

// Last will return the last entry
func (c *cursor) Last(val Value) (err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	k, v := c.cur.Last()
	if k == nil && v == nil {
		err = Break
		return
	}

	if err = c.get(k, v, val); err != nil {
		return
	}

	return
}

// Next will return the next entry
func (c *cursor) Next(val Value) (err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	k, v := c.cur.Next()
	if k == nil && v == nil {
		err = Break
		return
	}

	if err = c.get(k, v, val); err != nil {
		return
	}

	return
}

// Prev will return the previous entry
func (c *cursor) Prev(val Value) (err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	k, v := c.cur.Prev()
	if k == nil && v == nil {
		err = Break
		return
	}

	if err = c.get(k, v, val); err != nil {
		return
	}

	return
}

// Cursor is used to iterate through entries
type Cursor interface {
	Seek(seekID string, value Value) (err error)
	First(value Value) (err error)
	Last(value Value) (err error)
	Next(value Value) (err error)
	Prev(value Value) (err error)
}
