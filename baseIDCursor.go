package mojura

import "github.com/mojura/backend"

var _ IDCursor = &baseIDCursor{}

func newBaseIDCursor(txn *Transaction) (c IDCursor, err error) {
	var bkt backend.Bucket
	if bkt, err = txn.getEntriesBucket(); err != nil {
		return
	}

	var b baseIDCursor
	b.txn = txn
	b.cur = bkt.Cursor()
	c = &b
	return
}

// baseIDCursor is an iterating structure
type baseIDCursor struct {
	txn *Transaction
	cur backend.Cursor

	relationship bool
}

func (c *baseIDCursor) getCurrentRelationshipID() (relationshipID string) {
	return string("")
}

func (c *baseIDCursor) seek(seekID []byte) (entryID []byte, err error) {
	entryID, _ = c.cur.Seek(seekID)
	if entryID == nil {
		err = Break
		return
	}

	return
}

func (c *baseIDCursor) seekReverse(seekID []byte) (entryID []byte, err error) {
	return c.seek(seekID)
}

// First will return the first entry
func (c *baseIDCursor) first() (entryID []byte, err error) {
	entryID, _ = c.cur.First()
	if entryID == nil {
		err = Break
		return
	}

	return
}

func (c *baseIDCursor) last() (entryID []byte, err error) {
	entryID, _ = c.cur.Last()
	if entryID == nil {
		err = Break
		return
	}

	return
}

func (c *baseIDCursor) next() (entryID []byte, err error) {
	entryID, _ = c.cur.Next()
	if entryID == nil {
		err = Break
		return
	}

	return
}

func (c *baseIDCursor) prev() (entryID []byte, err error) {
	entryID, _ = c.cur.Prev()
	if entryID == nil {
		err = Break
		return
	}

	return
}

// Seek will seek the provided ID
func (c *baseIDCursor) Seek(seekID string) (entryID string, err error) {
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
func (c *baseIDCursor) SeekReverse(seekID string) (entryID string, err error) {
	return c.Seek(seekID)
}

// First will return the first entry
func (c *baseIDCursor) First() (entryID string, err error) {
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
func (c *baseIDCursor) Last() (entryID string, err error) {
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
func (c *baseIDCursor) Next() (entryID string, err error) {
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
func (c *baseIDCursor) Prev() (entryID string, err error) {
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

func (c *baseIDCursor) teardown() {
	c.txn = nil
	c.cur = nil
}
