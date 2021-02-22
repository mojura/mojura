package mojura

var _ Cursor = &multiCursor{}

func newMultiCursor(txn *Transaction, fs []Filter) (c Cursor, err error) {
	var m multiCursor
	if m.mid, err = newMultiIDCursor(txn, fs); err != nil {
		return
	}

	m.txn = txn
	c = &m
	return
}

type multiCursor struct {
	txn *Transaction
	mid *multiIDCursor
}

// Seek is an alias for SeekForward
func (c *multiCursor) Seek(seekID string) (val Value, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	var entryID []byte
	if entryID, err = c.mid.seek([]byte(seekID)); err != nil {
		return
	}

	return c.get(entryID)
}

// SeekReverse will seek the provided ID and move reverse until match
func (c *multiCursor) SeekReverse(seekID string) (val Value, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	var entryID []byte
	if entryID, err = c.mid.seekReverse([]byte(seekID)); err != nil {
		return
	}

	return c.get(entryID)
}

// First will return the first entry
func (c *multiCursor) First() (val Value, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	var entryID []byte
	if entryID, err = c.mid.first(); err != nil {
		return
	}

	return c.get(entryID)
}

// Last will return the last entry
func (c *multiCursor) Last() (val Value, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	var entryID []byte
	if entryID, err = c.mid.last(); err != nil {
		return
	}

	return c.get(entryID)
}

// Next will return the next entry
func (c *multiCursor) Next() (val Value, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	var entryID []byte
	if entryID, err = c.mid.next(); err != nil {
		return
	}

	return c.get(entryID)
}

// Prev will return the previous entry
func (c *multiCursor) Prev() (val Value, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	var entryID []byte
	if entryID, err = c.mid.prev(); err != nil {
		return
	}

	return c.get(entryID)
}

// HasForward will determine if an entry exists in a forward direction
func (c *multiCursor) HasForward(entryID string) (ok bool, err error) {
	return c.mid.HasForward(entryID)
}

// HasReverse will determine if an entry exists in a reverse direction
func (c *multiCursor) HasReverse(entryID string) (ok bool, err error) {
	return c.mid.HasReverse(entryID)
}

func (c *multiCursor) get(entryID []byte) (val Value, err error) {
	var bs []byte
	// Attempt to get and associate bytes to value
	if bs, err = c.txn.getBytes(entryID); err != nil {
		return
	}

	// Set value from bytes
	return c.txn.m.newValueFromBytes(bs)
}

func (c *multiCursor) getCurrentRelationshipID() (relationshipID string) {
	return c.mid.getCurrentRelationshipID()
}

func (c *multiCursor) teardown() {
	c.txn = nil
	c.mid = nil
}
