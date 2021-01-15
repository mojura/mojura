package mojura

import "strings"

var _ Cursor = &multiCursor{}

func newMultiCursor(txn *Transaction, fs ...filterCursor) (c Cursor, err error) {
	var m multiCursor
	if len(fs) == 0 {
		err = ErrEmptyFilters
		return
	}

	m.txn = txn
	m.primary = fs[0]
	if len(fs) > 1 {
		m.secondary = fs[1:]
	}

	c = &m
	return
}

type multiCursor struct {
	txn *Transaction

	primary   filterCursor
	secondary []filterCursor
}

func (c *multiCursor) teardown() {
	c.txn = nil
	c.primary = nil
	c.secondary = nil
}

func (c *multiCursor) isForwardMatch(entryID []byte, reverse bool) (isMatch bool, err error) {
	for _, secondary := range c.secondary {
		if isMatch, err = secondary.HasForward(entryID); err != nil {
			isMatch = false
			return
		}

		if !isMatch {
			return
		}
	}

	return
}

func (c *multiCursor) isReverseMatch(entryID []byte, reverse bool) (isMatch bool, err error) {
	for _, secondary := range c.secondary {
		if isMatch, err = secondary.HasReverse(entryID); err != nil {
			isMatch = false
			return
		}

		if !isMatch {
			return
		}
	}

	return
}

func (c *multiCursor) nextUntilMatch(entryID []byte, val Value) (err error) {
	var isMatch bool
	for err == nil {
		isMatch, err = c.isForwardMatch(entryID, false)
		switch {
		case err != nil:
			return
		case isMatch:
			return c.txn.get(entryID, val)

		default:
			entryID, err = c.primary.Next()
		}
	}

	return
}

func (c *multiCursor) prevUntilMatch(entryID []byte, val Value) (err error) {
	var isMatch bool
	for err == nil {
		isMatch, err = c.isReverseMatch(entryID, true)
		switch {
		case err != nil:
			return
		case isMatch:
			return c.txn.get(entryID, val)

		default:
			entryID, err = c.primary.Prev()
		}
	}

	return
}

// Seek is an alias for SeekForward
func (c *multiCursor) Seek(seekID string, val Value) (err error) {
	var relationshipKey string
	if len(seekID) > 0 {
		spl := strings.SplitN(seekID, "::", 2)
		relationshipKey = spl[0]
		seekID = spl[1]
	}

	return c.SeekForward(relationshipKey, seekID, val)
}

// SeekForward will seek the provided ID and move forward until match
func (c *multiCursor) SeekForward(relationshipKey, seekID string, val Value) (err error) {
	var entryID []byte
	if entryID, err = c.primary.SeekForward([]byte(relationshipKey), []byte(seekID)); err != nil {
		return
	}

	return c.nextUntilMatch(entryID, val)
}

// SeekReverse will seek the provided ID and move reverse until match
func (c *multiCursor) SeekReverse(relationshipKey, seekID string, val Value) (err error) {
	var entryID []byte
	if entryID, err = c.primary.SeekReverse([]byte(relationshipKey), []byte(seekID)); err != nil {
		return
	}

	return c.prevUntilMatch(entryID, val)
}

// First will return the first entry
func (c *multiCursor) First(val Value) (err error) {
	var entryID []byte
	if entryID, err = c.primary.First(); err != nil {
		return
	}

	return c.nextUntilMatch(entryID, val)
}

// Last will return the last entry
func (c *multiCursor) Last(val Value) (err error) {
	var entryID []byte
	if entryID, err = c.primary.Last(); err != nil {
		return
	}

	return c.prevUntilMatch(entryID, val)
}

// Next will return the next entry
func (c *multiCursor) Next(val Value) (err error) {
	var entryID []byte
	if entryID, err = c.primary.Next(); err != nil {
		return
	}

	return c.nextUntilMatch(entryID, val)
}

// Prev will return the previous entry
func (c *multiCursor) Prev(val Value) (err error) {
	var entryID []byte
	if entryID, err = c.primary.Prev(); err != nil {
		return
	}

	return c.prevUntilMatch(entryID, val)
}

// HasForward will determine if an entry exists in a forward direction
func (c *multiCursor) HasForward(entryID string) (ok bool, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if ok, err = c.primary.HasForward([]byte(entryID)); !ok || err != nil {
		return
	}

	for _, secondary := range c.secondary {
		if ok, err = secondary.HasForward([]byte(entryID)); !ok || err != nil {
			return
		}
	}

	return
}

// HasReverse will determine if an entry exists in a reverse direction
func (c *multiCursor) HasReverse(entryID string) (ok bool, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	if ok, err = c.primary.HasReverse([]byte(entryID)); !ok || err != nil {
		return
	}

	for _, secondary := range c.secondary {
		if ok, err = secondary.HasReverse([]byte(entryID)); !ok || err != nil {
			return
		}
	}

	return
}
