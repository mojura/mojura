package mojura

var _ IDCursor = &multiIDCursor[*Entry]{}

func newMultiIDCursor[T Value](txn *Transaction[T], fs []Filter) (mp *multiIDCursor[T], err error) {
	var m multiIDCursor[T]
	if len(fs) == 0 {
		err = ErrEmptyFilters
		return
	}

	fcs := make([]filterCursor, 0, len(fs))
	for _, f := range fs {
		var fc filterCursor
		if fc, err = newFilterCursor(txn, f); err != nil {
			return
		}

		fcs = append(fcs, fc)
	}

	m.txn = txn
	m.primary = fcs[0]
	if len(fs) > 1 {
		m.secondary = fcs[1:]
	}

	mp = &m
	return
}

type multiIDCursor[T Value] struct {
	txn *Transaction[T]

	primary   filterCursor
	secondary []filterCursor
}

func (c *multiIDCursor[T]) teardown() {
	c.txn = nil
	c.primary = nil
	c.secondary = nil
}

func (c *multiIDCursor[T]) getCurrentRelationshipID() (relationshipID string) {
	return c.primary.getCurrentRelationshipID()
}

func (c *multiIDCursor[T]) isForwardMatch(entryID []byte) (isMatch bool, err error) {
	for _, secondary := range c.secondary {
		if isMatch, err = secondary.HasForward(entryID); err != nil {
			isMatch = false
			return
		}

		if !isMatch {
			return
		}
	}

	return true, nil
}

func (c *multiIDCursor[T]) isReverseMatch(entryID []byte) (isMatch bool, err error) {
	for _, secondary := range c.secondary {
		if isMatch, err = secondary.HasReverse(entryID); err != nil {
			isMatch = false
			return
		}

		if !isMatch {
			return
		}
	}

	return true, nil
}

func (c *multiIDCursor[T]) nextUntilMatch(entryID []byte) (matchEntryID []byte, err error) {
	var isMatch bool
	for err == nil {
		isMatch, err = c.isForwardMatch(entryID)
		switch {
		case err != nil:
			return
		case isMatch:
			matchEntryID = entryID
			return

		default:
			entryID, err = c.primary.Next()
		}
	}

	return
}

func (c *multiIDCursor[T]) prevUntilMatch(entryID []byte) (matchEntryID []byte, err error) {
	var isMatch bool
	for err == nil {
		isMatch, err = c.isReverseMatch(entryID)
		switch {
		case err != nil:
			return
		case isMatch:
			matchEntryID = entryID
			return

		default:
			entryID, err = c.primary.Prev()
		}
	}

	return
}

func (c *multiIDCursor[T]) seek(seekID []byte) (entryID []byte, err error) {
	var relationshipKey []byte
	relationshipKey, seekID = splitSeekID(seekID)
	if entryID, err = c.primary.SeekForward(relationshipKey, seekID); err != nil {
		return
	}

	return c.nextUntilMatch(entryID)
}

func (c *multiIDCursor[T]) seekReverse(seekID []byte) (entryID []byte, err error) {
	var relationshipKey []byte
	relationshipKey, seekID = splitSeekID(seekID)
	if entryID, err = c.primary.SeekReverse(relationshipKey, seekID); err != nil {
		return
	}

	return c.prevUntilMatch(entryID)
}

func (c *multiIDCursor[T]) first() (entryID []byte, err error) {
	if entryID, err = c.primary.First(); err != nil {
		return
	}

	return c.nextUntilMatch(entryID)
}

func (c *multiIDCursor[T]) next() (entryID []byte, err error) {
	if entryID, err = c.primary.Next(); err != nil {
		return
	}

	return c.nextUntilMatch(entryID)
}

func (c *multiIDCursor[T]) prev() (entryID []byte, err error) {
	if entryID, err = c.primary.Prev(); err != nil {
		return
	}

	return c.prevUntilMatch(entryID)
}

func (c *multiIDCursor[T]) last() (entryID []byte, err error) {
	if entryID, err = c.primary.Last(); err != nil {
		return
	}

	return c.prevUntilMatch(entryID)
}

// Seek will seek the provided ID and move forward until match
func (c *multiIDCursor[T]) Seek(seekID string) (entryID string, err error) {
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

// SeekReverse will seek the provided ID and move reverse until match
func (c *multiIDCursor[T]) SeekReverse(seekID string) (entryID string, err error) {
	if err = c.txn.cc.isDone(); err != nil {
		return
	}

	var eID []byte
	if eID, err = c.seekReverse([]byte(seekID)); err != nil {
		return
	}

	entryID = string(eID)
	return
}

// First will return the first entry
func (c *multiIDCursor[T]) First() (entryID string, err error) {
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
func (c *multiIDCursor[T]) Last() (entryID string, err error) {
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
func (c *multiIDCursor[T]) Next() (entryID string, err error) {
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
func (c *multiIDCursor[T]) Prev() (entryID string, err error) {
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

// HasForward will determine if an entry exists in a forward direction
func (c *multiIDCursor[T]) HasForward(entryID string) (ok bool, err error) {
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
func (c *multiIDCursor[T]) HasReverse(entryID string) (ok bool, err error) {
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
