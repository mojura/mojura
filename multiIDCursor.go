package mojura

var _ IDCursor = &multiIDCursor[Entry, *Entry]{}

func newMultiIDCursor[T any, V Value[T]](txn *Transaction[T, V], fs []Filter) (mp *multiIDCursor[T, V], err error) {
	var m multiIDCursor[T, V]
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

type multiIDCursor[T any, V Value[T]] struct {
	txn *Transaction[T, V]

	primary   filterCursor
	secondary []filterCursor
}

func (c *multiIDCursor[T, V]) teardown() {
	c.txn = nil
	c.primary = nil
	c.secondary = nil
}

func (c *multiIDCursor[T, V]) getCurrentRelationshipID() (relationshipID string) {
	return c.primary.getCurrentRelationshipID()
}

func (c *multiIDCursor[T, V]) isForwardMatch(entryID []byte) (isMatch bool, err error) {
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

func (c *multiIDCursor[T, V]) isReverseMatch(entryID []byte) (isMatch bool, err error) {
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

func (c *multiIDCursor[T, V]) nextUntilMatch(entryID []byte) (matchEntryID []byte, err error) {
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

func (c *multiIDCursor[T, V]) prevUntilMatch(entryID []byte) (matchEntryID []byte, err error) {
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

func (c *multiIDCursor[T, V]) seek(seekID []byte) (entryID []byte, err error) {
	var relationshipKey []byte
	if relationshipKey, seekID = splitSeekID(seekID); err != nil {
		return
	}

	if entryID, err = c.primary.SeekForward(relationshipKey, seekID); err != nil {
		return
	}

	return c.nextUntilMatch(entryID)
}

func (c *multiIDCursor[T, V]) seekReverse(seekID []byte) (entryID []byte, err error) {
	var relationshipKey []byte
	if relationshipKey, seekID = splitSeekID(seekID); err != nil {
		return
	}

	if entryID, err = c.primary.SeekReverse(relationshipKey, seekID); err != nil {
		return
	}

	return c.prevUntilMatch(entryID)
}

func (c *multiIDCursor[T, V]) first() (entryID []byte, err error) {
	if entryID, err = c.primary.First(); err != nil {
		return
	}

	return c.nextUntilMatch(entryID)
}

func (c *multiIDCursor[T, V]) next() (entryID []byte, err error) {
	if entryID, err = c.primary.Next(); err != nil {
		return
	}

	return c.nextUntilMatch(entryID)
}

func (c *multiIDCursor[T, V]) prev() (entryID []byte, err error) {
	if entryID, err = c.primary.Prev(); err != nil {
		return
	}

	return c.prevUntilMatch(entryID)
}

func (c *multiIDCursor[T, V]) last() (entryID []byte, err error) {
	if entryID, err = c.primary.Last(); err != nil {
		return
	}

	return c.prevUntilMatch(entryID)
}

// Seek will seek the provided ID and move forward until match
func (c *multiIDCursor[T, V]) Seek(seekID string) (entryID string, err error) {
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
func (c *multiIDCursor[T, V]) SeekReverse(seekID string) (entryID string, err error) {
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
func (c *multiIDCursor[T, V]) First() (entryID string, err error) {
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
func (c *multiIDCursor[T, V]) Last() (entryID string, err error) {
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
func (c *multiIDCursor[T, V]) Next() (entryID string, err error) {
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
func (c *multiIDCursor[T, V]) Prev() (entryID string, err error) {
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
func (c *multiIDCursor[T, V]) HasForward(entryID string) (ok bool, err error) {
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
func (c *multiIDCursor[T, V]) HasReverse(entryID string) (ok bool, err error) {
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
