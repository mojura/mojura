package mojura

// entryFilterCursor keeps the existing ID-based cursor interface on sorted
// stores. Ordered intersections use the raw keys directly and avoid these
// compatibility lookups. Legacy ID-only seek behavior is otherwise unchanged.
type entryFilterCursor[T Value] struct {
	filterCursor
	txn *Transaction[T]
}

func (c *entryFilterCursor[T]) entry(key []byte, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}

	if len(key) <= c.txn.m.sortKeyWidth {
		return nil, ErrInvalidSortKey
	}

	return key[c.txn.m.sortKeyWidth:], nil
}

func (c *entryFilterCursor[T]) key(id []byte) ([]byte, error) {
	if len(id) == 0 {
		return nil, nil
	}

	value, err := c.txn.get(id)
	if err != nil {
		return nil, err
	}

	return c.txn.membershipKey(id, value)
}

func (c *entryFilterCursor[T]) First() ([]byte, error) { return c.entry(c.filterCursor.First()) }
func (c *entryFilterCursor[T]) Last() ([]byte, error)  { return c.entry(c.filterCursor.Last()) }
func (c *entryFilterCursor[T]) Next() ([]byte, error)  { return c.entry(c.filterCursor.Next()) }
func (c *entryFilterCursor[T]) Prev() ([]byte, error)  { return c.entry(c.filterCursor.Prev()) }

func (c *entryFilterCursor[T]) SeekForward(relationship, id []byte) ([]byte, error) {
	key, err := c.key(id)
	if err != nil {
		return nil, err
	}

	return c.entry(c.filterCursor.SeekForward(relationship, key))
}

func (c *entryFilterCursor[T]) SeekReverse(relationship, id []byte) ([]byte, error) {
	key, err := c.key(id)
	if err != nil {
		return nil, err
	}

	return c.entry(c.filterCursor.SeekReverse(relationship, key))
}

func (c *entryFilterCursor[T]) HasForward(id []byte) (bool, error) {
	key, err := c.key(id)
	if err == ErrEntryNotFound {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return c.filterCursor.HasForward(key)
}

func (c *entryFilterCursor[T]) HasReverse(id []byte) (bool, error) {
	key, err := c.key(id)
	if err == ErrEntryNotFound {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return c.filterCursor.HasReverse(key)
}
