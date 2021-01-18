package mojura

func getIDIteratorFunc(c IDCursor, reverse bool) (fn idIteratorFn) {
	if !reverse {
		// Current request is a forward-direction cursor, return cursor.Next (incrementing)
		return c.Next
	}

	// Current request is a reverse-direction cursor, return cursor.Prev (decrementing)
	return c.Prev
}

type idIteratorFn func() (entryID string, err error)

func getIteratorFunc(c Cursor, reverse bool) (fn iteratorFn) {
	if !reverse {
		// Current request is a forward-direction cursor, return cursor.Next (incrementing)
		return c.Next
	}

	// Current request is a reverse-direction cursor, return cursor.Prev (decrementing)
	return c.Prev
}

type iteratorFn func() (val Value, err error)
