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

func getIteratorFunc[T Value](c Cursor[T], reverse bool) (fn iteratorFn[T]) {
	if !reverse {
		// Current request is a forward-direction cursor, return cursor.Next (incrementing)
		return c.Next
	}

	// Current request is a reverse-direction cursor, return cursor.Prev (decrementing)
	return c.Prev
}

type iteratorFn[T Value] func() (val T, err error)
