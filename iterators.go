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

func getIteratorFunc[T any, V Value[T]](c Cursor[T, V], reverse bool) (fn iteratorFn[T, V]) {
	if !reverse {
		// Current request is a forward-direction cursor, return cursor.Next (incrementing)
		return c.Next
	}

	// Current request is a reverse-direction cursor, return cursor.Prev (decrementing)
	return c.Prev
}

type iteratorFn[T any, V Value[T]] func() (val *T, err error)
