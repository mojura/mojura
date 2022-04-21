package mojura

func MakeWriteWrapper[T any, V Value[T]](m *Mojura[T, V]) (w WriteWrapper[T, V]) {
	w.m = m
	return
}

type WriteWrapper[T any, V Value[T]] struct {
	m *Mojura[T, V]
}

// New will insert a new entry with the given value and the associated relationships
func (w *WriteWrapper[T, V]) New(val T) (created *T, err error) {
	return w.m.New(val)
}

// Put will place an entry at a given entry ID
// Note: This will not check to see if the entry exists beforehand. If this functionality
// is needed, look into using the Edit method
func (w *WriteWrapper[T, V]) Put(entryID string, val T) (updated *T, err error) {
	return w.m.Put(entryID, val)
}

// Edit will attempt to edit an entry by ID
func (w *WriteWrapper[T, V]) Update(entryID string, fn UpdateFn[T, V]) (updated *T, err error) {
	return w.m.Update(entryID, fn)
}

// Delete will remove an entry and it's related relationship IDs
func (w *WriteWrapper[T, V]) Delete(entryID string) (deleted *T, err error) {
	return w.m.Delete(entryID)
}
