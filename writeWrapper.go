package mojura

func MakeWriteWrapper[T Value](m *Mojura[T]) (w WriteWrapper[T]) {
	w.m = m
	return
}

type WriteWrapper[T Value] struct {
	m *Mojura[T]
}

// New will insert a new entry with the given value and the associated relationships
func (w *WriteWrapper[T]) New(val T) (created T, err error) {
	return w.m.New(val)
}

// Put will place an entry at a given entry ID
// Note: This will not check to see if the entry exists beforehand. If this functionality
// is needed, look into using the Edit method
func (w *WriteWrapper[T]) Put(entryID string, val T) (updated T, err error) {
	return w.m.Put(entryID, val)
}

// Edit will attempt to edit an entry by ID
func (w *WriteWrapper[T]) Update(entryID string, fn UpdateFn[T]) (updated T, err error) {
	return w.m.Update(entryID, fn)
}

// Delete will remove an entry and it's related relationship IDs
func (w *WriteWrapper[T]) Delete(entryID string) (deleted T, err error) {
	return w.m.Delete(entryID)
}
