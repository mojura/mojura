package mojura

type WriteWrapper[T Value] struct {
	m Mojura[T]
}

// New will insert a new entry with the given value and the associated relationships
func (w *WriteWrapper[T]) New(val T) (entryID string, err error) {
	return w.m.New(val)
}

// Put will place an entry at a given entry ID
// Note: This will not check to see if the entry exists beforehand. If this functionality
// is needed, look into using the Edit method
func (w *WriteWrapper[T]) Put(entryID string, val T) (err error) {
	return w.m.Put(entryID, val)
}

// Edit will attempt to edit an entry by ID
func (w *WriteWrapper[T]) Edit(entryID string, val T) (err error) {
	return w.m.Edit(entryID, val)
}

// Remove will remove a relationship ID and it's related relationship IDs
func (w *WriteWrapper[T]) Remove(entryID string) (err error) {
	return w.m.Remove(entryID)
}
