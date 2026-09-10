package mojura

// MakeWriteWrapper exposes write convenience methods on m without owning its lifetime.
func MakeWriteWrapper[T Value](m *Mojura[T]) (w WriteWrapper[T]) {
	w.m = m
	return
}

// WriteWrapper delegates CRUD writes to its underlying database.
type WriteWrapper[T Value] struct {
	m *Mojura[T]
}

// New will insert a new entry with the given value and the associated relationships
func (w *WriteWrapper[T]) New(val T) (created T, err error) {
	return w.m.New(val)
}

// Put inserts or replaces val at entryID and updates its relationship indexes.
// It mutates val's ID/timestamps and does not advance the generated-ID counter.
// Use Update when the entry must already exist.
func (w *WriteWrapper[T]) Put(entryID string, val T) (updated T, err error) {
	return w.m.Put(entryID, val)
}

// Update modifies an existing entry and updates its relationship indexes.
func (w *WriteWrapper[T]) Update(entryID string, fn UpdateFn[T]) (updated T, err error) {
	return w.m.Update(entryID, fn)
}

// Delete removes an existing entry and its relationship memberships.
func (w *WriteWrapper[T]) Delete(entryID string) (deleted T, err error) {
	return w.m.Delete(entryID)
}
