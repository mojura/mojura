package mojura

func MakeReadWrapper[T Value](m *Mojura[T]) (r ReadWrapper[T]) {
	r.m = m
	return
}

type ReadWrapper[T Value] struct {
	m *Mojura[T]
}

// Exists will notiy if an entry exists for a given entry ID
func (r *ReadWrapper[T]) Exists(entryID string) (exists bool, err error) {
	return r.m.Exists(entryID)
}

// Get will attempt to get an entry by ID
func (r *ReadWrapper[T]) Get(entryID string) (val T, err error) {
	return r.m.Get(entryID)
}

// GetFiltered will attempt to get the filtered entries
func (r *ReadWrapper[T]) GetFiltered(o *FilteringOpts) (filtered []T, lastID string, err error) {
	return r.m.GetFiltered(o)
}

// AppendFiltered will attempt to append all entries associated with a set of given filters
func (r *ReadWrapper[T]) AppendFiltered(in []T, o *FilteringOpts) (filtered []T, lastID string, err error) {
	return r.m.GetFiltered(o)

}

// GetFirst will attempt to get the first entry which matches the provided filters
// Note: Will return ErrEntryNotFound if no match is found
func (r *ReadWrapper[T]) GetFirst(o *FilteringOpts) (val T, err error) {
	return r.m.GetFirst(o)
}

// GetLast will attempt to get the last entry which matches the provided filters
// Note: Will return ErrEntryNotFound if no match is found
func (r *ReadWrapper[T]) GetLast(o *FilteringOpts) (val T, err error) {
	return r.m.GetLast(o)
}

// ForEach will iterate through each of the entries
func (r *ReadWrapper[T]) ForEach(fn ForEachFn[T], o *FilteringOpts) (err error) {
	return r.m.ForEach(fn, o)
}

// ForEachID will iterate through each of the entry IDs
func (r *ReadWrapper[T]) ForEachID(fn ForEachIDFn, o *FilteringOpts) (err error) {
	return r.m.ForEachID(fn, o)
}
