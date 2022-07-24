package mojura

func MakeReadWrapper[T any, V Value[T]](m *Mojura[T, V]) (r ReadWrapper[T, V]) {
	r.m = m
	return
}

type ReadWrapper[T any, V Value[T]] struct {
	m *Mojura[T, V]
}

// Exists will notiy if an entry exists for a given entry ID
func (r *ReadWrapper[T, V]) Exists(entryID string) (exists bool, err error) {
	return r.m.Exists(entryID)
}

// Get will attempt to get an entry by ID
func (r *ReadWrapper[T, V]) Get(entryID string) (val *T, err error) {
	return r.m.Get(entryID)
}

// GetFiltered will attempt to get the filtered entries
func (r *ReadWrapper[T, V]) GetFiltered(o *FilteringOpts) (filtered []*T, lastID string, err error) {
	return r.m.GetFiltered(o)
}

// GetFilteredIDs will attempt to get the filtered entry IDs
func (r *ReadWrapper[T, V]) GetFilteredIDs(o *FilteringOpts) (filtered []string, lastID string, err error) {
	return r.m.GetFilteredIDs(o)
}

// AppendFiltered will attempt to append all entries associated with a set of given filters
func (r *ReadWrapper[T, V]) AppendFiltered(in []*T, o *FilteringOpts) (filtered []*T, lastID string, err error) {
	return r.m.AppendFiltered(in, o)
}

// AppendFilteredIDs will attempt to append all entry IDs associated with a set of given filters
func (r *ReadWrapper[T, V]) AppendFilteredIDs(in []string, o *FilteringOpts) (filtered []string, lastID string, err error) {
	return r.m.AppendFilteredIDs(in, o)
}

// GetFirst will attempt to get the first entry which matches the provided filters
// Note: Will return ErrEntryNotFound if no match is found
func (r *ReadWrapper[T, V]) GetFirst(o *FilteringOpts) (val *T, err error) {
	return r.m.GetFirst(o)
}

// GetLast will attempt to get the last entry which matches the provided filters
// Note: Will return ErrEntryNotFound if no match is found
func (r *ReadWrapper[T, V]) GetLast(o *FilteringOpts) (val *T, err error) {
	return r.m.GetLast(o)
}

// ForEach will iterate through each of the entries
func (r *ReadWrapper[T, V]) ForEach(fn ForEachFn[T, V], o *FilteringOpts) (err error) {
	return r.m.ForEach(fn, o)
}

// ForEachID will iterate through each of the entry IDs
func (r *ReadWrapper[T, V]) ForEachID(fn ForEachIDFn, o *FilteringOpts) (err error) {
	return r.m.ForEachID(fn, o)
}
