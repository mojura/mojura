package mojura

// MakeWrapper combines read and write convenience methods without owning m.
func MakeWrapper[T Value](m *Mojura[T]) (w Wrapper[T]) {
	w.ReadWrapper = MakeReadWrapper(m)
	w.WriteWrapper = MakeWriteWrapper(m)
	return
}

// Wrapper combines ReadWrapper and WriteWrapper for the same database.
type Wrapper[T Value] struct {
	ReadWrapper[T]
	WriteWrapper[T]
}
