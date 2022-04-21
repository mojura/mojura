package mojura

func MakeWrapper[T any, V Value[T]](m *Mojura[T, V]) (w Wrapper[T, V]) {
	w.ReadWrapper = MakeReadWrapper(m)
	w.WriteWrapper = MakeWriteWrapper(m)
	return
}

type Wrapper[T any, V Value[T]] struct {
	ReadWrapper[T, V]
	WriteWrapper[T, V]
}
