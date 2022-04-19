package mojura

func MakeWriteWrapper[T Value](m Mojura[T]) (w WriteWrapper[T]) {
	w.m = m
	return
}
func MakeWrapper[T Value](m Mojura[T]) (w Wrapper[T]) {
	w.ReadWrapper = MakeReadWrapper(m)
	w.WriteWrapper = MakeWriteWrapper(m)
	return
}

type Wrapper[T Value] struct {
	ReadWrapper[T]
	WriteWrapper[T]
}
