package mojura

type TypedValue[T any] interface {
	Value
	*T
}
