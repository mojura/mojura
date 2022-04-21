package mojura

type calls[T any, V Value[T]] []call[T, V]

func (c calls[T, V]) notifyAll(err error) {
	for _, call := range c {
		call.notify(err)
	}
}
