package mojura

type calls[T Value] []call[T]

func (c calls[T]) notifyAll(err error) {
	for _, call := range c {
		call.notify(err)
	}
}
