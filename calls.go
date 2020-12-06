package mojura

type calls []call

func (c calls) notifyAll(err error) {
	for _, call := range c {
		call.notify(err)
	}
}
