package core

type call struct {
	fn   TransactionFn
	errC chan<- error
}

type calls []call

func (c calls) notifyAll(err error) {
	for _, call := range c {
		call.errC <- err
	}
}
