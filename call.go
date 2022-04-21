package mojura

import "context"

type call[T any, V Value[T]] struct {
	fn   TransactionFn[T, V]
	ctx  context.Context
	errC chan error
}

func (c *call[T, V]) notify(err error) {
	c.fn = nil
	c.errC <- err
	close(c.errC)
}
