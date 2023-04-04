package mojura

import "context"

type call[T Value] struct {
	fn   TransactionFn[T]
	ctx  context.Context
	errC chan error
}

func (c *call[T]) notify(err error) {
	c.fn = nil
	c.errC <- err
	close(c.errC)
}
