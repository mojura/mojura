package dbl

import "context"

type call struct {
	fn   TransactionFn
	ctx  context.Context
	errC chan error
}

func (c *call) notify(err error) {
	c.fn = nil
	c.errC <- err
	close(c.errC)
}
