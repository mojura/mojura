package core

type call struct {
	fn   TransactionFn
	errC chan error
}

func (c *call) notify(err error) {
	c.fn = nil
	c.errC <- err
	close(c.errC)
}
