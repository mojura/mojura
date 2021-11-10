package mojura

import (
	"context"
	"sync"
)

func newContextContainer(ctx context.Context) *contextContainer {
	var c contextContainer
	c.ctx = ctx
	return &c
}

type contextContainer struct {
	mux sync.RWMutex

	ctx    context.Context
	cancel chan struct{}
	done   chan struct{}

	err    error
	closed bool
}

func (c *contextContainer) isDone() (err error) {
	c.mux.RLock()
	defer c.mux.RUnlock()
	// Check to see if context is done
	if !isDone(c.ctx) {
		// Context is not done, return
		return
	}

	// Attempt to get error from context
	if err = c.ctx.Err(); err != nil {
		// Context has an error, return
		return
	}

	// Set error as default cancelled error
	err = ErrContextCancelled
	return
}

func (c *contextContainer) cancelCurrent() {
	select {
	case c.cancel <- struct{}{}:
	default:
	}
}

func (c *contextContainer) update(ctx context.Context) (ok bool) {
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.closed {
		return
	}

	// Set context
	c.ctx = ctx
	// Set true
	ok = true
	// Cancel current context waiter if exists
	c.cancelCurrent()
	// Start new context waiter
	go c.waitForClose()
	return
}

func (c *contextContainer) getContext() (ctx context.Context) {
	c.mux.RLock()
	defer c.mux.RUnlock()
	ctx = c.ctx
	return
}

func (c *contextContainer) waitForClose() {
	ctx := c.getContext()

	select {
	case <-ctx.Done():
		c.setError(ctx.Err())
		closeSema(c.done)
	case <-c.cancel:
	}
}

func (c *contextContainer) setError(err error) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.err = err
}

func (c *contextContainer) Done() <-chan struct{} {
	return c.done
}

func (c *contextContainer) Err() (err error) {
	c.mux.RLock()
	defer c.mux.RUnlock()
	err = c.err
	return
}

func (c *contextContainer) Close() {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.cancelCurrent()
	closeSema(c.done)
	closeSema(c.cancel)
	c.closed = true
}
