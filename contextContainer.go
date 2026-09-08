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

	ctx context.Context
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

func (c *contextContainer) update(ctx context.Context) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.ctx = ctx
}
