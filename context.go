package core

import (
	"context"
	"sync"
	"time"
)

func newContext(ctx context.Context, duration time.Duration) *Context {
	var c Context
	c.Context, c.cancel = context.WithCancel(ctx)
	c.duration = duration
	c.timer = time.NewTimer(duration)
	go c.closeOnExpire()
	return &c
}

// Context represents an expiring context that can be refreshed by Touching
type Context struct {
	context.Context

	mux sync.Mutex

	duration time.Duration

	timer  *time.Timer
	cancel context.CancelFunc
}

func (c *Context) closeOnExpire() {
	<-c.timer.C
	c.cancel()
}

func (c *Context) isDone() (done bool) {
	select {
	case <-c.Done():
		done = true
	}

	return
}

// Touch will refesh the context timer
func (c *Context) Touch() (ok bool) {
	c.mux.Lock()
	defer c.mux.Unlock()

	// Attempt to stop the timer
	if !c.timer.Stop() {
		// Timer has already been stopped or expired, return
		return false
	}

	// Reset timer with context duration
	c.timer.Reset(c.duration)
	return true
}
