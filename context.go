package dbl

import (
	"context"
	"sync"
	"time"

	"github.com/hatchify/atoms"
)

func newContext(ctx context.Context, duration time.Duration) *Context {
	var c Context
	c.Context, c.cancel = context.WithCancel(ctx)
	c.duration = duration
	c.timer = time.NewTimer(duration)
	go c.closeOnExpire()
	go c.stopTimerOnDone()
	return &c
}

// Context represents an expiring context that can be refreshed by Touching
type Context struct {
	context.Context

	mux sync.RWMutex

	duration time.Duration

	timer  *time.Timer
	cancel context.CancelFunc

	timedOut atoms.Bool
	err      error
}

func (c *Context) closeOnExpire() {
	<-c.timer.C
	c.timedOut.Set(true)
	c.mux.Lock()
	defer c.mux.Unlock()
	c.cancel()
}

func (c *Context) stopTimerOnDone() {
	<-c.Done()

	c.mux.Lock()
	defer c.mux.Unlock()
	c.timer.Stop()
}

func (c *Context) isDone() (done bool) {
	select {
	case <-c.Done():
		done = true
	}

	return
}

// Touch will refresh the context timer
func (c *Context) Touch() (ok bool) {
	c.mux.Lock()
	defer c.mux.Unlock()

	// Attempt to stop the timer
	if !c.timer.Stop() {
		c.timedOut.Set(true)
		// Timer has already been stopped or expired, return
		return false
	}

	// Reset timer with context duration
	c.timer.Reset(c.duration)
	return true
}

// Err will return the underlying error
func (c *Context) Err() (err error) {
	c.mux.RLock()
	defer c.mux.RUnlock()
	// Check to see if context timed out
	if c.timedOut.Get() {
		// Context timed out, return time out error
		return ErrTransactionTimedOut
	}

	// Get error from context
	if err = c.Context.Err(); err != nil {
		// Error exists, return
		return
	}

	// Context does not have an error associated, return a generic context cancelled error
	return ErrContextCancelled
}
