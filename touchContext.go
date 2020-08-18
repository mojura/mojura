package dbl

import (
	"context"
	"sync"
	"time"

	"github.com/hatchify/atoms"
)

// NewTouchContext will create a new timeout touch context
func NewTouchContext(ctx context.Context, duration time.Duration) *TouchContext {
	var c TouchContext
	c.Context, c.cancel = context.WithCancel(ctx)
	c.duration = duration
	c.timer = time.NewTimer(duration)
	go c.closeOnExpire()
	go c.stopTimerOnDone()
	return &c
}

// TouchContext represents an expiring context that can be refreshed by Touching
type TouchContext struct {
	context.Context

	mux sync.RWMutex

	duration time.Duration

	timer  *time.Timer
	cancel context.CancelFunc

	timedOut atoms.Bool
	err      error
}

func (c *TouchContext) closeOnExpire() {
	<-c.timer.C
	c.timedOut.Set(true)
	c.mux.Lock()
	defer c.mux.Unlock()
	c.cancel()
}

func (c *TouchContext) stopTimerOnDone() {
	<-c.Done()

	c.mux.Lock()
	defer c.mux.Unlock()
	c.timer.Stop()
}

// Touch will refesh the context timer
func (c *TouchContext) Touch() (ok bool) {
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
func (c *TouchContext) Err() (err error) {
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
