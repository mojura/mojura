package dbl

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hatchify/errors"
)

func newBatcher(core *Core) *batcher {
	var b batcher
	b.core = core
	return &b
}

type batcher struct {
	mux sync.Mutex

	core *Core

	timer *time.Timer
	calls []call
}

func (b *batcher) performCalls(txn *Transaction, cs calls) (failIndex int, err error) {
	failIndex = -1
	for i, c := range cs {
		if err = recoverCall(txn, c.fn); err != nil {
			failIndex = i
			return
		}
	}

	return
}

func (b *batcher) clearTimer() {
	if b.timer == nil {
		return
	}

	// Stop timer
	b.timer.Stop()

	// Clear timer
	b.timer = nil
}

// run performs the transactions in the batch and communicates results
// back to DB.Batch.
func (b *batcher) run(ctx context.Context, cs calls) {
	if len(cs) == 0 {
		// We have no calls to run, bail out
		return
	}

	var failIndex int
	err := b.core.Transaction(context.Background(), func(txn *Transaction) (err error) {
		failIndex, err = b.performCalls(txn, cs)
		return
	})

	if err == errors.ErrIsClosed {
		cs.notifyAll(err)
		return
	}

	// Check to see if we had no failures in our batch
	if failIndex == -1 {
		// We successfully batched our list of calls without error, notify all calls of nil error status
		cs.notifyAll(nil)
		return
	}

	// Create group for successful calls
	successful := cs[:failIndex]

	// Attempt to retry the successful group before the failing call
	b.retry(ctx, successful, err)

	// Send error down error channel to call who caused issue
	cs[failIndex].errC <- err

	// Create group for remaining calls
	remaining := cs[failIndex+1:]

	// Run the remaining calls
	b.run(ctx, remaining)
}

func (b *batcher) retry(ctx context.Context, cs calls, err error) {
	if b.core.opts.RetryBatchFail {
		// Re-run the successful portion
		// Note: This is expected to pass
		b.run(ctx, cs)
		return
	}

	groupErr := fmt.Errorf("error occurred within batch, but not within this request: %v", err)
	// Notify group
	cs.notifyAll(groupErr)
}

func (b *batcher) flush(ctx context.Context) {
	// Clear the timer
	b.clearTimer()

	// Run the batcher
	b.run(ctx, b.calls)

	// Reset calls buffer
	b.calls = b.calls[:0]
}

func (b *batcher) Append(fn TransactionFn) (errC chan error) {
	b.mux.Lock()
	defer b.mux.Unlock()

	// TODO: discuss implementation of context on Batch
	ctx := context.Background()

	var c call
	c.fn = fn
	c.errC = make(chan error, 1)

	// Append calls to calls buffer
	b.calls = append(b.calls, c)

	// If length of calls equals or exceeds MaxBatchCalls, run the current calls
	if len(b.calls) >= b.core.opts.MaxBatchCalls {
		// Since we've matched or exceeded our MaxBatchCalls, manually flush the calls buffer and return
		b.flush(ctx)
		return
	}

	if b.timer == nil {
		// Set func to run after MaxBatchDuration
		b.timer = time.AfterFunc(b.core.opts.MaxBatchDuration, func() {
			b.Run(ctx)
		})
	}

	return c.errC
}

// Run triggers the current set of calls to be ran
func (b *batcher) Run(ctx context.Context) {
	b.mux.Lock()
	defer b.mux.Unlock()

	// Flush the calls buffer
	b.flush(ctx)
}
