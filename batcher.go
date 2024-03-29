package mojura

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hatchify/errors"
)

func newBatcher[T Value](m *Mojura[T]) *batcher[T] {
	var b batcher[T]
	b.m = m
	return &b
}

type batcher[T Value] struct {
	mux sync.Mutex

	m *Mojura[T]

	timer *time.Timer
	calls []call[T]
}

func (b *batcher[T]) performCalls(txn *Transaction[T], cs calls[T]) (failIndex int, err error) {
	failIndex = -1
	for i, c := range cs {
		// Update transaction context
		txn.cc.update(c.ctx)

		// Pass call func to recoverCall
		if err = recoverCall(txn, c.fn); err != nil {
			failIndex = i
			return
		}
	}

	return
}

func (b *batcher[T]) clearTimer() {
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
func (b *batcher[T]) run(cs calls[T]) {
	if len(cs) == 0 {
		// We have no calls to run, bail out
		return
	}

	var failIndex int
	err := b.m.Transaction(context.Background(), func(txn *Transaction[T]) (err error) {
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
	b.retry(successful, err)

	// Send error down error channel to call who caused issue
	cs[failIndex].notify(err)

	// Create group for remaining calls
	remaining := cs[failIndex+1:]

	// Run the remaining calls
	b.run(remaining)
}

func (b *batcher[T]) retry(cs calls[T], err error) {
	if b.m.opts.RetryBatchFail {
		// Re-run the successful portion
		// Note: This is expected to pass
		b.run(cs)
		return
	}

	groupErr := fmt.Errorf("error occurred within batch, but not within this request: %v", err)
	// Notify group
	cs.notifyAll(groupErr)
}

func (b *batcher[T]) flush() {
	// Clear the timer
	b.clearTimer()

	// Run the batcher
	b.run(b.calls)

	// Reset calls buffer
	b.calls = b.calls[:0]
}

func (b *batcher[T]) Append(ctx context.Context, fn TransactionFn[T]) (errC chan error) {
	b.mux.Lock()
	defer b.mux.Unlock()

	var c call[T]
	c.fn = fn
	c.ctx = ctx
	c.errC = make(chan error, 1)

	// Append calls to calls buffer
	b.calls = append(b.calls, c)

	// If length of calls equals or exceeds MaxBatchCalls, run the current calls
	if len(b.calls) >= b.m.opts.MaxBatchCalls {
		// Since we've matched or exceeded our MaxBatchCalls, manually flush the calls buffer and return
		b.flush()
		return
	}

	if b.timer == nil {
		// Set func to run after MaxBatchDuration
		b.timer = time.AfterFunc(b.m.opts.MaxBatchDuration, b.Run)
	}

	return c.errC
}

// Run triggers the current set of calls to be ran
func (b *batcher[T]) Run() {
	b.mux.Lock()
	defer b.mux.Unlock()

	// Flush the calls buffer
	b.flush()
}
