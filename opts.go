package core

import "time"

const (
	// DefaultMaxBatchCalls is the default maximum number of calls a batch will take
	DefaultMaxBatchCalls = 1024
	// DefaultMaxBatchDuration is the default maximum duration a batch will take to collect calls
	DefaultMaxBatchDuration = time.Millisecond * 10
	// DefaultRetryBatchFail is the default value for if a batch call will retry when a batch sibling fails
	DefaultRetryBatchFail = true
)

var defaultOpts = Opts{
	MaxBatchCalls:    DefaultMaxBatchCalls,
	MaxBatchDuration: DefaultMaxBatchDuration,
	RetryBatchFail:   DefaultRetryBatchFail,
}

// Opts represent service core options
type Opts struct {
	MaxBatchCalls    int
	MaxBatchDuration time.Duration
	RetryBatchFail   bool
}
