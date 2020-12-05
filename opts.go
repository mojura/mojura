package dbl

import (
	"time"

	"github.com/hatchify/errors"
	"github.com/mojura/backend"
)

const (
	// DefaultMaxBatchCalls is the default maximum number of calls a batch will take
	DefaultMaxBatchCalls = 1024
	// DefaultMaxBatchDuration is the default maximum duration a batch will take to collect calls
	DefaultMaxBatchDuration = time.Millisecond * 10
	// DefaultRetryBatchFail is the default value for if a batch call will retry when a batch sibling fails
	DefaultRetryBatchFail = true
)

// DefaultEncoder represents the default encoder used by DBL
var DefaultEncoder JSONEncoder

const (
	// ErrEmptyEncoder is returned when an encoder is unset
	ErrEmptyEncoder = errors.Error("invalid encoder, cannot be empty")
)

var defaultOpts = Opts{
	MaxBatchCalls:    DefaultMaxBatchCalls,
	MaxBatchDuration: DefaultMaxBatchDuration,
	RetryBatchFail:   DefaultRetryBatchFail,
}

// Opts represent dbl options
type Opts struct {
	MaxBatchCalls    int
	MaxBatchDuration time.Duration
	RetryBatchFail   bool

	Initializer backend.Initializer
	Encoder     Encoder
}

// Validate will validate a set of Options
func (o *Opts) Validate() (err error) {
	o.init()
	return
}

func (o *Opts) init() {
	if o.Encoder == nil {
		o.Encoder = &DefaultEncoder
	}

	if o.MaxBatchCalls == 0 {
		o.MaxBatchCalls = DefaultMaxBatchCalls
	}

	if o.MaxBatchDuration == 0 {
		o.MaxBatchDuration = DefaultMaxBatchDuration
	}
}
