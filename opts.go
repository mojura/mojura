package mojura

import (
	"time"

	"github.com/gdbu/errors"
	"github.com/mojura-backends/bolt"
	"github.com/mojura/backend"
	"github.com/mojura/kiroku"
	"github.com/mojura/mojura/action"
)

const (
	// DefaultMaxBatchCalls is the default maximum number of calls a batch will take
	DefaultMaxBatchCalls = 1024
	// DefaultMaxBatchDuration is the default maximum duration a batch will take to collect calls
	DefaultMaxBatchDuration = time.Millisecond * 10
	// DefaultRetryBatchFail is a declared retry default; Opts.fill does not apply it.
	// Set Opts.RetryBatchFail explicitly to enable retries.
	DefaultRetryBatchFail = true
	// DefaultIndexLength is the default index length
	DefaultIndexLength = 8
)

const (
	// ErrEmptyEncoder is a legacy sentinel; a nil Encoder currently selects JSON.
	ErrEmptyEncoder = errors.Error("invalid encoder, cannot be empty")
)

var defaultOpts = Opts{
	MaxBatchCalls:    DefaultMaxBatchCalls,
	MaxBatchDuration: DefaultMaxBatchDuration,
	RetryBatchFail:   DefaultRetryBatchFail,

	Initializer: bolt.New(),
	Encoder:     &JSONEncoder{},
}

// MakeOpts sets Name and Dir only. New or Validate fills the remaining defaults.
// Create dir before opening a database with the default backend.
func MakeOpts(name, dir string) (o Opts) {
	o.Name = name
	o.Dir = dir
	return
}

// Opts configures a Mojura instance. TOML tags do not provide a configuration loader.
type Opts struct {
	// Options holds the embedded Kiroku name, paths, and history/consumer settings.
	kiroku.Options

	// IndexLength is the minimum decimal ID width; zero becomes 8.
	IndexLength int `toml:"index_length"`
	// MaxBatchCalls is the size trigger; zero becomes 1024. See Batch's blocking limitation.
	MaxBatchCalls int `toml:"max_batch_calls"`
	// MaxBatchDuration is the flush timer; zero becomes 10 milliseconds.
	MaxBatchDuration time.Duration `toml:"max_batch_duration"`

	// RetryBatchFail retries earlier callbacks after a later callback fails; defaults to false.
	RetryBatchFail bool `toml:"retry_batch_fail"`
	// IsMirror rejects public writes and consumes Source when one is configured.
	IsMirror bool `toml:"is_mirror"`
	// IgnoreEmptyRelationshipKeys is unused; empty relationship IDs are always skipped.
	IgnoreEmptyRelationshipKeys bool `toml:"ignore_empty_relationship_keys"`

	// Initializer opens the backend; nil selects the default Bolt adapter.
	Initializer backend.Initializer
	// Encoder serializes entry payloads; nil selects JSONEncoder.
	Encoder Encoder

	// OnImport runs after a successful backend import; consume its reader during the callback.
	OnImport func(kiroku.Type, *action.Reader)

	// Source imports/exports Kiroku history. Nil does not configure a retained backup.
	Source kiroku.Source
	// Logger receives Mojura and forwarded Kiroku messages; nil selects NewLogger.
	Logger Logger
}

// Validate fills zero-value defaults and validates the embedded Kiroku options.
// Name and Dir must be non-empty. It does not comprehensively validate numeric settings.
func (o *Opts) Validate() (err error) {
	o.fill()
	return o.Options.Validate()
}

func (o *Opts) fill() {
	if o.Encoder == nil {
		o.Encoder = defaultOpts.Encoder
	}

	if o.Initializer == nil {
		o.Initializer = defaultOpts.Initializer
	}

	if o.MaxBatchCalls == 0 {
		o.MaxBatchCalls = DefaultMaxBatchCalls
	}

	if o.MaxBatchDuration == 0 {
		o.MaxBatchDuration = DefaultMaxBatchDuration
	}

	if o.IndexLength == 0 {
		o.IndexLength = DefaultIndexLength
	}
}
