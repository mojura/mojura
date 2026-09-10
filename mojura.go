package mojura

import (
	"context"
	"fmt"
	"log"
	"path"
	"sync"

	"github.com/gdbu/stopwatch"

	"github.com/gdbu/errors"

	"github.com/mojura/backend"
	"github.com/mojura/kiroku"
	"github.com/mojura/mojura/action"
)

const (
	// ErrNotInitialized is returned when a service has not been properly initialized
	ErrNotInitialized = errors.Error("service has not been properly initialized")
	// ErrRelationshipNotFound is returned when an index key is not available.
	ErrRelationshipNotFound = errors.Error("relationship was not found")
	// ErrEntryNotFound is returned when an entry is not available for the given ID
	ErrEntryNotFound = errors.Error("entry was not found")
	// ErrEndOfEntries is a legacy sentinel. Current cursors return Break on exhaustion.
	ErrEndOfEntries = errors.Error("end of entries")
	// ErrInvalidNumberOfRelationships reports a mismatch between constructor keys and zero-value relationship slots.
	ErrInvalidNumberOfRelationships = errors.Error("invalid number of relationships")
	// ErrInvalidType is a legacy sentinel with no current return path.
	ErrInvalidType = errors.Error("invalid type encountered, please check generators")
	// ErrInvalidEntries is a legacy sentinel from the removed GetByRelationship API.
	ErrInvalidEntries = errors.Error("invalid entries, slice expected")
	// ErrEmptyFilters is returned by internal multi-cursor construction for an empty filter list.
	// Public queries with no filters use an unfiltered cursor instead.
	ErrEmptyFilters = errors.Error("invalid relationship pairs, cannot be empty")
	// ErrContextCancelled is returned when a transaction ends early from context
	ErrContextCancelled = errors.Error("context cancelled")
	// ErrInvalidBlockWriter is a legacy sentinel with no current return path.
	ErrInvalidBlockWriter = errors.Error("invalid block writer, cannot be used for the requested action")
	// ErrEmptyEntryID is returned when an entry ID is empty
	ErrEmptyEntryID = errors.Error("invalid entry ID, cannot be empty")
	// ErrMirrorCannotPerformWriteActions is returned when write actions are called on a mirror
	ErrMirrorCannotPerformWriteActions = errors.Error("mirrors cannot perform write actions")
	// Break is a non-error which will cause a ForEach loop to break early
	Break = errors.Error("break!")
)

var (
	entriesBktKey       = []byte("entries")
	relationshipsBktKey = []byte("relationships")
	lookupsBktKey       = []byte("lookups")
	metaBktKey          = []byte("meta")
)

// New opens a database for T with positional relationship index keys.
// It fills and validates opts and checks the relationship count on a fresh T.
// Prefer a concrete pointer type embedding Entry by value. Create opts.Dir before
// calling New with the default backend. Close the database after successful use.
// Initialization failures do not consistently clean up previously opened resources.
func New[T Value](opts Opts, relationships ...string) (mp *Mojura[T], err error) {
	var m Mojura[T]
	if m, err = makeMojura[T](opts, relationships); err != nil {
		return
	}

	// Initialize new batcher
	m.b = newBatcher(&m)

	mp = &m
	return
}

func makeMojura[T Value](opts Opts, relationships []string) (m Mojura[T], err error) {
	if err = opts.Validate(); err != nil {
		return
	}

	m.make = makeType[T]()
	t := m.make()
	if len(t.GetRelationships()) != len(relationships) {
		err = ErrInvalidNumberOfRelationships
		return
	}

	if opts.Logger == nil {
		opts.Logger = NewLogger()
	}

	m.out = opts.Logger
	opts.OnLog = func(message string) { m.out.Info(message) }
	opts.OnError = func(err error) { m.out.Error(err.Error()) }
	m.opts = &opts
	m.indexFmt = fmt.Sprintf("%s0%dd", "%", opts.IndexLength)

	if err = m.init(relationships); err != nil {
		return
	}

	log.Writer()

	return
}

// Mojura stores typed values, relationship indexes, and Kiroku history.
type Mojura[T Value] struct {
	// Closed state mutex
	mux sync.RWMutex

	db  backend.Backend
	out Logger
	b   *batcher[T]

	make func() T

	p *kiroku.Producer
	c closer

	opts     *Opts
	indexFmt string

	relationships [][]byte

	// Closed state
	closed bool
}

func (m *Mojura[T]) init(relationships []string) (err error) {
	filename := path.Join(m.opts.Dir, m.opts.FullName()+".bdb")
	if m.db, err = m.opts.Initializer.New(filename); err != nil {
		return fmt.Errorf("error opening db for %s (%s): %v", m.opts.Name, m.opts.Dir, err)
	}

	// Set relationships
	m.relationships = getRelationshipsAsBytes(relationships)

	if err = m.db.Transaction(func(txn backend.Transaction) (err error) {
		if _, err = txn.GetOrCreateBucket(entriesBktKey); err != nil {
			return
		}

		if _, err = txn.GetOrCreateBucket(lookupsBktKey); err != nil {
			return
		}

		if _, err = txn.GetOrCreateBucket(metaBktKey); err != nil {
			return
		}

		var relationshipsBkt backend.Bucket
		if relationshipsBkt, err = txn.GetOrCreateBucket(relationshipsBktKey); err != nil {
			return
		}

		for _, relationship := range m.relationships {
			if _, err = relationshipsBkt.GetOrCreateBucket(relationship); err != nil {
				return
			}
		}

		return
	}); err != nil {
		return
	}

	if !m.opts.IsMirror {
		err = m.primaryInitialization()
	} else {
		err = m.mirrorInitialization()
	}

	return
}

func (m *Mojura[T]) initBuckets(txn backend.Transaction) (err error) {
	if _, err = txn.GetOrCreateBucket(entriesBktKey); err != nil {
		return
	}

	if _, err = txn.GetOrCreateBucket(lookupsBktKey); err != nil {
		return
	}

	if _, err = txn.GetOrCreateBucket(metaBktKey); err != nil {
		return
	}

	return m.initRelationshipsBuckets(txn)
}

func (m *Mojura[T]) initRelationshipsBuckets(txn backend.Transaction) (err error) {
	var relationshipsBkt backend.Bucket
	if relationshipsBkt, err = txn.GetOrCreateBucket(relationshipsBktKey); err != nil {
		return
	}

	for _, relationship := range m.relationships {
		if _, err = relationshipsBkt.GetOrCreateBucket(relationship); err != nil {
			return
		}
	}

	return
}

func (m *Mojura[T]) primaryInitialization() (err error) {
	if m.opts.Source != nil {
		if err = kiroku.NewOneShotConsumer(m.opts.Options, m.opts.Source, m.onImport); err != nil {
			return
		}
	}

	if m.p, err = kiroku.NewProducer(m.opts.Options, m.opts.Source); err != nil {
		err = fmt.Errorf("error initializing kiroku: %v", err)
		return
	}

	m.c = m.p
	return m.buildHistory()
}

func (m *Mojura[T]) mirrorInitialization() (err error) {
	if m.opts.Source == nil {
		return
	}

	if m.c, err = kiroku.NewConsumer(m.opts.Options, m.opts.Source, m.onImport); err != nil {
		err = fmt.Errorf("error initializing kiroku: %v", err)
		return
	}

	return
}

func (m *Mojura[T]) buildHistory() (err error) {
	var meta kiroku.Meta
	if meta, err = m.p.Meta(); err != nil {
		return
	}

	if meta.LastProcessedTimestamp > 0 {
		return
	}

	var hasEntries bool
	if err = m.ReadTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		hasEntries, err = m.hasEntries(txn)
		return
	}); err != nil {
		return
	}

	if !hasEntries {
		return
	}

	var n int64
	m.out.Info("Found populated database with an empty history file, building history file from database entries")
	if err = m.importTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		if n, err = m.dumpHistory(txn); err != nil {
			err = fmt.Errorf("error encountered while dumping to history file: %v", err)
			return
		}

		return
	}); err != nil {
		return
	}

	m.out.Info("Appended %d blocks to the history file", n)
	return
}

func (m *Mojura[T]) dumpHistory(txn *Transaction[T]) (n int64, err error) {
	var bkt backend.Bucket
	if bkt, err = txn.getEntriesBucket(); err != nil {
		err = fmt.Errorf("error getting entries bucket: %v", err)
		return
	}

	var lastIndex uint64
	if err = m.p.Transaction(func(txn *kiroku.Transaction) (err error) {
		cur := bkt.Cursor()
		aw := action.MakeWriter(txn)
		for key, value := cur.First(); len(key) > 0; key, value = cur.Next() {
			if err = aw.Write(key, value); err != nil {
				return
			}

			if parsed, err := parseIDAsIndex(key); err == nil {
				lastIndex = parsed
			}

			n++
		}

		return
	}); err != nil {
		return
	}

	txn.setIndex(lastIndex + 1)
	return
}

func (m *Mojura[T]) purge(txn backend.Transaction) (err error) {
	if err = txn.DeleteBucket(lookupsBktKey); err != nil {
		return
	}

	if err = txn.DeleteBucket(metaBktKey); err != nil {
		return
	}

	if err = txn.DeleteBucket(relationshipsBktKey); err != nil {
		return
	}

	return m.initBuckets(txn)
}

func (m *Mojura[T]) marshal(val interface{}) (bs []byte, err error) {
	return m.opts.Encoder.Marshal(val)
}

func (m *Mojura[T]) unmarshal(bs []byte, val interface{}) (err error) {
	return m.opts.Encoder.Unmarshal(bs, val)
}

func (m *Mojura[T]) newValueFromBytes(bs []byte) (val T, err error) {
	err = m.unmarshal(bs, &val)
	return
}

func (m *Mojura[T]) onImport(t kiroku.Type, r *kiroku.Reader) (err error) {
	if err = m.importTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		return m.importReader(txn, t, r)
	}); err != nil {
		return
	}

	if m.opts.OnImport == nil {
		return
	}

	ar := action.MakeReader(r)
	m.opts.OnImport(t, &ar)
	return
}

func (m *Mojura[T]) importReader(txn *Transaction[T], t kiroku.Type, r *kiroku.Reader) (err error) {
	var sw stopwatch.Stopwatch
	sw.Start()
	if t == kiroku.TypeSnapshot {
		// Snapshot import resets indexes and metadata; purge currently retains entries.
		if err = m.purge(txn.txn); err != nil {
			return
		}
	}

	var count int
	// Iterate through all entries from a given point within Reader
	if err = r.ForEach(0, func(b kiroku.Block) (err error) {
		count++
		return txn.processBlock(b)
	}); err != nil {
		err = fmt.Errorf("Mojura.importReader(): error during r.ForEach: %v", err)
		return
	}

	m.out.Info("Successfully processed %d blocks in %v", count, sw.Stop())
	return
}

func (m *Mojura[T]) transaction(fn func(backend.Transaction, *kiroku.Transaction) (Transaction[T], error)) (err error) {
	err = m.db.Transaction(func(txn backend.Transaction) (err error) {
		var t Transaction[T]
		err = m.p.Transaction(func(ktxn *kiroku.Transaction) (err error) {
			t, err = fn(txn, ktxn)
			return
		})
		defer t.teardown()
		return
	})

	return
}

func (m *Mojura[T]) runTransaction(ctx context.Context, txn backend.Transaction, bw action.BlockWriter, fn TransactionFn[T]) (t Transaction[T], err error) {
	t = newTransaction(ctx, m, txn, bw)
	if bw != nil {
		// We only need to load meta for write transactions
		if err = t.loadMeta(); err != nil {
			return
		}
		defer func() {
			if err == nil {
				err = t.saveMeta()
			}
		}()
	}
	errCh := make(chan error)

	// Call function from within goroutine
	go func() {
		// Pass returning error to error channel
		errCh <- fn(&t)
	}()

	select {
	case err = <-errCh:
	case <-t.cc.Done():
		// Context is done, attempt to set error from Context
		if err = t.cc.Err(); err != nil {
			return
		}

		err = ErrContextCancelled
	}

	return
}

func (m *Mojura[T]) importTransaction(ctx context.Context, fn func(*Transaction[T]) error) (err error) {
	err = m.db.Transaction(func(txn backend.Transaction) (err error) {
		var t Transaction[T]
		t, err = m.runTransaction(ctx, txn, nopBW, fn)
		defer t.teardown()
		return
	})

	return
}

func (m *Mojura[T]) hasEntries(txn *Transaction[T]) (ok bool, err error) {
	var bkt backend.Bucket
	if bkt, err = txn.getEntriesBucket(); err != nil {
		err = fmt.Errorf("error getting entries bucket: %v", err)
		return
	}

	ok = hasEntries(bkt)
	return
}

func (m *Mojura[T]) copyEntries(txn *Transaction[T]) (err error) {
	var bkt backend.Bucket
	if bkt, err = txn.getEntriesBucket(); err != nil {
		return
	}

	writeFn := func(ss *kiroku.Snapshot) (err error) {
		aw := action.MakeWriter(ss)
		return bkt.ForEach(aw.Write)
	}

	return m.p.Snapshot(writeFn)
}

func (m *Mojura[T]) reindex(txn *Transaction[T]) (err error) {
	if err = txn.txn.DeleteBucket(relationshipsBktKey); err != nil {
		return
	}

	if err = m.initRelationshipsBuckets(txn.txn); err != nil {
		return
	}

	fn := func(entryID string, t T) (err error) {
		return txn.setRelationships(t.GetRelationships(), []byte(entryID))
	}

	return txn.ForEach(fn, nil)
}

// New assigns the next generated ID and stores val and its relationship indexes.
// It mutates val's ID and timestamps. Explicit IDs written with Put do not advance
// the counter and can be overwritten by later generated IDs.
func (m *Mojura[T]) New(val T) (created T, err error) {
	if m.opts.IsMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	err = m.Transaction(context.Background(), func(txn *Transaction[T]) (err error) {
		created, err = txn.new(val)
		return
	})

	return
}

// Exists reports whether an entry is stored at entryID.
func (m *Mojura[T]) Exists(entryID string) (exists bool, err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		exists, err = txn.exists([]byte(entryID))
		return
	})

	return
}

// Get will attempt to get an entry by ID
func (m *Mojura[T]) Get(entryID string) (val T, err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		val, err = txn.get([]byte(entryID))
		return
	})

	return
}

// GetFiltered will attempt to get the filtered entries
func (m *Mojura[T]) GetFiltered(o *FilteringOpts) (filtered []T, lastID string, err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		filtered, lastID, err = txn.GetFiltered(o)
		return
	})

	return
}

// GetFilteredIDs will attempt to get the filtered entry IDs
func (m *Mojura[T]) GetFilteredIDs(o *FilteringOpts) (filtered []string, lastID string, err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		filtered, lastID, err = txn.GetFilteredIDs(o)
		return
	})

	return
}

// AppendFiltered will attempt to append all entries associated with a set of given filters
func (m *Mojura[T]) AppendFiltered(in []T, o *FilteringOpts) (filtered []T, lastID string, err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		filtered, lastID, err = txn.appendFiltered(in, o)
		return
	})

	return
}

// AppendFilteredIDs will attempt to append all entry IDs associated with a set of given filters
func (m *Mojura[T]) AppendFilteredIDs(in []string, o *FilteringOpts) (filtered []string, lastID string, err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		filtered, lastID, err = txn.appendFilteredIDs(in, o)
		return
	})

	return
}

// GetFirst will attempt to get the first entry which matches the provided filters
// The options must be non-nil. It returns ErrEntryNotFound if no match is found.
func (m *Mojura[T]) GetFirst(o *FilteringOpts) (val T, err error) {
	if err = m.ReadTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		val, err = txn.getFirst(o)
		return
	}); err != nil {
		return
	}

	return
}

// GetLast will attempt to get the last entry which matches the provided filters
// The options must be non-nil. It returns ErrEntryNotFound if no match is found.
func (m *Mojura[T]) GetLast(o *FilteringOpts) (val T, err error) {
	if err = m.ReadTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		val, err = txn.getLast(o)
		return
	}); err != nil {
		return
	}

	return
}

// ForEach will iterate through each of the entries
func (m *Mojura[T]) ForEach(fn ForEachFn[T], o *FilteringOpts) (err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		return txn.ForEach(fn, o)
	})

	return
}

// ForEachID will iterate through each of the entry IDs
func (m *Mojura[T]) ForEachID(fn ForEachIDFn, o *FilteringOpts) (err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		return txn.ForEachID(fn, o)
	})

	return
}

// Cursor will return an iterating cursor
func (m *Mojura[T]) Cursor(fn func(Cursor[T]) error, fs ...Filter) (err error) {
	if err = m.ReadTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		var c Cursor[T]
		if c, err = txn.cursor(fs); err != nil {
			return
		}

		return fn(c)
	}); err == Break {
		err = nil
	}

	return
}

// Put inserts or replaces val at entryID and updates its relationship indexes.
// It mutates val's ID/timestamps and does not advance the generated-ID counter.
// Use Update when the entry must already exist.
func (m *Mojura[T]) Put(entryID string, val T) (updated T, err error) {
	if m.opts.IsMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	err = m.Transaction(context.Background(), func(txn *Transaction[T]) (err error) {
		updated, err = txn.put([]byte(entryID), val)
		return
	})

	return
}

// Update will attempt to edit an entry by ID
func (m *Mojura[T]) Update(entryID string, fn UpdateFn[T]) (updated T, err error) {
	if m.opts.IsMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	err = m.Transaction(context.Background(), func(txn *Transaction[T]) (err error) {
		updated, err = txn.update([]byte(entryID), fn)
		return
	})

	return
}

// Delete removes an existing entry and its relationship memberships.
func (m *Mojura[T]) Delete(entryID string) (deleted T, err error) {
	if m.opts.IsMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	err = m.Transaction(context.Background(), func(txn *Transaction[T]) (err error) {
		deleted, err = txn.delete([]byte(entryID))
		return
	})

	return
}

// Transaction runs fn in a backend write transaction with Kiroku history recording.
// Return operation errors from fn to abort the backend transaction. Use only the
// supplied transaction inside fn; it must not escape or be shared by goroutines.
// Operations check ctx, but cancellation does not interrupt an arbitrary callback.
// Backend and history commits are separate and are not crash-atomic together.
func (m *Mojura[T]) Transaction(ctx context.Context, fn func(*Transaction[T]) error) (err error) {
	m.mux.RLock()
	defer m.mux.RUnlock()

	if m.opts.IsMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	err = m.transaction(func(txn backend.Transaction, ktxn *kiroku.Transaction) (Transaction[T], error) {
		return m.runTransaction(ctx, txn, ktxn, fn)
	})

	return
}

// ReadTransaction runs fn with a read-only backend transaction.
// Use the transaction only during fn. Context checks occur during operations;
// cancellation does not interrupt arbitrary callback code.
func (m *Mojura[T]) ReadTransaction(ctx context.Context, fn func(*Transaction[T]) error) (err error) {
	m.mux.RLock()
	defer m.mux.RUnlock()
	err = m.db.ReadTransaction(func(txn backend.Transaction) (err error) {
		var t Transaction[T]
		t, err = m.runTransaction(ctx, txn, nil, fn)
		t.teardown()
		return
	})

	return
}

// Batch groups concurrent callbacks into a write transaction.
// Callbacks may be repeated when RetryBatchFail is enabled.
// Known limitation: a size-triggered flush returns a nil completion channel and
// blocks its caller. Commit errors can also be lost; prefer Transaction until fixed.
func (m *Mojura[T]) Batch(ctx context.Context, fn func(*Transaction[T]) error) (err error) {
	if m.opts.IsMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	return <-m.b.Append(ctx, fn)
}

// Snapshot writes stored entry bytes to a Kiroku snapshot on a primary.
// Export to Source is handled by Kiroku and may complete after this call returns.
func (m *Mojura[T]) Snapshot(ctx context.Context) (err error) {
	if m.opts.IsMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	err = m.db.Transaction(func(btxn backend.Transaction) (err error) {
		txn := newTransaction(ctx, m, btxn, nil)
		return m.copyEntries(&txn)
	})

	return
}

// Reindex deletes and rebuilds all relationship indexes in one backend write
// transaction. It does not rewrite entries or history and is unavailable on mirrors.
func (m *Mojura[T]) Reindex(ctx context.Context) (err error) {
	if m.opts.IsMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	err = m.db.Transaction(func(btxn backend.Transaction) (err error) {
		txn := newTransaction(ctx, m, btxn, nil)
		return m.reindex(&txn)
	})

	return
}

// Close closes the backend, then the Kiroku producer or consumer.
// It does not drain pending batches; callers must coordinate shutdown with work.
// A second call returns github.com/gdbu/errors.ErrIsClosed.
func (m *Mojura[T]) Close() (err error) {
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.closed {
		return errors.ErrIsClosed
	}

	m.closed = true

	var errs errors.ErrorList
	errs.Push(m.db.Close())
	if m.c != nil {
		errs.Push(m.c.Close())
	}

	return errs.Err()
}

type closer interface {
	Close() error
}
