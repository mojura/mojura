package mojura

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/gdbu/scribe"
	"github.com/gdbu/stopwatch"

	"github.com/hatchify/errors"

	"github.com/mojura/backend"
	"github.com/mojura/kiroku"
)

const (
	// ErrNotInitialized is returned when a service has not been properly initialized
	ErrNotInitialized = errors.Error("service has not been properly initialized")
	// ErrRelationshipNotFound is returned when an relationship is not available for the given relationship key
	ErrRelationshipNotFound = errors.Error("relationship was not found")
	// ErrEntryNotFound is returned when an entry is not available for the given ID
	ErrEntryNotFound = errors.Error("entry was not found")
	// ErrEndOfEntries is returned when a cursor has reached the end of entries
	ErrEndOfEntries = errors.Error("end of entries")
	// ErrInvalidNumberOfRelationships is returned when an invalid number of relationships is provided in a New call
	ErrInvalidNumberOfRelationships = errors.Error("invalid number of relationships")
	// ErrInvalidType is returned when a type which does not match the generator is provided
	ErrInvalidType = errors.Error("invalid type encountered, please check generators")
	// ErrInvalidEntries is returned when a non-slice is presented to GetByRelationship
	ErrInvalidEntries = errors.Error("invalid entries, slice expected")
	// ErrEmptyFilters is returned when relationship pairs are empty for a filter or joined request
	ErrEmptyFilters = errors.Error("invalid relationship pairs, cannot be empty")
	// ErrContextCancelled is returned when a transaction ends early from context
	ErrContextCancelled = errors.Error("context cancelled")
	// ErrInvalidBlockWriter is called when NextIndex is called on a nopBlockWriters
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

// New will return a new instance of Mojura
func New[T Value](opts Opts, relationships ...string) (mp *Mojura[T], err error) {
	var m Mojura[T]
	if m, err = makeMojura[T](opts, relationships); err != nil {
		return
	}

	if !opts.IsMirror {
		if err = m.primaryInitialization(); err != nil {
			return
		}
	} else {
		if err = m.mirrorInitialization(); err != nil {
			return
		}
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

	m.out = scribe.New(fmt.Sprintf("Mojura (%s)", opts.Name))
	m.opts = &opts
	m.indexFmt = fmt.Sprintf("%s0%dd", "%", opts.IndexLength)

	if err = m.init(relationships); err != nil {
		return
	}

	return
}

// Mojura is the DB manager
type Mojura[T Value] struct {
	// Closed state mutex
	mux sync.RWMutex

	db  backend.Backend
	out *scribe.Scribe
	b   *batcher[T]

	make func() T

	k kiroku.Ledger

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

	err = m.db.Transaction(func(txn backend.Transaction) (err error) {
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
	})

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
	if m.k, err = kiroku.New(m.opts.Options, m.opts.Source); err != nil {
		err = fmt.Errorf("error initializing kiroku: %v", err)
		return
	}

	var built bool
	if built, err = m.buildHistory(); built || err != nil {
		return
	}

	return m.syncDatabase()
}

func (m *Mojura[T]) mirrorInitialization() (err error) {
	if m.k, err = kiroku.NewMirror(m.opts.Options, m.opts.Source); err != nil {
		err = fmt.Errorf("error initializing mirror: %v", err)
		return
	}

	if err = m.syncDatabase(); err != nil {
		return
	}

	mirror, ok := m.k.(*kiroku.Mirror)
	if !ok {
		err = fmt.Errorf("invalid type, expected %T and received %T", mirror, m.k)
		return
	}

	go m.mirrorListen(mirror)
	return
}

func (m *Mojura[T]) mirrorListen(mirror *kiroku.Mirror) {
	for range mirror.Channel() {
		if err := m.syncDatabase(); err != nil {
			m.out.Errorf("Error encountered during sync: %v", err)
		}
	}
}

func (m *Mojura[T]) buildHistory() (ok bool, err error) {
	var kmeta kiroku.Meta
	if kmeta, err = m.k.Meta(); err != nil {
		return
	}

	if kmeta.BlockCount > 0 {
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
	m.out.Notification("Found populated database with an empty history file, building history file from database entries")
	if err = m.importTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		if n, err = m.dumpHistory(txn); err != nil {
			err = fmt.Errorf("error encountered while dumping to history file: %v", err)
			return
		}

		return
	}); err != nil {
		return
	}

	m.out.Successf("Appended %d blocks to the history file", n)
	ok = true
	return
}

func (m *Mojura[T]) syncDatabase() (err error) {
	var filename string
	if filename, err = m.k.Filename(); err != nil {
		err = fmt.Errorf("error getting filename of history file: %v", err)
		return
	}

	return kiroku.Read(filename, m.onImport)
}

func (m *Mojura[T]) dumpHistory(txn *Transaction[T]) (n int64, err error) {
	var bkt backend.Bucket
	if bkt, err = txn.getEntriesBucket(); err != nil {
		err = fmt.Errorf("error getting entries bucket: %v", err)
		return
	}

	var lastIndex uint64
	if err = m.k.Transaction(func(txn *kiroku.Transaction) (err error) {
		cur := bkt.Cursor()
		for key, value := cur.First(); len(key) > 0; key, value = cur.Next() {
			if err = txn.AddBlock(kiroku.TypeWriteAction, key, value); err != nil {
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

func (m *Mojura[T]) onImport(r *kiroku.Reader) (err error) {
	return m.importTransaction(context.Background(), func(txn *Transaction[T]) (err error) {
		return m.importReader(txn, r)
	})
}

func (m *Mojura[T]) importReader(txn *Transaction[T], r *kiroku.Reader) (err error) {
	meta := r.Meta()
	seekTo := txn.meta.TotalBlockSize
	blockCount := meta.BlockCount

	switch {
	case meta == txn.meta.Meta:
		// No changes have occurred, return
		return

	case blockCount == 0:
		return

	case txn.meta.BlockCount == 0:
		// Produce system log for fresh database build
		m.out.Notificationf("Building fresh database from %d blocks", blockCount)

	case txn.meta.LastSnapshotAt < meta.LastSnapshotAt:
		// Produce system log for purge
		m.out.Notificationf("Snapshot encountered, purging existing entries and building fresh database from %d blocks", blockCount)

		// Snapshot occurred, purge DB and perform a full sync
		if err = m.purge(txn.txn); err != nil {
			return
		}

		seekTo = 0

	case blockCount == txn.meta.BlockCount:
		return

	default:
		blockCount -= txn.meta.BlockCount

		// Produce system log for database sync
		m.out.Notificationf("Syncing %d new blocks", blockCount)
	}

	var sw stopwatch.Stopwatch
	sw.Start()

	// Iterate through all entries from a given point within Reader
	if err = r.ForEach(seekTo, txn.processBlock); err != nil {
		err = fmt.Errorf("Mojura.importReader(): error during r.ForEach with metas of <%+v> (db) <%+v> (kiroku): %v", txn.meta.Meta, meta, err)
		return
	}

	m.out.Successf("Successfully processed %d blocks in %v", blockCount, sw.Stop())
	return
}

func (m *Mojura[T]) transaction(fn func(backend.Transaction, *kiroku.Transaction) (Transaction[T], error)) (err error) {
	err = m.db.Transaction(func(txn backend.Transaction) (err error) {
		var t Transaction[T]
		err = m.k.Transaction(func(ktxn *kiroku.Transaction) (err error) {
			t, err = fn(txn, ktxn)
			return
		})
		defer t.teardown()

		if err != nil {
			return
		}

		var meta kiroku.Meta
		if meta, err = m.k.Meta(); err != nil {
			return
		}

		return t.storeMeta(meta)
	})

	return
}

func (m *Mojura[T]) runTransaction(ctx context.Context, txn backend.Transaction, bw blockWriter, fn TransactionFn[T]) (t Transaction[T], err error) {
	t = newTransaction(ctx, m, txn, bw)
	if bw != nil {
		// We only need to load meta for write transactions
		if err = t.loadMeta(); err != nil {
			return
		}
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
		if err != nil {
			return
		}

		var meta kiroku.Meta
		if meta, err = m.k.Meta(); err != nil {
			return
		}

		return t.storeMeta(meta)
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
		return bkt.ForEach(ss.Write)
	}

	return m.k.Snapshot(writeFn)
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

// New will insert a new entry with the given value and the associated relationships
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

// Exists will notiy if an entry exists for a given entry ID
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
// Note: Will return ErrEntryNotFound if no match is found
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
// Note: Will return ErrEntryNotFound if no match is found
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

// Put will place an entry at a given entry ID
// Note: This will not check to see if the entry exists beforehand. If this functionality
// is needed, look into using the Edit method
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

// Delete will remove an entry and it's related relationship IDs
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

// Transaction will initialize a transaction
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

// ReadTransaction will initialize a read-only transaction
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

// Batch will initialize a batch
func (m *Mojura[T]) Batch(ctx context.Context, fn func(*Transaction[T]) error) (err error) {
	if m.opts.IsMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	return <-m.b.Append(ctx, fn)
}

// Snapshot will create a snapshot of the database in it's current state
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

// Reindex will reindex the relationships
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

// Close will close the selected instance of Mojura
func (m *Mojura[T]) Close() (err error) {
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.closed {
		return errors.ErrIsClosed
	}

	m.closed = true

	var errs errors.ErrorList
	errs.Push(m.db.Close())
	errs.Push(m.k.Close())
	return errs.Err()
}
