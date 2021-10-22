package mojura

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"reflect"

	"github.com/gdbu/atoms"
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
func New(opts Opts, example Value, relationships ...string) (mp *Mojura, err error) {
	var m Mojura
	if m, err = makeMojura(opts, example, relationships); err != nil {
		return
	}

	if err = m.primaryInitialization(); err != nil {
		return
	}

	// Initialize new batcher
	m.b = newBatcher(&m)

	mp = &m
	return
}

// NewMirror will return a new mirror instance of Mojura
func NewMirror(opts Opts, example Value, relationships ...string) (mp *Mojura, err error) {
	var m Mojura
	if m, err = makeMojura(opts, example, relationships); err != nil {
		return
	}

	if err = m.mirrorInitialization(); err != nil {
		return
	}

	mp = &m
	return
}

func makeMojura(opts Opts, example Value, relationships []string) (m Mojura, err error) {
	if err = opts.Validate(); err != nil {
		return
	}

	if len(example.GetRelationships()) != len(relationships) {
		err = ErrInvalidNumberOfRelationships
		return
	}

	m.out = scribe.New(fmt.Sprintf("Mojura (%s)", opts.Name))
	m.opts = &opts
	m.entryType = getMojuraType(example)
	m.indexFmt = fmt.Sprintf("%s0%dd", "%", opts.IndexLength)

	if err = m.init(relationships); err != nil {
		return
	}

	return
}

// Mojura is the DB manager
type Mojura struct {
	db  backend.Backend
	out *scribe.Scribe
	b   *batcher

	k kiroku.Ledger

	opts     *Opts
	indexFmt string

	// Element type
	entryType reflect.Type

	relationships [][]byte

	isMirror bool

	// Closed state
	closed atoms.Bool
}

func (m *Mojura) init(relationships []string) (err error) {
	filename := path.Join(m.opts.Dir, m.opts.Name+".bdb")
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

func (m *Mojura) initBuckets(txn backend.Transaction) (err error) {
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
}

func (m *Mojura) primaryInitialization() (err error) {
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

func (m *Mojura) mirrorInitialization() (err error) {
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

func (m *Mojura) mirrorListen(mirror *kiroku.Mirror) {
	for range mirror.Channel() {
		if err := m.syncDatabase(); err != nil {
			m.out.Errorf("Error encountered during sync: %v", err)
		}
	}
}

func (m *Mojura) buildHistory() (ok bool, err error) {
	var kmeta kiroku.Meta
	if kmeta, err = m.k.Meta(); err != nil {
		return
	}

	if kmeta.BlockCount > 0 {
		return
	}

	var hasEntries bool
	if err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
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
	if err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		var bkt backend.Bucket
		if bkt, err = txn.getEntriesBucket(); err != nil {
			err = fmt.Errorf("error getting entries bucket: %v", err)
			return
		}

		if n, err = m.dumpHistory(bkt); err != nil {
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

func (m *Mojura) syncDatabase() (err error) {
	var filename string
	if filename, err = m.k.Filename(); err != nil {
		err = fmt.Errorf("error getting filename of history file: %v", err)
		return
	}

	return kiroku.Read(filename, m.onImport)
}

func (m *Mojura) dumpHistory(bkt backend.Bucket) (n int64, err error) {
	var lastID []byte
	err = m.k.Transaction(func(txn *kiroku.Transaction) (err error) {
		cur := bkt.Cursor()
		for key, value := cur.First(); len(key) > 0; key, value = cur.Next() {
			if err = txn.AddBlock(kiroku.TypeWriteAction, key, value); err != nil {
				return
			}

			lastID = key
			n++
		}

		var lastIndex uint64
		if lastIndex, err = parseIDAsIndex(lastID); err != nil {
			return
		}

		if err = txn.SetIndex(lastIndex); err != nil {
			return
		}

		return
	})

	return
}

func (m *Mojura) purge(txn backend.Transaction) (err error) {
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

func (m *Mojura) newReflectValue() (value reflect.Value) {
	// Zero value of the entry type
	return reflect.New(m.entryType)
}

func (m *Mojura) newEntryValue() (value Value) {
	// Zero value of the entry type
	zero := m.newReflectValue()
	// Interface of zero value
	iface := zero.Interface()
	// Assert as a value (we've confirmed this type at initialization)
	return iface.(Value)
}

func (m *Mojura) marshal(val interface{}) (bs []byte, err error) {
	return m.opts.Encoder.Marshal(val)
}

func (m *Mojura) unmarshal(bs []byte, val interface{}) (err error) {
	return m.opts.Encoder.Unmarshal(bs, val)
}

func (m *Mojura) newValueFromBytes(bs []byte) (val Value, err error) {
	val = m.newEntryValue()
	if err = m.unmarshal(bs, val); err != nil {
		val = nil
		return
	}

	return
}

func (m *Mojura) getMeta(txn *Transaction) (meta metadata, ok bool, err error) {
	var bkt backend.Bucket
	if bkt = txn.txn.GetBucket(metaBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

	var bs []byte
	if bs = bkt.Get([]byte("value")); len(bs) == 0 {
		return
	}

	if err = json.Unmarshal(bs, &meta); err != nil {
		return
	}

	ok = true
	return
}

func (m *Mojura) setMeta(txn *Transaction, meta metadata) (err error) {
	var bkt backend.Bucket
	if bkt = txn.txn.GetBucket(metaBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

	var bs []byte
	if bs, err = json.Marshal(meta); err != nil {
		return
	}

	return bkt.Put([]byte("value"), bs)
}

func (m *Mojura) onImport(r *kiroku.Reader) (err error) {
	return m.importTransaction(context.Background(), func(txn *Transaction) (err error) {
		return m.importReader(txn, r)
	})
}

func (m *Mojura) importReader(txn *Transaction, r *kiroku.Reader) (err error) {
	var (
		current metadata
		ok      bool
	)

	if current, ok, err = m.getMeta(txn); err != nil {
		return
	}

	meta := r.Meta()
	seekTo := current.TotalBlockSize
	blockCount := meta.BlockCount

	switch {
	case meta == current.Meta:
		// No changes have occurred, return
		return

	case !ok && blockCount == 0:
		return

	case !ok:
		// Produce system log for fresh database build
		m.out.Notificationf("Building fresh database from %d blocks", blockCount)

	case current.LastSnapshotAt < meta.LastSnapshotAt:
		// Produce system log for purge
		m.out.Notificationf("Snapshot encountered, purging existing entries and building fresh database from %d blocks", blockCount)

		// Snapshot occurred, purge DB and perform a full sync
		if err = m.purge(txn.txn); err != nil {
			return
		}

		seekTo = 0

	default:
		blockCount -= current.BlockCount

		// Produce system log for database sync
		m.out.Notificationf("Syncing %d new blocks", blockCount)
	}

	var sw stopwatch.Stopwatch
	sw.Start()

	var md metadata
	// Iterate through all entries from a given point within Reader
	if err = r.ForEach(seekTo, txn.processBlock); err != nil {
		return
	}

	md.Meta = meta

	// Update meta to match the new meta state
	if err = m.setMeta(txn, md); err != nil {
		return
	}

	m.out.Successf("Successfully processed %d blocks in %v", blockCount, sw.Stop())
	return
}

func (m *Mojura) transaction(fn func(backend.Transaction, *kiroku.Transaction) error) (err error) {
	err = m.db.Transaction(func(txn backend.Transaction) (err error) {
		err = m.k.Transaction(func(ktxn *kiroku.Transaction) (err error) {
			return fn(txn, ktxn)
		})

		return
	})

	return
}

func (m *Mojura) runTransaction(ctx context.Context, txn backend.Transaction, bw blockWriter, fn TransactionFn) (err error) {
	t := newTransaction(ctx, m, txn, bw)
	defer t.teardown()
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

func (m *Mojura) importTransaction(ctx context.Context, fn func(*Transaction) error) (err error) {
	err = m.db.Transaction(func(txn backend.Transaction) (err error) {
		return m.runTransaction(ctx, txn, nopBW, fn)
	})

	return
}

func (m *Mojura) hasEntries(txn *Transaction) (ok bool, err error) {
	var bkt backend.Bucket
	if bkt, err = txn.getEntriesBucket(); err != nil {
		err = fmt.Errorf("error getting entries bucket: %v", err)
		return
	}

	ok = hasEntries(bkt)
	return
}

func (m *Mojura) copyEntries(txn *Transaction) (err error) {
	var bkt backend.Bucket
	if bkt, err = txn.getEntriesBucket(); err != nil {
		return
	}

	writeFn := func(ss *kiroku.Snapshot) (err error) {
		return bkt.ForEach(ss.Write)
	}

	return m.k.Snapshot(writeFn)
}

// New will insert a new entry with the given value and the associated relationships
func (m *Mojura) New(val Value) (entryID string, err error) {
	if m.isMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	err = m.Transaction(context.Background(), func(txn *Transaction) (err error) {
		var id []byte
		if id, err = txn.new(val); err != nil {
			return
		}

		entryID = string(id)
		return
	})

	return
}

// Exists will notiy if an entry exists for a given entry ID
func (m *Mojura) Exists(entryID string) (exists bool, err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		exists, err = txn.exists([]byte(entryID))
		return
	})

	return
}

// Get will attempt to get an entry by ID
func (m *Mojura) Get(entryID string, val Value) (err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.get([]byte(entryID), val)
	})

	return
}

// GetFiltered will attempt to get the filtered entries
func (m *Mojura) GetFiltered(entries interface{}, o *FilteringOpts) (lastID string, err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		lastID, err = txn.GetFiltered(entries, o)
		return
	})

	return
}

// GetFirst will attempt to get the first entry which matches the provided filters
// Note: Will return ErrEntryNotFound if no match is found
func (m *Mojura) GetFirst(val Value, o *IteratingOpts) (err error) {
	if err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.getFirst(val, o)
	}); err != nil {
		return
	}

	return
}

// GetLast will attempt to get the last entry which matches the provided filters
// Note: Will return ErrEntryNotFound if no match is found
func (m *Mojura) GetLast(val Value, o *IteratingOpts) (err error) {
	if err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.getLast(val, o)
	}); err != nil {
		return
	}

	return
}

// ForEach will iterate through each of the entries
func (m *Mojura) ForEach(fn ForEachFn, o *IteratingOpts) (err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.ForEach(fn, o)
	})

	return
}

// ForEachID will iterate through each of the entry IDs
func (m *Mojura) ForEachID(fn ForEachIDFn, o *IteratingOpts) (err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.ForEachID(fn, o)
	})

	return
}

// Cursor will return an iterating cursor
func (m *Mojura) Cursor(fn func(Cursor) error, fs ...Filter) (err error) {
	if err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		var c Cursor
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
func (m *Mojura) Put(entryID string, val Value) (err error) {
	if m.isMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	err = m.Transaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.put([]byte(entryID), val)
	})

	return
}

// Edit will attempt to edit an entry by ID
func (m *Mojura) Edit(entryID string, val Value) (err error) {
	if m.isMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	err = m.Transaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.edit([]byte(entryID), val, false)
	})

	return
}

// Remove will remove a relationship ID and it's related relationship IDs
func (m *Mojura) Remove(entryID string) (err error) {
	if m.isMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	err = m.Transaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.remove([]byte(entryID))
	})

	return
}

// Transaction will initialize a transaction
func (m *Mojura) Transaction(ctx context.Context, fn func(*Transaction) error) (err error) {
	if m.isMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	err = m.transaction(func(txn backend.Transaction, ktxn *kiroku.Transaction) (err error) {
		return m.runTransaction(ctx, txn, ktxn, fn)
	})

	return
}

// ReadTransaction will initialize a read-only transaction
func (m *Mojura) ReadTransaction(ctx context.Context, fn func(*Transaction) error) (err error) {
	err = m.db.ReadTransaction(func(txn backend.Transaction) (err error) {
		return m.runTransaction(ctx, txn, nil, fn)
	})

	return
}

// Batch will initialize a batch
func (m *Mojura) Batch(ctx context.Context, fn func(*Transaction) error) (err error) {
	if m.isMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	return <-m.b.Append(ctx, fn)
}

// Snapshot will create a snapshot of the database in it's current state
func (m *Mojura) Snapshot(ctx context.Context) (err error) {
	if m.isMirror {
		err = ErrMirrorCannotPerformWriteActions
		return
	}

	err = m.db.Transaction(func(btxn backend.Transaction) (err error) {
		txn := newTransaction(ctx, m, btxn, nil)
		return m.copyEntries(&txn)
	})

	return
}

// Close will close the selected instance of Mojura
func (m *Mojura) Close() (err error) {
	if !m.closed.Set(true) {
		return errors.ErrIsClosed
	}

	var errs errors.ErrorList
	errs.Push(m.db.Close())
	errs.Push(m.k.Close())
	return errs.Err()
}
