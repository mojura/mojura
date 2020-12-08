package mojura

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"reflect"
	"time"

	"github.com/gdbu/actions"
	"github.com/gdbu/atoms"
	"github.com/gdbu/indexer"

	"github.com/hatchify/errors"

	"github.com/mojura/backend"
)

const (
	// ErrNotInitialized is returned when a service has not been properly initialized
	ErrNotInitialized = errors.Error("service has not been properly initialized")
	// ErrRelationshipNotFound is returned when an relationship is not available for the given relationship key
	ErrRelationshipNotFound = errors.Error("relationship was not found")
	// ErrLookupNotFound is returned when a lookup is not available for the given lookup key
	ErrLookupNotFound = errors.Error("lookup was not found")
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
	// ErrInvalidLogKey is returned when an invalid log key is encountered
	ErrInvalidLogKey = errors.Error("invalid log key, expecting a single :: delimiter")
	// ErrEmptyFilters is returned when relationship pairs are empty for a filter or joined request
	ErrEmptyFilters = errors.Error("invalid relationship pairs, cannot be empty")
	// ErrInversePrimaryFilter is returned when the primary filter is set as an inverse comparison
	ErrInversePrimaryFilter = errors.Error("invalid primary filter, cannot be an inverse comparison")
	// ErrContextCancelled is returned when a transaction ends early from context
	ErrContextCancelled = errors.Error("context cancelled")
	// ErrTransactionTimedOut is returned when a transaction times out
	ErrTransactionTimedOut = errors.Error("transaction timed out")

	// Break is a non-error which will cause a ForEach loop to break early
	Break = errors.Error("break!")
)

var (
	entriesBktKey       = []byte("entries")
	relationshipsBktKey = []byte("relationships")
	lookupsBktKey       = []byte("lookups")
)

// New will return a new instance of Mojura
func New(name, dir string, example Value, relationships ...string) (cc *Mojura, err error) {
	return NewWithOpts(name, dir, example, defaultOpts, relationships...)
}

// NewWithOpts will return a new instance of Mojura
func NewWithOpts(name, dir string, example Value, opts Opts, relationships ...string) (mp *Mojura, err error) {
	var m Mojura
	if err = opts.Validate(); err != nil {
		return
	}

	if len(example.GetRelationships()) != len(relationships) {
		err = ErrInvalidNumberOfRelationships
		return
	}

	m.opts = &opts
	m.entryType = getMojuraType(example)
	m.logsDir = path.Join(dir, "logs")
	m.indexFmt = fmt.Sprintf("%s0%dd", "%", 8)

	if err = os.MkdirAll(m.logsDir, 0744); err != nil {
		return
	}

	if err = m.init(name, dir, relationships); err != nil {
		return
	}

	if m.a, err = actions.New(m.logsDir, name); err != nil {
		return
	}

	m.a.SetRotateFn(m.handleLogRotation)
	m.a.SetRotateInterval(time.Minute)
	// Set maximum number of lines to 10,000
	m.a.SetNumLines(100000)
	// Initialize new batcher
	m.b = newBatcher(&m)
	// Set return pointer
	mp = &m
	return
}

// Mojura is the DB manager
type Mojura struct {
	db  backend.Backend
	idx *indexer.Indexer
	a   *actions.Actions
	b   *batcher

	opts     *Opts
	logsDir  string
	indexFmt string

	// Element type
	entryType reflect.Type

	relationships [][]byte

	// Closed state
	closed atoms.Bool
}

func (m *Mojura) init(name, dir string, relationships []string) (err error) {
	indexFilename := path.Join(dir, name+".idb")
	filename := path.Join(dir, name+".bdb")

	if m.idx, err = indexer.New(indexFilename); err != nil {
		return fmt.Errorf("error opening index db for %s (%s): %v", name, dir, err)
	}

	if m.db, err = m.opts.Initializer.New(filename); err != nil {
		return fmt.Errorf("error opening db for %s (%s): %v", name, dir, err)
	}

	err = m.db.Transaction(func(txn backend.Transaction) (err error) {
		var entriesBkt backend.Bucket
		if entriesBkt, err = txn.GetOrCreateBucket(entriesBktKey); err != nil {
			return
		}

		if _, err = txn.GetOrCreateBucket(lookupsBktKey); err != nil {
			return
		}

		var relationshipsBkt backend.Bucket
		if relationshipsBkt, err = txn.GetOrCreateBucket(relationshipsBktKey); err != nil {
			return
		}

		for _, relationship := range relationships {
			rbs := []byte(relationship)
			if _, err = relationshipsBkt.GetOrCreateBucket(rbs); err != nil {
				return
			}

			m.relationships = append(m.relationships, rbs)
		}

		if err = m.setIndexer(entriesBkt); err != nil {
			err = fmt.Errorf("error reading last ID: %v", err)
			return
		}

		return
	})

	return
}

func (m *Mojura) newReflectValue() (value reflect.Value) {
	// Zero value of the entry type
	return reflect.New(m.entryType)
}

func (m *Mojura) setIndexer(entriesBkt backend.Bucket) (err error) {
	if m.idx.Get() != 0 {
		// Indexer has already been set, bail out
		return
	}

	// Since indexer has not been set it, set the index value from the current last entry

	lastID, _ := entriesBkt.Cursor().Last()

	var value uint64
	if value, err = parseIDAsIndex(lastID); err != nil {
		return
	}

	m.idx.Set(value)
	return m.idx.Flush()
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

func (m *Mojura) handleLogRotation(filename string) {
	var err error
	archiveDir := path.Join(m.logsDir, "archived")
	name := path.Base(filename)
	destination := path.Join(archiveDir, name)

	if err = os.MkdirAll(archiveDir, 0744); err != nil {
		// TODO: Add error logging here after we implement output interface to mojura
		log.Printf("error creating archive directory: %v\n", err)
		return
	}

	if err = os.Rename(filename, destination); err != nil {
		// TODO: Add error logging here after we implement output interface to mojura
		log.Printf("error renaming file: %v\n", err)
		return
	}

	return
}

func (m *Mojura) transaction(fn func(backend.Transaction, *actions.Transaction) error) (err error) {
	err = m.db.Transaction(func(txn backend.Transaction) (err error) {
		err = m.a.Transaction(func(atxn *actions.Transaction) (err error) {
			return fn(txn, atxn)
		})

		return
	})

	return
}

func (m *Mojura) runTransaction(ctx context.Context, txn backend.Transaction, atxn *actions.Transaction, fn TransactionFn) (err error) {
	t := newTransaction(ctx, m, txn, atxn)
	defer t.teardown()
	// Always ensure index has been flushed
	defer m.idx.Flush()
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

// New will insert a new entry with the given value and the associated relationships
func (m *Mojura) New(val Value) (entryID string, err error) {
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

// GetByRelationship will attempt to get all entries associated with a given relationship
func (m *Mojura) GetByRelationship(relationship, relationshipID string, entries interface{}) (err error) {
	var es reflect.Value
	if es, err = getReflectedSlice(m.entryType, entries); err != nil {
		return
	}

	err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.getByRelationship([]byte(relationship), []byte(relationshipID), es)
	})

	return
}

// GetFiltered will attempt to get the filtered entries
func (m *Mojura) GetFiltered(seekTo string, entries interface{}, limit int64, filters ...Filter) (err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.GetFiltered(seekTo, entries, limit, filters...)
	})

	return
}

// GetFirstByRelationship will attempt to get the first entry associated with a given relationship and relationship ID
func (m *Mojura) GetFirstByRelationship(relationship, relationshipID string, val Value) (err error) {
	if err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.getFirstByRelationship([]byte(relationship), []byte(relationshipID), val)
	}); err != nil {
		return
	}

	return
}

// GetLastByRelationship will attempt to get the last entry associated with a given relationship and relationship ID
func (m *Mojura) GetLastByRelationship(relationship, relationshipID string, val Value) (err error) {
	if err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.getLastByRelationship([]byte(relationship), []byte(relationshipID), val)
	}); err != nil {
		return
	}

	return
}

// ForEach will iterate through each of the entries
func (m *Mojura) ForEach(seekTo string, fn ForEachFn, filters ...Filter) (err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.ForEach(seekTo, fn, filters...)
	})

	return
}

// ForEachID will iterate through each of the entry IDs
func (m *Mojura) ForEachID(seekTo string, fn ForEachEntryIDFn, filters ...Filter) (err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.ForEachID(seekTo, fn, filters...)
	})

	return
}

// Cursor will return an iterating cursor
func (m *Mojura) Cursor(fn CursorFn) (err error) {
	if err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.cursor(fn)
	}); err == Break {
		err = nil
	}

	return
}

// CursorRelationship will return an iterating cursor for a given relationship and relationship ID
func (m *Mojura) CursorRelationship(relationship, relationshipID string, fn CursorFn) (err error) {
	if err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.CursorRelationship(relationship, relationshipID, fn)
	}); err == Break {
		err = nil
	}

	return
}

// Edit will attempt to edit an entry by ID
func (m *Mojura) Edit(entryID string, val Value) (err error) {
	err = m.Transaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.edit([]byte(entryID), val)
	})

	return
}

// Remove will remove a relationship ID and it's related relationship IDs
func (m *Mojura) Remove(entryID string) (err error) {
	err = m.Transaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.remove([]byte(entryID))
	})

	return
}

// SetLookup will set a lookup value
func (m *Mojura) SetLookup(lookup, lookupID, key string) (err error) {
	err = m.Transaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.setLookup([]byte(lookup), []byte(lookupID), []byte(key))
	})

	return
}

// GetLookup will retrieve the matching lookup keys
func (m *Mojura) GetLookup(lookup, lookupID string) (keys []string, err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		keys, err = txn.getLookupKeys([]byte(lookup), []byte(lookupID))
		return
	})

	return
}

// GetLookupKey will retrieve the first lookup key
func (m *Mojura) GetLookupKey(lookup, lookupID string) (key string, err error) {
	err = m.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		var keys []string
		if keys, err = txn.getLookupKeys([]byte(lookup), []byte(lookupID)); err != nil {
			return
		}

		if len(keys) == 0 {
			err = ErrEntryNotFound
			return
		}

		return
	})

	return
}

// RemoveLookup will set a lookup value
func (m *Mojura) RemoveLookup(lookup, lookupID, key string) (err error) {
	err = m.Transaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.removeLookup([]byte(lookup), []byte(lookupID), []byte(key))
	})

	return
}

// Transaction will initialize a transaction
func (m *Mojura) Transaction(ctx context.Context, fn func(*Transaction) error) (err error) {
	err = m.transaction(func(txn backend.Transaction, atxn *actions.Transaction) (err error) {
		return m.runTransaction(ctx, txn, atxn, fn)
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
	return <-m.b.Append(ctx, fn)
}

// Close will close the selected instance of Mojura
func (m *Mojura) Close() (err error) {
	if !m.closed.Set(true) {
		return errors.ErrIsClosed
	}

	var errs errors.ErrorList
	errs.Push(m.db.Close())
	errs.Push(m.a.Close())
	return errs.Err()
}
