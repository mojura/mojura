package dbl

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"reflect"
	"time"

	"github.com/gdbu/actions"
	"github.com/gdbu/dbutils"
	"github.com/hatchify/errors"

	"github.com/hatchify/atoms"

	"github.com/boltdb/bolt"
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

var boltOpts = &bolt.Options{Timeout: 1 * time.Second}

// New will return a new instance of Core
func New(name, dir string, example Value, relationships ...string) (cc *Core, err error) {
	return NewWithOpts(name, dir, example, defaultOpts, relationships...)
}

// NewWithOpts will return a new instance of Core
func NewWithOpts(name, dir string, example Value, opts Opts, relationships ...string) (cc *Core, err error) {
	var c Core
	if len(example.GetRelationshipIDs()) != len(relationships) {
		err = ErrInvalidNumberOfRelationships
		return
	}

	c.opts = &opts
	c.entryType = getCoreType(example)
	c.logsDir = path.Join(dir, "logs")

	if err = os.MkdirAll(c.logsDir, 0744); err != nil {
		return
	}

	if err = c.init(name, dir, relationships); err != nil {
		return
	}

	if c.a, err = actions.New(c.logsDir, name); err != nil {
		return
	}

	c.a.SetRotateFn(c.handleLogRotation)
	c.a.SetRotateInterval(time.Minute)
	// Set maximum number of lines to 10,000
	c.a.SetNumLines(100000)
	// Initialize new batcher
	c.b = newBatcher(&c)
	// Set return pointer
	cc = &c
	return
}

// Core is the core manager
type Core struct {
	db  *bolt.DB
	dbu *dbutils.DBUtils
	a   *actions.Actions
	b   *batcher

	opts    *Opts
	logsDir string

	// Element type
	entryType reflect.Type

	relationships [][]byte

	// Closed state
	closed atoms.Bool
}

func (c *Core) init(name, dir string, relationships []string) (err error) {
	filename := path.Join(dir, name+".bdb")
	c.dbu = dbutils.New(8)

	if c.db, err = bolt.Open(filename, 0644, boltOpts); err != nil {
		return fmt.Errorf("error opening db for %s (%s): %v", name, dir, err)
	}

	err = c.db.Update(func(txn *bolt.Tx) (err error) {
		if err = c.dbu.Init(txn); err != nil {
			return
		}

		if _, err = txn.CreateBucketIfNotExists(entriesBktKey); err != nil {
			return
		}

		if _, err = txn.CreateBucketIfNotExists(lookupsBktKey); err != nil {
			return
		}

		var relationshipsBkt *bolt.Bucket
		if relationshipsBkt, err = txn.CreateBucketIfNotExists(relationshipsBktKey); err != nil {
			return
		}

		for _, relationship := range relationships {
			rbs := []byte(relationship)
			if _, err = relationshipsBkt.CreateBucketIfNotExists(rbs); err != nil {
				return
			}

			c.relationships = append(c.relationships, rbs)
		}

		return
	})

	return
}

func (c *Core) newEntryValue() (value Value) {
	// Zero value of the entry type
	zero := reflect.New(c.entryType)
	// Interface of zero value
	iface := zero.Interface()
	// Assert as a value (we've confirmed this type at initialization)
	return iface.(Value)
}

func (c *Core) newValueFromBytes(bs []byte) (val Value, err error) {
	val = reflect.New(c.entryType).Interface().(Value)
	if err = json.Unmarshal(bs, val); err != nil {
		val = nil
		return
	}

	return
}

func (c *Core) handleLogRotation(filename string) {
	var err error
	archiveDir := path.Join(c.logsDir, "archived")
	name := path.Base(filename)
	destination := path.Join(archiveDir, name)

	if err = os.MkdirAll(archiveDir, 0744); err != nil {
		// TODO: Add error logging here after we implement output interface to core
		log.Printf("error creating archive directory: %v\n", err)
		return
	}

	if err = os.Rename(filename, destination); err != nil {
		// TODO: Add error logging here after we implement output interface to core
		log.Printf("error renaming file: %v\n", err)
		return
	}

	return
}

func (c *Core) transaction(fn func(*bolt.Tx, *actions.Transaction) error) (err error) {
	err = c.db.Update(func(txn *bolt.Tx) (err error) {
		err = c.a.Transaction(func(atxn *actions.Transaction) (err error) {
			return fn(txn, atxn)
		})

		return
	})

	return
}

func (c *Core) runTransaction(ctx context.Context, txn *bolt.Tx, atxn *actions.Transaction, fn TransactionFn) (err error) {
	t := newTransaction(ctx, c, txn, atxn)
	defer t.teardown()
	errCh := make(chan error)

	// Call function from within goroutine
	go func() {
		// Pass returning error to error channel
		errCh <- fn(&t)
	}()

	select {
	case err = <-errCh:
	case <-t.ctx.Done():
		// Context is done, attempt to set error from Context
		if err = t.ctx.Err(); err != nil {
			return
		}

		err = ErrContextCancelled
	}

	return
}

// New will insert a new entry with the given value and the associated relationships
func (c *Core) New(val Value) (entryID string, err error) {
	err = c.Transaction(context.Background(), func(txn *Transaction) (err error) {
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
func (c *Core) Exists(entryID string) (exists bool, err error) {
	err = c.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		exists, err = txn.exists([]byte(entryID))
		return
	})

	return
}

// Get will attempt to get an entry by ID
func (c *Core) Get(entryID string, val Value) (err error) {
	err = c.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.get([]byte(entryID), val)
	})

	return
}

// GetByRelationship will attempt to get all entries associated with a given relationship
func (c *Core) GetByRelationship(relationship, relationshipID string, entries interface{}) (err error) {
	var es reflect.Value
	if es, err = getReflectedSlice(c.entryType, entries); err != nil {
		return
	}

	err = c.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.getByRelationship([]byte(relationship), []byte(relationshipID), es)
	})

	return
}

// GetFirstByRelationship will attempt to get the first entry associated with a given relationship and relationship ID
func (c *Core) GetFirstByRelationship(relationship, relationshipID string, val Value) (err error) {
	if err = c.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.getFirstByRelationship([]byte(relationship), []byte(relationshipID), val)
	}); err != nil {
		return
	}

	return
}

// GetLastByRelationship will attempt to get the last entry associated with a given relationship and relationship ID
func (c *Core) GetLastByRelationship(relationship, relationshipID string, val Value) (err error) {
	if err = c.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.getLastByRelationship([]byte(relationship), []byte(relationshipID), val)
	}); err != nil {
		return
	}

	return
}

// ForEach will iterate through each of the entries
func (c *Core) ForEach(seekTo string, fn ForEachFn, filters ...Filter) (err error) {
	err = c.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.ForEach(seekTo, fn, filters...)
	})

	return
}

// ForEachID will iterate through each of the entry IDs
func (c *Core) ForEachID(seekTo string, fn ForEachEntryIDFn, filters ...Filter) (err error) {
	err = c.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.ForEachID(seekTo, fn, filters...)
	})

	return
}

// Cursor will return an iterating cursor
func (c *Core) Cursor(fn CursorFn) (err error) {
	if err = c.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.cursor(fn)
	}); err == Break {
		err = nil
	}

	return
}

// CursorRelationship will return an iterating cursor for a given relationship and relationship ID
func (c *Core) CursorRelationship(relationship, relationshipID string, fn CursorFn) (err error) {
	if err = c.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.CursorRelationship(relationship, relationshipID, fn)
	}); err == Break {
		err = nil
	}

	return
}

// Edit will attempt to edit an entry by ID
func (c *Core) Edit(entryID string, val Value) (err error) {
	err = c.Transaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.edit([]byte(entryID), val)
	})

	return
}

// Remove will remove a relationship ID and it's related relationship IDs
func (c *Core) Remove(entryID string) (err error) {
	err = c.Transaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.remove([]byte(entryID))
	})

	return
}

// SetLookup will set a lookup value
func (c *Core) SetLookup(lookup, lookupID, key string) (err error) {
	err = c.Transaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.setLookup([]byte(lookup), []byte(lookupID), []byte(key))
	})

	return
}

// GetLookup will retrieve the matching lookup keys
func (c *Core) GetLookup(lookup, lookupID string) (keys []string, err error) {
	err = c.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
		keys, err = txn.getLookupKeys([]byte(lookup), []byte(lookupID))
		return
	})

	return
}

// GetLookupKey will retrieve the first lookup key
func (c *Core) GetLookupKey(lookup, lookupID string) (key string, err error) {
	err = c.ReadTransaction(context.Background(), func(txn *Transaction) (err error) {
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
func (c *Core) RemoveLookup(lookup, lookupID, key string) (err error) {
	err = c.Transaction(context.Background(), func(txn *Transaction) (err error) {
		return txn.removeLookup([]byte(lookup), []byte(lookupID), []byte(key))
	})

	return
}

// Transaction will initialize a transaction
func (c *Core) Transaction(ctx context.Context, fn func(*Transaction) error) (err error) {
	err = c.transaction(func(txn *bolt.Tx, atxn *actions.Transaction) (err error) {
		return c.runTransaction(ctx, txn, atxn, fn)
	})

	return
}

// ReadTransaction will initialize a read-only transaction
func (c *Core) ReadTransaction(ctx context.Context, fn func(*Transaction) error) (err error) {
	err = c.db.View(func(txn *bolt.Tx) (err error) {
		return c.runTransaction(ctx, txn, nil, fn)
	})

	return
}

// Batch will initialize a batch
func (c *Core) Batch(fn func(*Transaction) error) (err error) {
	return <-c.b.Append(fn)
}

// Close will close the selected instance of Core
func (c *Core) Close() (err error) {
	if !c.closed.Set(true) {
		return errors.ErrIsClosed
	}

	var errs errors.ErrorList
	errs.Push(c.db.Close())
	errs.Push(c.a.Close())
	return errs.Err()
}
