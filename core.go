package core

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"reflect"
	"time"

	"github.com/Hatch1fy/actions"
	"github.com/Hatch1fy/dbutils"
	"github.com/Hatch1fy/errors"

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
	c.b = newBatcher(&c, &opts)
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

func (c *Core) transaction(fn func(*bolt.Tx, *actions.Transaction) error) (err error) {
	err = c.db.Update(func(txn *bolt.Tx) (err error) {
		err = c.a.Transaction(func(atxn *actions.Transaction) (err error) {
			return fn(txn, atxn)
		})

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

func (c *Core) getRelationshipBucket(txn *bolt.Tx, relationship []byte) (bkt *bolt.Bucket, err error) {
	var relationshipsBkt *bolt.Bucket
	if relationshipsBkt = txn.Bucket(relationshipsBktKey); relationshipsBkt == nil {
		err = ErrNotInitialized
		return
	}

	if bkt = relationshipsBkt.Bucket(relationship); bkt == nil {
		err = ErrRelationshipNotFound
		return
	}

	return
}

func (c *Core) getLookupBucket(txn *bolt.Tx, lookup []byte) (bkt *bolt.Bucket, err error) {
	var lookupsBkt *bolt.Bucket
	if lookupsBkt = txn.Bucket(lookupsBktKey); lookupsBkt == nil {
		err = ErrNotInitialized
		return
	}

	bkt = lookupsBkt.Bucket(lookup)
	return
}

func (c *Core) get(txn *bolt.Tx, entryID []byte, val interface{}) (err error) {
	var bkt *bolt.Bucket
	if bkt = txn.Bucket(entriesBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

	var bs []byte
	if bs = bkt.Get(entryID); len(bs) == 0 {
		err = ErrEntryNotFound
		return
	}

	err = json.Unmarshal(bs, val)
	return
}

func (c *Core) getIDsByRelationship(txn *bolt.Tx, relationship, relationshipID []byte) (entryIDs [][]byte, err error) {
	var relationshipBkt *bolt.Bucket
	if relationshipBkt, err = c.getRelationshipBucket(txn, relationship); err != nil {
		return
	}

	var bkt *bolt.Bucket
	if bkt = relationshipBkt.Bucket(relationshipID); bkt == nil {
		return
	}

	err = bkt.ForEach(func(entryID, _ []byte) (err error) {
		entryIDs = append(entryIDs, entryID)
		return
	})

	return
}

func (c *Core) getByRelationship(txn *bolt.Tx, relationship, relationshipID []byte, entries reflect.Value) (err error) {
	var relationshipBkt *bolt.Bucket
	if relationshipBkt, err = c.getRelationshipBucket(txn, relationship); err != nil {
		return
	}

	var bkt *bolt.Bucket
	if bkt = relationshipBkt.Bucket(relationshipID); bkt == nil {
		return
	}

	err = bkt.ForEach(func(entryID, _ []byte) (err error) {
		val := reflect.New(c.entryType)
		if err = c.get(txn, entryID, val.Interface()); err != nil {
			return
		}

		entries.Set(reflect.Append(entries, val))
		return
	})

	return
}

func (c *Core) exists(txn *bolt.Tx, entryID []byte) (ok bool, err error) {
	var bkt *bolt.Bucket
	if bkt = txn.Bucket(entriesBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

	bs := bkt.Get(entryID)
	ok = len(bs) > 0
	return
}

func (c *Core) newValueFromBytes(bs []byte) (val Value, err error) {
	val = reflect.New(c.entryType).Interface().(Value)
	if err = json.Unmarshal(bs, val); err != nil {
		val = nil
		return
	}

	return
}

func (c *Core) forEach(txn *bolt.Tx, fn ForEachFn) (err error) {
	var bkt *bolt.Bucket
	if bkt = txn.Bucket(entriesBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

	if err = bkt.ForEach(func(key, bs []byte) (err error) {
		var val Value
		if val, err = c.newValueFromBytes(bs); err != nil {
			return
		}

		return fn(string(key), val)
	}); err == Break {
		err = nil
	}

	return
}

func (c *Core) forEachRelationship(txn *bolt.Tx, relationship, relationshipID []byte, fn ForEachFn) (err error) {
	var relationshipBkt *bolt.Bucket
	if relationshipBkt, err = c.getRelationshipBucket(txn, relationship); err != nil {
		return
	}

	var bkt *bolt.Bucket
	if bkt = relationshipBkt.Bucket(relationshipID); bkt == nil {
		return
	}

	if err = bkt.ForEach(func(entryID, _ []byte) (err error) {
		val := c.newEntryValue()
		if err = c.get(txn, entryID, val); err != nil {
			return
		}

		return fn(string(entryID), val)
	}); err == Break {
		err = nil
	}

	return
}

func (c *Core) cursor(txn *bolt.Tx, fn CursorFn) (err error) {
	var bkt *bolt.Bucket
	if bkt = txn.Bucket(entriesBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

	cur := newCursor(c, txn, bkt.Cursor(), false)
	err = fn(&cur)
	cur.teardown()

	if err == Break {
		err = nil
	}

	return
}

func (c *Core) cursorRelationship(txn *bolt.Tx, relationship, relationshipID []byte, fn CursorFn) (err error) {
	var relationshipBkt *bolt.Bucket
	if relationshipBkt, err = c.getRelationshipBucket(txn, relationship); err != nil {
		return
	}

	var bkt *bolt.Bucket
	if bkt = relationshipBkt.Bucket(relationshipID); bkt == nil {
		return
	}

	cur := newCursor(c, txn, bkt.Cursor(), true)
	err = fn(&cur)
	cur.teardown()

	if err == Break {
		err = nil
	}

	return
}

func (c *Core) put(txn *bolt.Tx, entryID []byte, val Value) (err error) {
	var bkt *bolt.Bucket
	if bkt = txn.Bucket(entriesBktKey); bkt == nil {
		return ErrNotInitialized
	}

	val.SetUpdatedAt(time.Now().Unix())

	var bs []byte
	if bs, err = json.Marshal(val); err != nil {
		return
	}

	return bkt.Put(entryID, bs)
}

func (c *Core) delete(txn *bolt.Tx, entryID []byte) (err error) {
	var bkt *bolt.Bucket
	if bkt = txn.Bucket(entriesBktKey); bkt == nil {
		return ErrNotInitialized
	}

	return bkt.Delete(entryID)
}

func (c *Core) setRelationships(txn *bolt.Tx, relationshipIDs []string, entryID []byte) (err error) {
	for i, relationshipID := range relationshipIDs {
		if err = c.setRelationship(txn, c.relationships[i], []byte(relationshipID), entryID); err != nil {
			return
		}
	}

	return
}

func (c *Core) setRelationship(txn *bolt.Tx, relationship, relationshipID, entryID []byte) (err error) {
	if len(relationshipID) == 0 {
		// Unset relationship IDs can be ignored
		return
	}

	var relationshipBkt *bolt.Bucket
	if relationshipBkt, err = c.getRelationshipBucket(txn, relationship); err != nil {
		return
	}

	var bkt *bolt.Bucket
	if bkt, err = relationshipBkt.CreateBucketIfNotExists(relationshipID); err != nil {
		return
	}

	return bkt.Put(entryID, nil)
}

func (c *Core) unsetRelationships(txn *bolt.Tx, relationshipIDs []string, entryID []byte) (err error) {
	for i, relationshipID := range relationshipIDs {
		if err = c.unsetRelationship(txn, c.relationships[i], []byte(relationshipID), entryID); err != nil {
			return
		}
	}

	return
}

func (c *Core) unsetRelationship(txn *bolt.Tx, relationship, relationshipID, entryID []byte) (err error) {
	var relationshipBkt *bolt.Bucket
	if relationshipBkt, err = c.getRelationshipBucket(txn, relationship); err != nil {
		return
	}

	var bkt *bolt.Bucket
	if bkt = relationshipBkt.Bucket(relationshipID); bkt == nil {
		return
	}

	return bkt.Delete(entryID)
}

func (c *Core) updateRelationships(txn *bolt.Tx, entryID []byte, orig, val Value) (err error) {
	origRelationships := orig.GetRelationshipIDs()
	newRelationships := val.GetRelationshipIDs()
	if isSliceMatch(origRelationships, newRelationships) {
		// Relationships already match, return
		return
	}

	if err = c.unsetRelationships(txn, origRelationships, entryID); err != nil {
		return
	}

	if err = c.setRelationships(txn, newRelationships, entryID); err != nil {
		return
	}

	return
}

func (c *Core) getFirstByRelationship(txn *bolt.Tx, relationship, relationshipID []byte, val Value) (err error) {
	var match bool
	if err = c.cursorRelationship(txn, relationship, relationshipID, func(cur *Cursor) (err error) {
		if err = cur.First(val); err == Break {
			err = ErrEntryNotFound
			return
		}

		match = true
		return
	}); err != nil {
		return
	}

	if !match {
		err = ErrEntryNotFound
	}

	return
}

func (c *Core) getLastByRelationship(txn *bolt.Tx, relationship, relationshipID []byte, val Value) (err error) {
	var match bool
	if err = c.cursorRelationship(txn, relationship, relationshipID, func(cur *Cursor) (err error) {
		if err = cur.Last(val); err == Break {
			err = ErrEntryNotFound
			return
		}

		match = true
		return
	}); err != nil {
		return
	}

	if !match {
		err = ErrEntryNotFound
	}

	return
}

func (c *Core) setLookup(txn *bolt.Tx, atxn *actions.Transaction, lookup, lookupID, key []byte) (err error) {
	var lookupsBkt *bolt.Bucket
	if lookupsBkt = txn.Bucket(lookupsBktKey); lookupsBkt == nil {
		err = ErrNotInitialized
		return
	}

	var lookupBkt *bolt.Bucket
	if lookupBkt, err = lookupsBkt.CreateBucketIfNotExists(lookup); err != nil {
		return
	}

	var bkt *bolt.Bucket
	if bkt, err = lookupBkt.CreateBucketIfNotExists(lookupID); err != nil {
		return
	}

	if err = bkt.Put(key, nil); err != nil {
		return
	}

	logKey := getLogKey(lookupsBktKey, lookupID)
	if err = atxn.LogJSON(actions.ActionCreate, logKey, key); err != nil {
		return
	}

	return
}

func (c *Core) getLookupKeys(txn *bolt.Tx, lookup, lookupID []byte) (keys []string, err error) {
	var lookupBkt *bolt.Bucket
	if lookupBkt, err = c.getLookupBucket(txn, lookup); lookupBkt == nil || err != nil {
		return
	}

	var bkt *bolt.Bucket
	if bkt = lookupBkt.Bucket(lookupID); bkt == nil {
		return
	}

	err = bkt.ForEach(func(key, _ []byte) (err error) {
		keys = append(keys, string(key))
		return
	})

	return
}

func (c *Core) removeLookup(txn *bolt.Tx, atxn *actions.Transaction, lookup, lookupID, key []byte) (err error) {
	var lookupBkt *bolt.Bucket
	if lookupBkt, err = c.getLookupBucket(txn, lookup); lookupBkt == nil || err != nil {
		return
	}

	var bkt *bolt.Bucket
	if bkt = lookupBkt.Bucket(lookupID); bkt == nil {
		return
	}

	if err = bkt.Delete(key); err != nil {
		return
	}

	logKey := getLogKey(lookupsBktKey, lookupID)
	if err = atxn.LogJSON(actions.ActionDelete, logKey, key); err != nil {
		return
	}

	return
}

func (c *Core) new(txn *bolt.Tx, atxn *actions.Transaction, val Value) (entryID []byte, err error) {
	if entryID, err = c.dbu.Next(txn, entriesBktKey); err != nil {
		return
	}

	val.SetID(string(entryID))
	if val.GetCreatedAt() == 0 {
		val.SetCreatedAt(time.Now().Unix())
	}

	if err = c.put(txn, entryID, val); err != nil {
		return
	}

	if err = c.setRelationships(txn, val.GetRelationshipIDs(), entryID); err != nil {
		return
	}

	if err = atxn.LogJSON(actions.ActionCreate, getLogKey(entriesBktKey, entryID), val); err != nil {
		return
	}

	return
}

func (c *Core) edit(txn *bolt.Tx, atxn *actions.Transaction, entryID []byte, val Value) (err error) {
	orig := reflect.New(c.entryType).Interface().(Value)
	if err = c.get(txn, entryID, orig); err != nil {
		return
	}

	// Ensure the ID is set as the original ID
	val.SetID(orig.GetID())
	// Ensure the created at timestamp is set as the original created at
	val.SetCreatedAt(orig.GetCreatedAt())

	if err = c.put(txn, entryID, val); err != nil {
		return
	}

	// Update relationships (if needed)
	if err = c.updateRelationships(txn, entryID, orig, val); err != nil {
		return
	}

	if err = atxn.LogJSON(actions.ActionEdit, getLogKey(entriesBktKey, entryID), val); err != nil {
		return
	}

	return
}

func (c *Core) remove(txn *bolt.Tx, atxn *actions.Transaction, entryID []byte) (err error) {
	val := c.newEntryValue()
	if err = c.get(txn, entryID, val); err != nil {
		return
	}

	if err = c.delete(txn, entryID); err != nil {
		return
	}

	if err = c.unsetRelationships(txn, val.GetRelationshipIDs(), entryID); err != nil {
		return
	}

	if err = atxn.LogJSON(actions.ActionDelete, getLogKey(entriesBktKey, entryID), nil); err != nil {
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

// New will insert a new entry with the given value and the associated relationships
func (c *Core) New(val Value) (entryID string, err error) {
	err = c.transaction(func(txn *bolt.Tx, atxn *actions.Transaction) (err error) {
		var id []byte
		if id, err = c.new(txn, atxn, val); err != nil {
			return
		}

		entryID = string(id)
		return
	})

	return
}

// Exists will notiy if an entry exists for a given entry ID
func (c *Core) Exists(entryID string) (exists bool, err error) {
	err = c.db.View(func(txn *bolt.Tx) (err error) {
		exists, err = c.exists(txn, []byte(entryID))
		return
	})

	return
}

// Get will attempt to get an entry by ID
func (c *Core) Get(entryID string, val Value) (err error) {
	err = c.db.View(func(txn *bolt.Tx) (err error) {
		return c.get(txn, []byte(entryID), val)
	})

	return
}

// GetByRelationship will attempt to get all entries associated with a given relationship
func (c *Core) GetByRelationship(relationship, relationshipID string, entries interface{}) (err error) {
	var es reflect.Value
	if es, err = getReflectedSlice(c.entryType, entries); err != nil {
		return
	}

	err = c.db.View(func(txn *bolt.Tx) (err error) {
		return c.getByRelationship(txn, []byte(relationship), []byte(relationshipID), es)
	})

	return
}

// GetFirstByRelationship will attempt to get the first entry associated with a given relationship and relationship ID
func (c *Core) GetFirstByRelationship(relationship, relationshipID string, val Value) (err error) {
	if err = c.db.View(func(txn *bolt.Tx) (err error) {
		return c.getFirstByRelationship(txn, []byte(relationship), []byte(relationshipID), val)
	}); err != nil {
		return
	}

	return
}

// GetLastByRelationship will attempt to get the last entry associated with a given relationship and relationship ID
func (c *Core) GetLastByRelationship(relationship, relationshipID string, val Value) (err error) {
	if err = c.db.View(func(txn *bolt.Tx) (err error) {
		return c.getLastByRelationship(txn, []byte(relationship), []byte(relationshipID), val)
	}); err != nil {
		return
	}

	return
}

// ForEach will iterate through each of the entries
func (c *Core) ForEach(fn ForEachFn) (err error) {
	err = c.db.View(func(txn *bolt.Tx) (err error) {
		return c.forEach(txn, fn)
	})

	return
}

// ForEachRelationship will iterate through each of the entries for a given relationship and relationship ID
func (c *Core) ForEachRelationship(relationship, relationshipID string, fn ForEachFn) (err error) {
	err = c.db.View(func(txn *bolt.Tx) (err error) {
		return c.forEachRelationship(txn, []byte(relationship), []byte(relationshipID), fn)
	})

	return
}

// Cursor will return an iterating cursor
func (c *Core) Cursor(fn CursorFn) (err error) {
	if err = c.db.View(func(txn *bolt.Tx) (err error) {
		return c.cursor(txn, fn)
	}); err == Break {
		err = nil
	}

	return
}

// CursorRelationship will return an iterating cursor for a given relationship and relationship ID
func (c *Core) CursorRelationship(relationship, relationshipID string, fn CursorFn) (err error) {
	if err = c.db.View(func(txn *bolt.Tx) (err error) {
		return c.cursorRelationship(txn, []byte(relationship), []byte(relationshipID), fn)
	}); err == Break {
		err = nil
	}

	return
}

// Edit will attempt to edit an entry by ID
func (c *Core) Edit(entryID string, val Value) (err error) {
	err = c.transaction(func(txn *bolt.Tx, atxn *actions.Transaction) (err error) {
		return c.edit(txn, atxn, []byte(entryID), val)
	})

	return
}

// Remove will remove a relationship ID and it's related relationship IDs
func (c *Core) Remove(entryID string) (err error) {
	err = c.transaction(func(txn *bolt.Tx, atxn *actions.Transaction) (err error) {
		return c.remove(txn, atxn, []byte(entryID))
	})

	return
}

// SetLookup will set a lookup value
func (c *Core) SetLookup(lookup, lookupID, key string) (err error) {
	err = c.transaction(func(txn *bolt.Tx, atxn *actions.Transaction) (err error) {
		return c.setLookup(txn, atxn, []byte(lookup), []byte(lookupID), []byte(key))
	})

	return
}

// GetLookup will retrieve the matching lookup keys
func (c *Core) GetLookup(lookup, lookupID string) (keys []string, err error) {
	err = c.db.View(func(txn *bolt.Tx) (err error) {
		keys, err = c.getLookupKeys(txn, []byte(lookup), []byte(lookupID))
		return
	})

	return
}

// GetLookupKey will retrieve the first lookup key
func (c *Core) GetLookupKey(lookup, lookupID string) (key string, err error) {
	err = c.db.View(func(txn *bolt.Tx) (err error) {
		var keys []string
		if keys, err = c.getLookupKeys(txn, []byte(lookup), []byte(lookupID)); err != nil {
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
	err = c.transaction(func(txn *bolt.Tx, atxn *actions.Transaction) (err error) {
		return c.removeLookup(txn, atxn, []byte(lookup), []byte(lookupID), []byte(key))
	})

	return
}

// Transaction will initialize a transaction
func (c *Core) Transaction(fn func(*Transaction) error) (err error) {
	err = c.transaction(func(txn *bolt.Tx, atxn *actions.Transaction) (err error) {
		t := newTransaction(c, txn, atxn)
		err = fn(&t)
		t.c = nil
		t.txn = nil
		return
	})

	return
}

// ReadTransaction will initialize a read-only transaction
func (c *Core) ReadTransaction(fn func(*Transaction) error) (err error) {
	err = c.db.View(func(txn *bolt.Tx) (err error) {
		t := newTransaction(c, txn, nil)
		err = fn(&t)
		t.c = nil
		t.txn = nil
		return
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
