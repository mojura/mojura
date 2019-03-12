package core

import (
	"encoding/json"
	"os"
	"path"
	"reflect"
	"time"

	"github.com/Hatch1fy/actions"
	"github.com/Hatch1fy/errors"
	"github.com/PathDNA/atoms"
	"github.com/boltdb/bolt"
	"gitlab.com/itsMontoya/dbutils"
)

const (
	// ErrNotInitialized is returned when a service has not been properly initialized
	ErrNotInitialized = errors.Error("service has not been properly initialized")
	// ErrRelationshipNotFound is returned when an relationship is not available for the given relationship key
	ErrRelationshipNotFound = errors.Error("relationship was not found")
	// ErrEntryNotFound is returned when an entry is not available for the given ID
	ErrEntryNotFound = errors.Error("entry was not found")
	// ErrInvalidNumberOfRelationships is returned when an invalid number of relationships is provided in a New call
	ErrInvalidNumberOfRelationships = errors.Error("invalid number of relationships")
	// ErrInvalidType is returned when a type which does not match the generator is provided
	ErrInvalidType = errors.Error("invalid type encountered, please check generators")
	// ErrInvalidEntries is returned when a non-slice is presented to GetByRelationship
	ErrInvalidEntries = errors.Error("invalid entries, slice expected")

	// Break is a non-error which will cause a ForEach loop to break early
	Break = errors.Error("break!")
)

// New will return a new instance of Core
func New(name, dir string, example Value, relationships ...string) (cc *Core, err error) {
	var c Core
	if len(example.GetRelationshipIDs()) != len(relationships) {
		err = ErrInvalidNumberOfRelationships
		return
	}

	c.entryType = getCoreType(example)

	if err = c.init(name, dir, relationships); err != nil {
		return
	}

	logsDir := path.Join(dir, "logs")
	if err = os.MkdirAll(logsDir, 0744); err != nil {
		return
	}

	if c.a, err = actions.New(logsDir, name); err != nil {
		return
	}

	cc = &c
	return
}

// Core is the core manager
type Core struct {
	db  *bolt.DB
	dbu *dbutils.DBUtils
	a   *actions.Actions

	// Element type
	entryType reflect.Type

	name          []byte
	relationships [][]byte

	// Closed state
	closed atoms.Bool
}

func (c *Core) init(name, dir string, relationships []string) (err error) {
	c.name = []byte(name)
	filename := path.Join(dir, name+".bdb")
	c.dbu = dbutils.New(8)

	if c.db, err = bolt.Open(filename, 0644, nil); err != nil {
		return
	}

	err = c.db.Update(func(txn *bolt.Tx) (err error) {
		if err = c.dbu.Init(txn); err != nil {
			return
		}

		if _, err = txn.CreateBucketIfNotExists(c.name); err != nil {
			return
		}

		for _, relationship := range relationships {
			rbs := []byte(relationship)
			if _, err = txn.CreateBucketIfNotExists(rbs); err != nil {
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

func (c *Core) getRelationshipBucket(txn *bolt.Tx, relationship, relationshipID []byte) (bkt *bolt.Bucket, err error) {
	var relationshipBkt *bolt.Bucket
	if relationshipBkt = txn.Bucket(relationship); relationshipBkt == nil {
		err = ErrRelationshipNotFound
		return
	}

	bkt = relationshipBkt.Bucket(relationshipID)
	return
}

func (c *Core) get(txn *bolt.Tx, entryID []byte, val interface{}) (err error) {
	var bkt *bolt.Bucket
	if bkt = txn.Bucket(c.name); bkt == nil {
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
	var bkt *bolt.Bucket
	if bkt, err = c.getRelationshipBucket(txn, relationship, relationshipID); bkt == nil || err != nil {
		return
	}

	err = bkt.ForEach(func(entryID, _ []byte) (err error) {
		entryIDs = append(entryIDs, entryID)
		return
	})

	return
}

func (c *Core) getByRelationship(txn *bolt.Tx, relationship, relationshipID []byte, entries reflect.Value) (err error) {
	var bkt *bolt.Bucket
	if bkt, err = c.getRelationshipBucket(txn, relationship, relationshipID); bkt == nil || err != nil {
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

func (c *Core) forEach(txn *bolt.Tx, fn ForEachFn) (err error) {
	var bkt *bolt.Bucket
	if bkt = txn.Bucket(c.name); bkt == nil {
		err = ErrNotInitialized
		return
	}

	if err = bkt.ForEach(func(key, bs []byte) (err error) {
		val := reflect.New(c.entryType).Interface().(Value)
		if err = json.Unmarshal(bs, val); err != nil {
			return
		}

		return fn(string(key), val)
	}); err == Break {
		err = nil
	}

	return
}

func (c *Core) forEachRelationship(txn *bolt.Tx, relationship, relationshipID []byte, fn ForEachFn) (err error) {
	var bkt *bolt.Bucket
	if bkt, err = c.getRelationshipBucket(txn, relationship, relationshipID); bkt == nil || err != nil {
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

func (c *Core) put(txn *bolt.Tx, entryID []byte, val Value) (err error) {
	var bkt *bolt.Bucket
	if bkt = txn.Bucket(c.name); bkt == nil {
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
	if bkt = txn.Bucket(c.name); bkt == nil {
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
	if relationshipBkt = txn.Bucket(relationship); relationshipBkt == nil {
		return ErrRelationshipNotFound
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
	if relationshipBkt = txn.Bucket(relationship); relationshipBkt == nil {
		return ErrRelationshipNotFound
	}

	var bkt *bolt.Bucket
	if bkt = relationshipBkt.Bucket(relationshipID); bkt == nil {
		return
	}

	return bkt.Delete(entryID)
}

func (c *Core) new(txn *bolt.Tx, val Value) (entryID []byte, err error) {
	if entryID, err = c.dbu.Next(txn, c.name); err != nil {
		return
	}

	val.SetID(string(entryID))
	val.SetCreatedAt(time.Now().Unix())
	if err = c.put(txn, entryID, val); err != nil {
		return
	}

	if err = c.setRelationships(txn, val.GetRelationshipIDs(), entryID); err != nil {
		return
	}

	if err = c.a.LogJSON(actions.ActionCreate, []byte(entryID), val); err != nil {
		return
	}

	return
}

func (c *Core) edit(txn *bolt.Tx, entryID []byte, val Value) (err error) {
	original := reflect.New(c.entryType).Interface().(Value)
	if err = c.get(txn, entryID, original); err != nil {
		return
	}

	// Ensure the ID is set as the original ID
	val.SetID(original.GetID())
	// Ensure the created at timestamp is set as the original created at
	val.SetCreatedAt(original.GetCreatedAt())

	if err = c.put(txn, entryID, val); err != nil {
		return
	}

	if err = c.a.LogJSON(actions.ActionEdit, []byte(entryID), val); err != nil {
		return
	}

	return
}

func (c *Core) remove(txn *bolt.Tx, entryID []byte) (err error) {
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

	if err = c.a.LogJSON(actions.ActionDelete, []byte(entryID), nil); err != nil {
		return
	}

	return
}

// New will insert a new entry with the given value and the associated relationships
func (c *Core) New(val Value) (entryID string, err error) {
	err = c.db.Update(func(txn *bolt.Tx) (err error) {
		var id []byte
		if id, err = c.new(txn, val); err != nil {
			return
		}

		entryID = string(id)
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

// Edit will attempt to edit an entry by ID
func (c *Core) Edit(entryID string, val Value) (err error) {
	err = c.db.Update(func(txn *bolt.Tx) (err error) {
		return c.edit(txn, []byte(entryID), val)
	})

	return
}

// Remove will remove a relationship ID and it's related relationship IDs
func (c *Core) Remove(entryID string) (err error) {
	err = c.db.Update(func(txn *bolt.Tx) (err error) {
		return c.remove(txn, []byte(entryID))
	})

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

// Transaction will initialize a transaction
func (c *Core) Transaction(fn func(*Transaction) error) (err error) {
	err = c.db.Update(func(txn *bolt.Tx) (err error) {
		t := newTransaction(c, txn)
		err = fn(&t)
		t.c = nil
		t.txn = nil
		return
	})

	return
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
