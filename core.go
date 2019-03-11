package core

import (
	"encoding/json"
	"path"
	"reflect"
	"time"

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
)

// New will return a new instance of Core
func New(name, dir string, example Value, relationships ...string) (cc *Core, err error) {
	var c Core
	c.elemType = getCoreType(example)
	if err = c.init(name, dir, relationships); err != nil {
		return
	}

	cc = &c
	return
}

// Core is the core manager
type Core struct {
	db  *bolt.DB
	dbu *dbutils.DBUtils

	// Element type
	elemType reflect.Type

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
		val := reflect.New(c.elemType)
		if err = c.get(txn, entryID, val.Interface()); err != nil {
			return
		}

		entries.Set(reflect.Append(entries, val))
		return
	})

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

func (c *Core) new(txn *bolt.Tx, val Value, relationshipIDs []string) (entryID []byte, err error) {
	if entryID, err = c.dbu.Next(txn, c.name); err != nil {
		return
	}

	val.SetID(string(entryID))
	val.SetCreatedAt(time.Now().Unix())
	if err = c.put(txn, entryID, val); err != nil {
		return
	}

	if err = c.setRelationships(txn, relationshipIDs, entryID); err != nil {
		return
	}

	return
}

func (c *Core) edit(txn *bolt.Tx, entryID []byte, fn func(interface{}) error) (err error) {
	val := reflect.New(c.elemType)
	if err = c.get(txn, entryID, val.Interface()); err != nil {
		return
	}

	if err = fn(val); err != nil {
		return
	}

	return c.put(txn, entryID, val.Interface().(Value))
}

func (c *Core) remove(txn *bolt.Tx, entryID []byte, relationshipIDs []string) (err error) {
	if err = c.delete(txn, entryID); err != nil {
		return
	}

	if err = c.unsetRelationships(txn, relationshipIDs, entryID); err != nil {
		return
	}

	return
}

// New will insert a new entry with the given value and the associated relationships
func (c *Core) New(val Value, relationshipIDs ...string) (entryID string, err error) {
	err = c.db.Update(func(txn *bolt.Tx) (err error) {
		var id []byte
		if id, err = c.new(txn, val, relationshipIDs); err != nil {
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
	es, ok := getReflectedSlice(entries)
	if !ok {
		return ErrInvalidEntries
	}

	if !isType(es, c.elemType) {
		return ErrInvalidType
	}

	err = c.db.View(func(txn *bolt.Tx) (err error) {
		return c.getByRelationship(txn, []byte(relationship), []byte(relationshipID), es)
	})

	return
}

// Edit will attempt to edit an entry by ID
func (c *Core) Edit(entryID string, fn func(interface{}) error) (err error) {
	err = c.db.Update(func(txn *bolt.Tx) (err error) {
		return c.edit(txn, []byte(entryID), fn)
	})

	return
}

// Remove will remove a relationship ID and it's related relationship IDs
func (c *Core) Remove(entryID string, relationshipIDs ...string) (err error) {
	err = c.db.View(func(txn *bolt.Tx) (err error) {
		return c.remove(txn, []byte(entryID), relationshipIDs)
	})

	return
}

// Close will close the selected instance of Core
func (c *Core) Close() (err error) {
	if !c.closed.Set(true) {
		return errors.ErrIsClosed
	}

	return c.db.Close()
}
