package dbl

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"time"

	"github.com/boltdb/bolt"
	"github.com/gdbu/actions"
)

func newTransaction(ctx context.Context, c *Core, txn *bolt.Tx, atxn *actions.Transaction) (t Transaction) {
	t.c = c
	t.ctx = ctx
	t.txn = txn
	t.atxn = atxn
	return
}

// Transaction manages a core transaction
type Transaction struct {
	c *Core

	ctx context.Context

	txn  *bolt.Tx
	atxn *actions.Transaction
}

func (t *Transaction) getRelationshipBucket(relationship []byte) (bkt *bolt.Bucket, err error) {
	if isDone(t.ctx) {
		err = t.ctx.Err()
		return
	}

	var relationshipsBkt *bolt.Bucket
	if relationshipsBkt = t.txn.Bucket(relationshipsBktKey); relationshipsBkt == nil {
		err = ErrNotInitialized
		return
	}

	if bkt = relationshipsBkt.Bucket(relationship); bkt == nil {
		err = ErrRelationshipNotFound
		return
	}

	return
}

func (t *Transaction) getRelationshipIDBucket(relationship, relationshipID []byte) (bkt *bolt.Bucket, ok bool, err error) {
	var relationshipBkt *bolt.Bucket
	if relationshipBkt, err = t.getRelationshipBucket(relationship); err != nil {
		return
	}

	if bkt = relationshipBkt.Bucket(relationshipID); bkt == nil {
		return
	}

	ok = true
	return
}

func (t *Transaction) getLookupBucket(lookup []byte) (bkt *bolt.Bucket, err error) {
	if isDone(t.ctx) {
		err = t.ctx.Err()
		return
	}

	var lookupsBkt *bolt.Bucket
	if lookupsBkt = t.txn.Bucket(lookupsBktKey); lookupsBkt == nil {
		err = ErrNotInitialized
		return
	}

	bkt = lookupsBkt.Bucket(lookup)
	return
}

func (t *Transaction) get(entryID []byte, val interface{}) (err error) {
	if isDone(t.ctx) {
		err = t.ctx.Err()
		return
	}

	var bkt *bolt.Bucket
	if bkt = t.txn.Bucket(entriesBktKey); bkt == nil {
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

func (t *Transaction) getIDsByRelationship(relationship, relationshipID []byte) (entryIDs [][]byte, err error) {
	if isDone(t.ctx) {
		err = t.ctx.Err()
		return
	}

	var relationshipBkt *bolt.Bucket
	if relationshipBkt, err = t.getRelationshipBucket(relationship); err != nil {
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

func (t *Transaction) getByRelationship(relationship, relationshipID []byte, entries reflect.Value) (err error) {
	if isDone(t.ctx) {
		err = t.ctx.Err()
		return
	}

	var relationshipBkt *bolt.Bucket
	if relationshipBkt, err = t.getRelationshipBucket(relationship); err != nil {
		return
	}

	var bkt *bolt.Bucket
	if bkt = relationshipBkt.Bucket(relationshipID); bkt == nil {
		return
	}

	c := bkt.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		val := reflect.New(t.c.entryType)
		if err = t.get(k, val.Interface()); err != nil {
			return
		}

		entries.Set(reflect.Append(entries, val))
	}

	return
}

func (t *Transaction) exists(entryID []byte) (ok bool, err error) {
	if isDone(t.ctx) {
		err = t.ctx.Err()
		return
	}

	var bkt *bolt.Bucket
	if bkt = t.txn.Bucket(entriesBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

	bs := bkt.Get(entryID)
	ok = len(bs) > 0
	return
}

func (t *Transaction) matchesAllPairs(rps []RelationshipPair, entryID []byte) (isMatch bool, err error) {
	eid := []byte(entryID)
	for _, pair := range rps {
		isMatch, err = t.isPairMatch(&pair, eid)
		switch {
		case err != nil:
			return
		case !isMatch:
			return
		}

	}

	// We've made it through all the pairs without failing, entry is a match
	return
}

func (t *Transaction) isPairMatch(pair *RelationshipPair, entryID []byte) (isMatch bool, err error) {
	if isDone(t.ctx) {
		err = t.ctx.Err()
		return
	}

	var relationshipBkt *bolt.Bucket
	if relationshipBkt, err = t.getRelationshipBucket(pair.relationship()); err != nil {
		return
	}

	var bkt *bolt.Bucket
	if bkt = relationshipBkt.Bucket(pair.id()); bkt == nil {
		return
	}

	bkt.Get(entryID)
	c := bkt.Cursor()
	firstKey, _ := c.Seek(entryID)
	isMatch = bytes.Compare(entryID, firstKey) == 0
	return
}

func (t *Transaction) forEach(fn ForEachFn) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	var bkt *bolt.Bucket
	if bkt = t.txn.Bucket(entriesBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

	if err = bkt.ForEach(func(key, bs []byte) (err error) {
		var val Value
		if val, err = t.c.newValueFromBytes(bs); err != nil {
			return
		}

		// Check to see if context has expired
		if isDone(t.ctx) {
			return t.ctx.Err()
		}

		errCh := make(chan error)
		go func() {
			errCh <- fn(string(key), val)
		}()

		select {
		case err = <-errCh:
		case <-t.ctx.Done():
			err = t.ctx.Err()
		}

		return
	}); err == Break {
		err = nil
	}

	return
}

func (t *Transaction) forEachRelationship(seekTo, relationship, relationshipID []byte, fn ForEachFn) (err error) {
	err = t.forEachRelationshipEntryID(seekTo, relationship, relationshipID, func(entryID []byte) (err error) {
		val := t.c.newEntryValue()
		if err = t.get(entryID, val); err != nil {
			return
		}

		if err = fn(string(entryID), val); err != nil {
			return
		}

		return
	})

	return
}

func (t *Transaction) forEachRelationshipEntryID(seekTo, relationship, relationshipID []byte, fn func(entryID []byte) error) (err error) {
	if isDone(t.ctx) {
		err = t.ctx.Err()
		return
	}

	var (
		bkt *bolt.Bucket
		ok  bool
	)

	if bkt, ok, err = t.getRelationshipIDBucket(relationship, relationshipID); !ok || err != nil {
		return
	}

	c := bkt.Cursor()
	for k, _ := c.Seek(seekTo); k != nil; k, _ = c.Next() {
		if err = fn(k); err != nil {
			return
		}
	}

	return
}

func (t *Transaction) forEachFilter(seekTo []byte, rps []RelationshipPair, fn ForEachFn) (err error) {
	if len(rps) == 0 {
		err = ErrEmptyRelationshipPairs
		return
	}

	// Set primary as the first entry
	primary := rps[0]
	// Set remaining values
	remaining := rps[1:]
	// Declare iterating function
	iteratingFn := func(entryID string, val Value) (err error) {
		var isMatch bool
		eid := []byte(entryID)
		if isMatch, err = t.matchesAllPairs(remaining, eid); !isMatch || err != nil {
			return
		}

		return fn(entryID, val)
	}

	// Iterate through each relationship item
	err = t.forEachRelationship(seekTo, primary.relationship(), primary.id(), iteratingFn)
	return
}

func (t *Transaction) cursor(fn CursorFn) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	var bkt *bolt.Bucket
	if bkt = t.txn.Bucket(entriesBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

	cur := newCursor(t, bkt.Cursor(), false)
	err = fn(&cur)
	cur.teardown()

	if err == Break {
		err = nil
	}

	return
}

func (t *Transaction) cursorRelationship(relationship, relationshipID []byte, fn CursorFn) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	var relationshipBkt *bolt.Bucket
	if relationshipBkt, err = t.getRelationshipBucket(relationship); err != nil {
		return
	}

	var bkt *bolt.Bucket
	if bkt = relationshipBkt.Bucket(relationshipID); bkt == nil {
		return
	}

	cur := newCursor(t, bkt.Cursor(), true)
	err = fn(&cur)
	cur.teardown()

	if err == Break {
		err = nil
	}

	return
}

func (t *Transaction) put(entryID []byte, val Value) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	var bkt *bolt.Bucket
	if bkt = t.txn.Bucket(entriesBktKey); bkt == nil {
		return ErrNotInitialized
	}

	val.SetUpdatedAt(time.Now().Unix())

	var bs []byte
	if bs, err = json.Marshal(val); err != nil {
		return
	}

	return bkt.Put(entryID, bs)
}

func (t *Transaction) delete(entryID []byte) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	var bkt *bolt.Bucket
	if bkt = t.txn.Bucket(entriesBktKey); bkt == nil {
		return ErrNotInitialized
	}

	return bkt.Delete(entryID)
}

func (t *Transaction) setRelationships(relationshipIDs []string, entryID []byte) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	for i, relationshipID := range relationshipIDs {
		if err = t.setRelationship(t.c.relationships[i], []byte(relationshipID), entryID); err != nil {
			return
		}
	}

	return
}

func (t *Transaction) setRelationship(relationship, relationshipID, entryID []byte) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	if len(relationshipID) == 0 {
		// Unset relationship IDs can be ignored
		return
	}

	var relationshipBkt *bolt.Bucket
	if relationshipBkt, err = t.getRelationshipBucket(relationship); err != nil {
		return
	}

	var bkt *bolt.Bucket
	if bkt, err = relationshipBkt.CreateBucketIfNotExists(relationshipID); err != nil {
		return
	}

	return bkt.Put(entryID, nil)
}

func (t *Transaction) unsetRelationships(relationshipIDs []string, entryID []byte) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	for i, relationshipID := range relationshipIDs {
		if err = t.unsetRelationship(t.c.relationships[i], []byte(relationshipID), entryID); err != nil {
			return
		}
	}

	return
}

func (t *Transaction) unsetRelationship(relationship, relationshipID, entryID []byte) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	var relationshipBkt *bolt.Bucket
	if relationshipBkt, err = t.getRelationshipBucket(relationship); err != nil {
		return
	}

	var bkt *bolt.Bucket
	if bkt = relationshipBkt.Bucket(relationshipID); bkt == nil {
		return
	}

	return bkt.Delete(entryID)
}

func (t *Transaction) updateRelationships(entryID []byte, orig, val Value) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	origRelationships := orig.GetRelationshipIDs()
	newRelationships := val.GetRelationshipIDs()
	if isSliceMatch(origRelationships, newRelationships) {
		// Relationships already match, return
		return
	}

	if err = t.unsetRelationships(origRelationships, entryID); err != nil {
		return
	}

	if err = t.setRelationships(newRelationships, entryID); err != nil {
		return
	}

	return
}

func (t *Transaction) getFirstByRelationship(relationship, relationshipID []byte, val Value) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	var match bool
	if err = t.cursorRelationship(relationship, relationshipID, func(cur *Cursor) (err error) {
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

func (t *Transaction) getLastByRelationship(relationship, relationshipID []byte, val Value) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	var match bool
	if err = t.cursorRelationship(relationship, relationshipID, func(cur *Cursor) (err error) {
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

func (t *Transaction) setLookup(lookup, lookupID, key []byte) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	var lookupsBkt *bolt.Bucket
	if lookupsBkt = t.txn.Bucket(lookupsBktKey); lookupsBkt == nil {
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
	if err = t.atxn.LogJSON(actions.ActionCreate, logKey, key); err != nil {
		return
	}

	return
}

func (t *Transaction) getLookupKeys(lookup, lookupID []byte) (keys []string, err error) {
	if isDone(t.ctx) {
		err = t.ctx.Err()
		return
	}

	var lookupBkt *bolt.Bucket
	if lookupBkt, err = t.getLookupBucket(lookup); lookupBkt == nil || err != nil {
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

func (t *Transaction) removeLookup(lookup, lookupID, key []byte) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	var lookupBkt *bolt.Bucket
	if lookupBkt, err = t.getLookupBucket(lookup); lookupBkt == nil || err != nil {
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
	if err = t.atxn.LogJSON(actions.ActionDelete, logKey, key); err != nil {
		return
	}

	return
}

func (t *Transaction) new(val Value) (entryID []byte, err error) {
	if isDone(t.ctx) {
		err = t.ctx.Err()
		return
	}

	if entryID, err = t.c.dbu.Next(t.txn, entriesBktKey); err != nil {
		return
	}

	val.SetID(string(entryID))
	if val.GetCreatedAt() == 0 {
		val.SetCreatedAt(time.Now().Unix())
	}

	if err = t.put(entryID, val); err != nil {
		return
	}

	if err = t.setRelationships(val.GetRelationshipIDs(), entryID); err != nil {
		return
	}

	if err = t.atxn.LogJSON(actions.ActionCreate, getLogKey(entriesBktKey, entryID), val); err != nil {
		return
	}

	return
}

func (t *Transaction) edit(entryID []byte, val Value) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	orig := reflect.New(t.c.entryType).Interface().(Value)
	if err = t.get(entryID, orig); err != nil {
		return
	}

	// Ensure the ID is set as the original ID
	val.SetID(orig.GetID())
	// Ensure the created at timestamp is set as the original created at
	val.SetCreatedAt(orig.GetCreatedAt())

	if err = t.put(entryID, val); err != nil {
		return
	}

	// Update relationships (if needed)
	if err = t.updateRelationships(entryID, orig, val); err != nil {
		return
	}

	if err = t.atxn.LogJSON(actions.ActionEdit, getLogKey(entriesBktKey, entryID), val); err != nil {
		return
	}

	return
}

func (t *Transaction) remove(entryID []byte) (err error) {
	if isDone(t.ctx) {
		return t.ctx.Err()
	}

	val := t.c.newEntryValue()
	if err = t.get(entryID, val); err != nil {
		return
	}

	if err = t.delete(entryID); err != nil {
		return
	}

	if err = t.unsetRelationships(val.GetRelationshipIDs(), entryID); err != nil {
		return
	}

	if err = t.atxn.LogJSON(actions.ActionDelete, getLogKey(entriesBktKey, entryID), nil); err != nil {
		return
	}

	return
}

func (t *Transaction) teardown() {
	t.txn = nil
	t.c = nil
}

// New will insert a new entry with the given value and the associated relationships
func (t *Transaction) New(val Value) (entryID string, err error) {
	var id []byte
	if id, err = t.new(val); err != nil {
		return
	}

	entryID = string(id)
	return
}

// Exists will notiy if an entry exists for a given entry ID
func (t *Transaction) Exists(entryID string) (exists bool, err error) {
	return t.exists([]byte(entryID))
}

// Get will attempt to get an entry by ID
func (t *Transaction) Get(entryID string, val Value) (err error) {
	return t.get([]byte(entryID), val)
}

// GetByRelationship will attempt to get all entries associated with a given relationship
func (t *Transaction) GetByRelationship(relationship, relationshipID string, entries interface{}) (err error) {
	var es reflect.Value
	if es, err = getReflectedSlice(t.c.entryType, entries); err != nil {
		return
	}

	return t.getByRelationship([]byte(relationship), []byte(relationshipID), es)
}

// GetFirstByRelationship will attempt to get the first entry associated with a given relationship and relationship ID
func (t *Transaction) GetFirstByRelationship(relationship, relationshipID string, val Value) (err error) {
	return t.getFirstByRelationship([]byte(relationship), []byte(relationshipID), val)
}

// GetLastByRelationship will attempt to get the last entry associated with a given relationship and relationship ID
func (t *Transaction) GetLastByRelationship(relationship, relationshipID string, val Value) (err error) {
	return t.getLastByRelationship([]byte(relationship), []byte(relationshipID), val)
}

// ForEach will iterate through each of the entries
func (t *Transaction) ForEach(fn ForEachFn) (err error) {
	return t.forEach(fn)
}

// ForEachRelationship will iterate through each of the entries for a given relationship and relationship ID
func (t *Transaction) ForEachRelationship(seekTo, relationship, relationshipID string, fn ForEachFn) (err error) {
	return t.forEachRelationship([]byte(seekTo), []byte(relationship), []byte(relationshipID), fn)
}

// ForEachFilter will iterate through each of the entries who match all relationship pairs
func (t *Transaction) ForEachFilter(seekTo string, rps []RelationshipPair, fn ForEachFn) (err error) {
	return t.forEachFilter([]byte(seekTo), rps, fn)
}

// Cursor will return an iterating cursor
func (t *Transaction) Cursor(fn CursorFn) (err error) {
	if err = t.cursor(fn); err == Break {
		err = nil
	}

	return
}

// CursorRelationship will return an iterating cursor for a given relationship and relationship ID
func (t *Transaction) CursorRelationship(relationship, relationshipID string, fn CursorFn) (err error) {
	if err = t.cursorRelationship([]byte(relationship), []byte(relationshipID), fn); err == Break {
		err = nil
	}

	return
}

// Edit will attempt to edit an entry by ID
func (t *Transaction) Edit(entryID string, val Value) (err error) {
	return t.edit([]byte(entryID), val)
}

// Remove will remove a relationship ID and it's related relationship IDs
func (t *Transaction) Remove(entryID string) (err error) {
	return t.remove([]byte(entryID))
}

// SetLookup will set a lookup value
func (t *Transaction) SetLookup(lookup, lookupID, key string) (err error) {
	return t.setLookup([]byte(lookup), []byte(lookupID), []byte(key))
}

// GetLookup will retrieve the matching lookup keys
func (t *Transaction) GetLookup(lookup, lookupID string) (keys []string, err error) {
	keys, err = t.getLookupKeys([]byte(lookup), []byte(lookupID))
	return
}

// GetLookupKey will retrieve the first lookup key
func (t *Transaction) GetLookupKey(lookup, lookupID string) (key string, err error) {
	var keys []string
	if keys, err = t.getLookupKeys([]byte(lookup), []byte(lookupID)); err != nil {
		return
	}

	if len(keys) == 0 {
		err = ErrEntryNotFound
		return
	}

	return
}

// RemoveLookup will set a lookup value
func (t *Transaction) RemoveLookup(lookup, lookupID, key string) (err error) {
	return t.removeLookup([]byte(lookup), []byte(lookupID), []byte(key))
}

// TransactionFn represents a transaction function
type TransactionFn func(*Transaction) error
