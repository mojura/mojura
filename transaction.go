package mojura

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/gdbu/actions"
	"github.com/mojura/backend"
)

func newTransaction(ctx context.Context, m *Mojura, txn backend.Transaction, atxn *actions.Transaction) (t Transaction) {
	t.m = m
	t.cc = newContextContainer(ctx)
	t.txn = txn
	t.atxn = atxn
	return
}

// Transaction manages a DB transaction
type Transaction struct {
	m *Mojura

	cc *contextContainer

	txn  backend.Transaction
	atxn *actions.Transaction
}

func (t *Transaction) getRelationshipBucket(relationship []byte) (bkt backend.Bucket, err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var relationshipsBkt backend.Bucket
	if relationshipsBkt = t.txn.GetBucket(relationshipsBktKey); relationshipsBkt == nil {
		err = ErrNotInitialized
		return
	}

	if bkt = relationshipsBkt.GetBucket(relationship); bkt == nil {
		err = ErrRelationshipNotFound
		return
	}

	return
}

func (t *Transaction) getRelationshipIDBucket(relationship, relationshipID []byte) (bkt backend.Bucket, ok bool, err error) {
	var relationshipBkt backend.Bucket
	if relationshipBkt, err = t.getRelationshipBucket(relationship); err != nil {
		return
	}

	if bkt = relationshipBkt.GetBucket(relationshipID); bkt == nil {
		return
	}

	ok = true
	return
}

func (t *Transaction) getLookupBucket(lookup []byte) (bkt backend.Bucket, err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var lookupsBkt backend.Bucket
	if lookupsBkt = t.txn.GetBucket(lookupsBktKey); lookupsBkt == nil {
		err = ErrNotInitialized
		return
	}

	bkt = lookupsBkt.GetBucket(lookup)
	return
}

func (t *Transaction) get(entryID []byte, val interface{}) (err error) {
	var bs []byte
	if bs, err = t.getBytes(entryID); err != nil {
		return
	}

	err = t.m.unmarshal(bs, val)
	return
}

func (t *Transaction) getBytes(entryID []byte) (bs []byte, err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt = t.txn.GetBucket(entriesBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

	if bs = bkt.Get(entryID); len(bs) == 0 {
		err = ErrEntryNotFound
		return
	}

	return
}

func (t *Transaction) getIDsByRelationship(relationship, relationshipID []byte) (entryIDs [][]byte, err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var relationshipBkt backend.Bucket
	if relationshipBkt, err = t.getRelationshipBucket(relationship); err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt = relationshipBkt.GetBucket(relationshipID); bkt == nil {
		return
	}

	err = bkt.ForEach(func(entryID, _ []byte) (err error) {
		entryIDs = append(entryIDs, entryID)
		return
	})

	return
}

func (t *Transaction) getByRelationship(relationship, relationshipID []byte, entries reflect.Value) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var relationshipBkt backend.Bucket
	if relationshipBkt, err = t.getRelationshipBucket(relationship); err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt = relationshipBkt.GetBucket(relationshipID); bkt == nil {
		return
	}

	c := bkt.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		val := reflect.New(t.m.entryType)
		if err = t.get(k, val.Interface()); err != nil {
			return
		}

		entries.Set(reflect.Append(entries, val))
	}

	return
}

func (t *Transaction) getFirstID(reverse bool, filters []Filter) (entryID string, err error) {
	if err = t.forEachID(nil, reverse, func(id []byte) (err error) {
		entryID = string(id)
		return Break
	}, filters); err != nil {
		return
	}

	if entryID == "" {
		err = ErrEntryNotFound
		return
	}

	return
}

func (t *Transaction) getFiltered(seekTo []byte, reverse bool, entries reflect.Value, limit int64, filters []Filter) (err error) {
	if limit == 0 {
		return
	}

	var count int64
	err = t.forEachID(seekTo, reverse, func(entryID []byte) (err error) {
		val := t.m.newEntryValue()
		if err = t.get(entryID, &val); err != nil {
			return
		}

		entries.Set(reflect.Append(entries, reflect.ValueOf(val)))

		if count++; count == limit {
			return Break
		}

		return
	}, filters)

	return
}

func (t *Transaction) exists(entryID []byte) (ok bool, err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt = t.txn.GetBucket(entriesBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

	bs := bkt.Get(entryID)
	ok = len(bs) > 0
	return
}

func (t *Transaction) matchesAllPairs(fs []Filter, entryID []byte) (isMatch bool, err error) {
	eid := []byte(entryID)
	for _, pair := range fs {
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

func (t *Transaction) isPairMatch(pair *Filter, entryID []byte) (isMatch bool, err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var relationshipBkt backend.Bucket
	if relationshipBkt, err = t.getRelationshipBucket(pair.relationship()); err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt = relationshipBkt.GetBucket(pair.id()); bkt == nil {
		return
	}

	// Initialize new cursor
	c := bkt.Cursor()

	// Get the first key matching entryID (will get next key if entryID does not exist)
	firstKey, _ := c.Seek(entryID)

	// Check to see if the entry exists within the relationship
	isMatch = bytes.Compare(entryID, firstKey) == 0

	// Check for an inverse comparison
	if pair.InverseComparison {
		// Inverse comparison exists, invert the match
		isMatch = !isMatch
	}

	return
}

// ForEach will iterate through each of the entries
func (t *Transaction) newForEach(seekTo string, reverse bool, fn ForEachFn, filters []Filter) (err error) {
	// Wrap the provided func with an entry iterating func
	entryFn := fn.toEntryIteratingFn(t)

	// Check to see if any filters exist
	if len(filters) == 0 {
		// Fast path for raw ForEach (non-filtered) calls. This will utilize the value bytes seen during the cursor pass
		return t.forEach([]byte(seekTo), reverse, entryFn)
	}

	// Wrap the entry iterating func with an id iterating func
	idFn := entryFn.toIDIteratingFn(t)

	// Call forEachID
	return t.forEachID([]byte(seekTo), reverse, idFn, filters)
}

func (t *Transaction) forEach(seekTo []byte, reverse bool, fn entryIteratingFn) (err error) {
	var bkt backend.Bucket
	if bkt = t.txn.GetBucket(entriesBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

	return t.iterateBucket(bkt, seekTo, reverse, fn)
}

func (t *Transaction) forEachID(seekTo []byte, reverse bool, fn idIteratingFn, fs []Filter) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	if len(fs) == 0 {
		return t.forEach(seekTo, reverse, fn.toEntryIteratingFn())
	}

	var primary Filter
	if primary, fs, err = getPartedFilters(fs); err != nil {
		return
	}

	// Wrap iterator with a filtered iterator (if needed)
	fn = newIDIteratingFn(fn, t, fs)

	// Iterate through each relationship item
	err = t.forEachIDByRelationship(seekTo, primary.relationship(), primary.id(), reverse, fn)
	return
}

func (t *Transaction) forEachIDByRelationship(seekTo, relationship, relationshipID []byte, reverse bool, fn idIteratingFn) (err error) {
	var (
		bkt backend.Bucket
		ok  bool
	)

	if bkt, ok, err = t.getRelationshipIDBucket(relationship, relationshipID); !ok || err != nil {
		return
	}

	return t.iterateBucket(bkt, seekTo, reverse, fn.toEntryIteratingFn())
}

func (t *Transaction) iterateBucket(bkt backend.Bucket, seekTo []byte, reverse bool, fn entryIteratingFn) (err error) {
	// Check to see if context has expired
	if err = t.cc.isDone(); err != nil {
		return
	}

	// Initialize cursor for targeting bucket
	c := bkt.Cursor()

	// Set iterating function
	iteratingFunc := getIteratorFunc(c, reverse)

	// Iterate through the entries by:
	// - Set initial KV pair using getFirstPair
	// - Continuing while key is not nil
	// - Incrementing KV pairs using iteratingFunc
	for k, v := getFirstPair(c, seekTo, reverse); k != nil; k, v = iteratingFunc() {
		// Check to see if context has expired
		if err = t.cc.isDone(); err != nil {
			return
		}

		err = fn(k, v)

		switch {
		case err == nil:
		case err == Break:
			// Error is a break statement, set error to nil and return
			err = nil
			return

		default:
			// Error is not nil, nor is it break - return.
			return
		}
	}

	return
}

func (t *Transaction) cursor(fn CursorFn) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt = t.txn.GetBucket(entriesBktKey); bkt == nil {
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
	if err = t.cc.isDone(); err != nil {
		return
	}

	var relationshipBkt backend.Bucket
	if relationshipBkt, err = t.getRelationshipBucket(relationship); err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt = relationshipBkt.GetBucket(relationshipID); bkt == nil {
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
	if err = t.cc.isDone(); err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt = t.txn.GetBucket(entriesBktKey); bkt == nil {
		return ErrNotInitialized
	}

	val.SetUpdatedAt(time.Now().Unix())

	var bs []byte
	if bs, err = t.m.marshal(val); err != nil {
		return
	}

	return bkt.Put(entryID, bs)
}

func (t *Transaction) delete(entryID []byte) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt = t.txn.GetBucket(entriesBktKey); bkt == nil {
		return ErrNotInitialized
	}

	return bkt.Delete(entryID)
}

func (t *Transaction) setRelationships(relationships Relationships, entryID []byte) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	for i, relationship := range relationships {
		relationshipKey := t.m.relationships[i]
		for _, relationshipID := range relationship {
			if err = t.setRelationship(relationshipKey, []byte(relationshipID), entryID); err != nil {
				return
			}
		}
	}

	return
}

func (t *Transaction) setRelationship(relationship, relationshipID, entryID []byte) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	if len(relationshipID) == 0 {
		// Unset relationship IDs can be ignored
		return
	}

	var relationshipBkt backend.Bucket
	if relationshipBkt, err = t.getRelationshipBucket(relationship); err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt, err = relationshipBkt.GetOrCreateBucket(relationshipID); err != nil {
		return
	}

	return bkt.Put(entryID, nil)
}

func (t *Transaction) unsetRelationships(relationships Relationships, entryID []byte) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	for i, relationship := range relationships {
		relationshipKey := t.m.relationships[i]
		for _, relationshipID := range relationship {
			if err = t.unsetRelationship(relationshipKey, []byte(relationshipID), entryID); err != nil {
				return
			}
		}
	}

	return
}

func (t *Transaction) unsetRelationship(relationship, relationshipID, entryID []byte) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var relationshipBkt backend.Bucket
	if relationshipBkt, err = t.getRelationshipBucket(relationship); err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt = relationshipBkt.GetBucket(relationshipID); bkt == nil {
		return
	}

	return bkt.Delete(entryID)
}

func (t *Transaction) updateRelationships(entryID []byte, orig, val Value) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	origRelationships := orig.GetRelationships()
	newRelationships := val.GetRelationships()

	for i, relationship := range newRelationships {
		onAdd := func(relationshipID []byte) (err error) {
			return t.setRelationship(t.m.relationships[i], relationshipID, entryID)
		}

		onRemove := func(relationshipID []byte) (err error) {
			return t.unsetRelationship(t.m.relationships[i], relationshipID, entryID)
		}

		relationship.delta(origRelationships[i], onAdd, onRemove)
	}

	return
}

func (t *Transaction) getFirstByRelationship(relationship, relationshipID []byte, val Value) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var match bool
	if err = t.cursorRelationship(relationship, relationshipID, func(cursor Cursor) (err error) {
		if err = cursor.First(val); err == Break {
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
	if err = t.cc.isDone(); err != nil {
		return
	}

	var match bool
	if err = t.cursorRelationship(relationship, relationshipID, func(cursor Cursor) (err error) {
		if err = cursor.Last(val); err == Break {
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

// getLast will attempt to get the first entry which matches the provided filters
// Note: Will return ErrEntryNotFound if no match is found
func (t *Transaction) getFirst(value Value, filters []Filter) (err error) {
	var match string
	// Get the first ID match of the provided filters as a forward-direction look-up
	if match, err = t.getFirstID(false, filters); err != nil {
		// Error encountered while finding match, return
		return
	}

	// Retrieve entry for the matching entry ID
	return t.get([]byte(match), value)
}

// getLast will attempt to get the last entry which matches the provided filters
// Note: Will return ErrEntryNotFound if no match is found
func (t *Transaction) getLast(value Value, filters []Filter) (err error) {
	var match string
	// Get the first ID match of the provided filters as a reverse-direction look-up
	if match, err = t.getFirstID(true, filters); err != nil {
		// Error encountered while finding match, return
		return
	}

	// Retrieve entry for the matching entry ID
	return t.get([]byte(match), value)
}

func (t *Transaction) setLookup(lookup, lookupID, key []byte) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var lookupsBkt backend.Bucket
	if lookupsBkt = t.txn.GetBucket(lookupsBktKey); lookupsBkt == nil {
		err = ErrNotInitialized
		return
	}

	var lookupBkt backend.Bucket
	if lookupBkt, err = lookupsBkt.GetOrCreateBucket(lookup); err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt, err = lookupBkt.GetOrCreateBucket(lookupID); err != nil {
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
	if err = t.cc.isDone(); err != nil {
		return
	}

	var lookupBkt backend.Bucket
	if lookupBkt, err = t.getLookupBucket(lookup); lookupBkt == nil || err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt = lookupBkt.GetBucket(lookupID); bkt == nil {
		return
	}

	err = bkt.ForEach(func(key, _ []byte) (err error) {
		keys = append(keys, string(key))
		return
	})

	return
}

func (t *Transaction) removeLookup(lookup, lookupID, key []byte) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var lookupBkt backend.Bucket
	if lookupBkt, err = t.getLookupBucket(lookup); lookupBkt == nil || err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt = lookupBkt.GetBucket(lookupID); bkt == nil {
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
	if err = t.cc.isDone(); err != nil {
		return
	}

	var index uint64
	if index = t.m.idx.Next(); err != nil {
		return
	}

	// Create a padded entry ID from index value
	entryID = []byte(fmt.Sprintf(t.m.indexFmt, index))

	val.SetID(string(entryID))
	if val.GetCreatedAt() == 0 {
		val.SetCreatedAt(time.Now().Unix())
	}

	if err = t.put(entryID, val); err != nil {
		return
	}

	if err = t.setRelationships(val.GetRelationships(), entryID); err != nil {
		return
	}

	if err = t.atxn.LogJSON(actions.ActionCreate, getLogKey(entriesBktKey, entryID), val); err != nil {
		return
	}

	return
}

func (t *Transaction) edit(entryID []byte, val Value) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	orig := reflect.New(t.m.entryType).Interface().(Value)
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
	if err = t.cc.isDone(); err != nil {
		return
	}

	val := t.m.newEntryValue()
	if err = t.get(entryID, val); err != nil {
		return
	}

	if err = t.delete(entryID); err != nil {
		return
	}

	if err = t.unsetRelationships(val.GetRelationships(), entryID); err != nil {
		return
	}

	if err = t.atxn.LogJSON(actions.ActionDelete, getLogKey(entriesBktKey, entryID), nil); err != nil {
		return
	}

	return
}

func (t *Transaction) teardown() {
	t.txn = nil
	t.m = nil
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
	if es, err = getReflectedSlice(t.m.entryType, entries); err != nil {
		return
	}

	return t.getByRelationship([]byte(relationship), []byte(relationshipID), es)
}

// GetFiltered will attempt to get all entries associated with a set of given filters
func (t *Transaction) GetFiltered(seekTo string, entries interface{}, limit int64, filters ...Filter) (err error) {
	var es reflect.Value
	if es, err = getReflectedSlice(t.m.entryType, entries); err != nil {
		return
	}

	return t.getFiltered([]byte(seekTo), false, es, limit, filters)
}

// GetFirst will attempt to get the first entry associated with a set of given filters
// Note: Will return ErrEntryNotFound if no match is found
func (t *Transaction) GetFirst(value Value, filters ...Filter) (err error) {
	return t.getFirst(value, filters)
}

// GetLast will attempt to get the last entry associated with a set of given filters
// Note: Will return ErrEntryNotFound if no match is found
func (t *Transaction) GetLast(value Value, filters ...Filter) (err error) {
	return t.getLast(value, filters)

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
func (t *Transaction) ForEach(seekTo string, fn ForEachFn, filters ...Filter) (err error) {
	return t.newForEach(seekTo, false, fn, filters)
}

// ForEachReverse will iterate through each of the entries in reverse order
func (t *Transaction) ForEachReverse(seekTo string, fn ForEachFn, filters ...Filter) (err error) {
	return t.newForEach(seekTo, true, fn, filters)
}

// ForEachID will iterate through each of the entry IDs
func (t *Transaction) ForEachID(seekTo string, fn ForEachEntryIDFn, filters ...Filter) (err error) {
	return t.forEachID([]byte(seekTo), false, fn.toIDIteratingFn(), filters)
}

// ForEachIDReverse will iterate through each of the entry IDs in reverse order
func (t *Transaction) ForEachIDReverse(seekTo string, fn ForEachEntryIDFn, filters ...Filter) (err error) {
	return t.forEachID([]byte(seekTo), true, fn.toIDIteratingFn(), filters)
}

// ForEachRelationshipID will iterate through the IDs of a given relationship
func (t *Transaction) ForEachRelationshipID(seekTo, relationship string, reverse bool, fn ForEachRelationshipIDFn) (err error) {
	var relationshipBkt backend.Bucket
	if relationshipBkt, err = t.getRelationshipBucket([]byte(relationship)); err != nil {
		return
	}

	iterFn := func(relationshipID, _ []byte) (err error) {
		return fn(string(relationshipID))
	}

	err = t.iterateBucket(relationshipBkt, []byte(seekTo), reverse, iterFn)
	return
}

// ForEachRelationshipValue will iterate through the values for each ID of a given relationship
func (t *Transaction) ForEachRelationshipValue(seekTo, relationship string, reverse bool, fn ForEachRelationshipValueFn) (err error) {
	var relationshipBkt backend.Bucket
	if relationshipBkt, err = t.getRelationshipBucket([]byte(relationship)); err != nil {
		return
	}

	var currentRelationshipID string
	iterFn := func(entryID, _ []byte) (err error) {
		return fn(currentRelationshipID, string(entryID))
	}

	bktIterFn := func(relationshipID, _ []byte) (err error) {
		currentRelationshipID = string(relationshipID)
		return t.iterateBucket(relationshipBkt.GetBucket(relationshipID), nil, reverse, iterFn)
	}

	err = t.iterateBucket(relationshipBkt, []byte(seekTo), reverse, bktIterFn)
	return
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
