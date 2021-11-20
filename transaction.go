package mojura

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/mojura/backend"
	"github.com/mojura/kiroku"
)

func newTransaction(ctx context.Context, m *Mojura, txn backend.Transaction, bw blockWriter) (t Transaction) {
	t.m = m
	t.cc = newContextContainer(ctx)
	t.txn = txn
	t.bw = bw
	return
}

// Transaction manages a DB transaction
type Transaction struct {
	m *Mojura

	cc *contextContainer

	txn backend.Transaction
	bw  blockWriter

	meta        metadata
	metaUpdated bool
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

func (t *Transaction) getEntriesBucket() (bkt backend.Bucket, err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	if bkt = t.txn.GetBucket(entriesBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

	return
}

func (t *Transaction) getMetaBucket() (bkt backend.Bucket, err error) {
	if bkt = t.txn.GetBucket(metaBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

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
	var bkt backend.Bucket
	if bkt, err = t.getEntriesBucket(); err != nil {
		return
	}

	if bs = bkt.Get(entryID); len(bs) == 0 {
		err = ErrEntryNotFound
		return
	}

	return
}

func (t *Transaction) idCursor(fs []Filter) (c IDCursor, err error) {
	if len(fs) == 0 {
		return newBaseIDCursor(t)
	}

	return newMultiIDCursor(t, fs)
}

func (t *Transaction) cursor(fs []Filter) (c Cursor, err error) {
	if len(fs) == 0 {
		return newBaseCursor(t)
	}

	return newMultiCursor(t, fs)
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

func (t *Transaction) insertEntry(entryID []byte, val Value) (err error) {
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

	if err = bkt.Put(entryID, bs); err != nil {
		return
	}

	return t.bw.AddBlock(kiroku.TypeWriteAction, entryID, bs)
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
	// Get relationship key parent bucket
	if relationshipBkt, err = t.getRelationshipBucket(relationship); err != nil {
		return
	}

	var bkt backend.Bucket
	// Get bucket for the given relationship ID within the relationship key parent bucket
	if bkt = relationshipBkt.GetBucket(relationshipID); bkt == nil {
		return
	}

	// Delete entry in bucket by entry ID
	if err = bkt.Delete(entryID); err != nil {
		return
	}

	// Check to see if relationship ID bucket has any entries left
	if hasEntries(bkt) {
		// Bucket has entries, return
		return
	}

	// No more entries exist for this relationship, delete bucket
	return relationshipBkt.DeleteBucket(relationshipID)
}

func (t *Transaction) updateRelationships(entryID []byte, orig, new Relationships) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	for i, relationship := range new {
		onAdd := func(relationshipID []byte) (err error) {
			return t.setRelationship(t.m.relationships[i], relationshipID, entryID)
		}

		onRemove := func(relationshipID []byte) (err error) {
			return t.unsetRelationship(t.m.relationships[i], relationshipID, entryID)
		}

		var origR Relationship
		if orig != nil {
			origR = orig[i]
		}

		if err = relationship.delta(origR, onAdd, onRemove); err != nil {
			return
		}
	}

	return
}

// getLast will attempt to get the first entry which matches the provided filters
// Note: Will return ErrEntryNotFound if no match is found
func (t *Transaction) getFirst(value Value, o *IteratingOpts) (err error) {
	var cur IDCursor
	if cur, err = t.idCursor(o.Filters); err != nil {
		return
	}

	var entryID string
	if entryID, err = getFirstID(cur, o.LastID, false); err == Break {
		return ErrEntryNotFound
	} else if err != nil {
		return
	}

	return t.get([]byte(entryID), value)
}

// getLast will attempt to get the last entry which matches the provided filters
// Note: Will return ErrEntryNotFound if no match is found
func (t *Transaction) getLast(value Value, o *IteratingOpts) (err error) {
	var cur IDCursor
	if cur, err = t.idCursor(o.Filters); err != nil {
		return
	}

	var entryID string
	if entryID, err = getFirstID(cur, o.LastID, true); err == Break {
		return ErrEntryNotFound
	} else if err != nil {
		return
	}

	return t.get([]byte(entryID), value)
}

func (t *Transaction) getFiltered(es reflect.Value, o *FilteringOpts) (lastID string, err error) {
	if o == nil {
		o = defaultFilteringOpts
	}

	if o.Limit == 0 {
		return
	}

	var c Cursor
	if c, err = t.cursor(o.Filters); err != nil {
		return
	}

	var count int64
	err = t.forEachWithCursor(c, &o.IteratingOpts, func(entryID string, val Value) (err error) {
		rVal := reflect.ValueOf(val)
		appended := reflect.Append(es, rVal)
		es.Set(appended)

		if count++; count == o.Limit {
			lastID = joinSeekID(c.getCurrentRelationshipID(), entryID)
			return Break
		}

		return
	})

	return
}

func (t *Transaction) forEachWithCursor(c Cursor, o *IteratingOpts, fn ForEachFn) (err error) {
	var val Value
	iterator := getIteratorFunc(c, o.Reverse)
	val, err = getFirst(c, o.LastID, o.Reverse)
	for err == nil {
		if err = fn(val.GetID(), val); err != nil {
			break
		}

		val, err = iterator()
	}

	if err == Break {
		err = nil
	}

	return
}

func (t *Transaction) new(val Value) (entryID []byte, err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	index := t.meta.CurrentIndex
	t.setIndex(index + 1)

	// Create a padded entry ID from index value
	entryID = []byte(fmt.Sprintf(t.m.indexFmt, index))

	if err = t.put(entryID, val); err != nil {
		entryID = nil
		return
	}

	return
}

func (t *Transaction) put(entryID []byte, val Value) (err error) {
	if len(entryID) == 0 {
		return ErrEmptyEntryID
	}

	if err = t.cc.isDone(); err != nil {
		return
	}

	val.SetID(string(entryID))
	if val.GetCreatedAt() == 0 {
		val.SetCreatedAt(time.Now().Unix())
	}

	if err = t.insertEntry(entryID, val); err != nil {
		return
	}

	if err = t.setRelationships(val.GetRelationships(), entryID); err != nil {
		return
	}

	return
}

func (t *Transaction) processBlock(b *kiroku.Block) (err error) {
	switch b.Type {
	case kiroku.TypeWriteAction:
		var val Value
		if val, err = t.m.newValueFromBytes(b.Value); err != nil {
			return
		}

		idx, err := parseIDAsIndex(b.Key)
		if err == nil && idx >= t.meta.CurrentIndex {
			t.setIndex(idx + 1)
		}

		return t.edit(b.Key, val, true)
	case kiroku.TypeDeleteAction:
		return t.delete(b.Key)
	}

	return
}

func (t *Transaction) edit(entryID []byte, val Value, allowInsert bool) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	orig := reflect.New(t.m.entryType).Interface().(Value)
	err = t.get(entryID, orig)
	switch {
	case err == nil:
	case err == ErrEntryNotFound && allowInsert:
		orig = nil

	default:
		return
	}

	return t.update(entryID, orig, val)
}

func (t *Transaction) update(entryID []byte, orig, val Value) (err error) {
	// Ensure the ID is set as the original ID
	val.SetID(string(entryID))

	var relationships Relationships
	if orig != nil {
		// Original exists, ensure the created at timestamp is set as the original created at
		val.SetCreatedAt(orig.GetCreatedAt())
		relationships = orig.GetRelationships()
	}

	if err = t.put(entryID, val); err != nil {
		err = fmt.Errorf("error putting updated Entry into DB: %v", err)
		return
	}

	// Update relationships (if needed)
	if err = t.updateRelationships(entryID, relationships, val.GetRelationships()); err != nil {
		err = fmt.Errorf("error updating relationships: %v", err)
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
		err = fmt.Errorf("error finding entry <%s>: %v", entryID, err)
		return
	}

	if err = t.delete(entryID); err != nil {
		err = fmt.Errorf("error removing entry <%s>: %v", entryID, err)
		return
	}

	if err = t.unsetRelationships(val.GetRelationships(), entryID); err != nil {
		err = fmt.Errorf("error unsetting relationships: %v", err)
		return
	}

	return t.bw.AddBlock(kiroku.TypeDeleteAction, entryID, nil)
}

func (t *Transaction) loadMeta() (err error) {
	var bkt backend.Bucket
	if bkt, err = t.getMetaBucket(); err != nil {
		return
	}

	var bs []byte
	if bs = bkt.Get([]byte("value")); len(bs) == 0 {
		return
	}

	var meta metadata
	if err = json.Unmarshal(bs, &meta); err != nil {
		return
	}

	t.meta = meta
	return
}

func (t *Transaction) storeMeta(meta kiroku.Meta) (err error) {
	var bkt backend.Bucket
	if bkt, err = t.getMetaBucket(); err != nil {
		return
	}

	t.meta.Meta = meta

	var bs []byte
	if bs, err = json.Marshal(t.meta); err != nil {
		return
	}

	return bkt.Put([]byte("value"), bs)
}

func (t *Transaction) setIndex(index uint64) {
	t.meta.CurrentIndex = index
	t.metaUpdated = true
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

// GetFiltered will attempt to get all entries associated with a set of given filters
func (t *Transaction) GetFiltered(entries interface{}, o *FilteringOpts) (lastID string, err error) {
	var es reflect.Value
	if es, err = getReflectedSlice(t.m.entryType, entries); err != nil {
		return
	}

	return t.getFiltered(es, o)
}

// GetFirst will attempt to get the first entry associated with a set of given filters
// Note: Will return ErrEntryNotFound if no match is found
func (t *Transaction) GetFirst(value Value, o *IteratingOpts) (err error) {
	return t.getFirst(value, o)
}

// GetLast will attempt to get the last entry associated with a set of given filters
// Note: Will return ErrEntryNotFound if no match is found
func (t *Transaction) GetLast(value Value, o *IteratingOpts) (err error) {
	return t.getLast(value, o)
}

// IDCursor will return an ID iterating cursor
func (t *Transaction) IDCursor(fs ...Filter) (c IDCursor, err error) {
	return t.idCursor(fs)
}

// Cursor will return an iterating cursor
func (t *Transaction) Cursor(fs ...Filter) (c Cursor, err error) {
	return t.cursor(fs)
}

// ForEach will iterate through entries
func (t *Transaction) ForEach(fn ForEachFn, o *IteratingOpts) (err error) {
	if o == nil {
		o = defaultIteratingOpts
	}

	var c Cursor
	if c, err = t.Cursor(o.Filters...); err != nil {
		return
	}

	iterator := getIteratorFunc(c, o.Reverse)

	var val Value
	for val, err = getFirst(c, o.LastID, o.Reverse); err == nil; val, err = iterator() {
		if err = fn(val.GetID(), val); err != nil {
			break
		}
	}

	if err == Break {
		err = nil
	}

	return
}

// ForEachID will iterate through entry IDs
func (t *Transaction) ForEachID(fn ForEachIDFn, o *IteratingOpts) (err error) {
	if o == nil {
		o = defaultIteratingOpts
	}

	var c IDCursor
	if c, err = t.IDCursor(o.Filters...); err != nil {
		return
	}

	iterator := getIDIteratorFunc(c, o.Reverse)
	var entryID string
	for entryID, err = getFirstID(c, o.LastID, o.Reverse); err == nil; entryID, err = iterator() {
		if err = fn(entryID); err != nil {
			break
		}
	}

	if err == Break {
		err = nil
	}

	return
}

// Put will place an entry at a given entry ID
// Note: This will not check to see if the entry exists beforehand. If this functionality
// is needed, look into using the Edit method
func (t *Transaction) Put(entryID string, val Value) (err error) {
	return t.put([]byte(entryID), val)
}

// Edit will attempt to edit an entry by ID
func (t *Transaction) Edit(entryID string, val Value) (err error) {
	return t.edit([]byte(entryID), val, false)
}

// Remove will remove a relationship ID and it's related relationship IDs
func (t *Transaction) Remove(entryID string) (err error) {
	return t.remove([]byte(entryID))
}

// TransactionFn represents a transaction function
type TransactionFn func(*Transaction) error
