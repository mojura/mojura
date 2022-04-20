package mojura

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mojura/backend"
	"github.com/mojura/kiroku"
)

func newTransaction[T Value](ctx context.Context, m *Mojura[T], txn backend.Transaction, bw blockWriter) (t Transaction[T]) {
	t.m = m
	t.cc = newContextContainer(ctx)
	t.txn = txn
	t.bw = bw
	return
}

// Transaction manages a DB transaction
type Transaction[T Value] struct {
	m *Mojura[T]

	cc *contextContainer

	txn backend.Transaction
	bw  blockWriter

	meta        metadata
	metaUpdated bool
}

func (t *Transaction[T]) getRelationshipBucket(relationship []byte) (bkt backend.Bucket, err error) {
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

func (t *Transaction[T]) getEntriesBucket() (bkt backend.Bucket, err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	if bkt = t.txn.GetBucket(entriesBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

	return
}

func (t *Transaction[T]) getMetaBucket() (bkt backend.Bucket, err error) {
	if bkt = t.txn.GetBucket(metaBktKey); bkt == nil {
		err = ErrNotInitialized
		return
	}

	return
}

func (t *Transaction[T]) get(entryID []byte) (val T, err error) {
	var bs []byte
	if bs, err = t.getBytes(entryID); err != nil {
		return
	}

	val = t.m.ifn()
	err = t.m.unmarshal(bs, val)
	return
}

func (t *Transaction[T]) getBytes(entryID []byte) (bs []byte, err error) {
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

func (t *Transaction[T]) idCursor(fs []Filter) (c IDCursor, err error) {
	if len(fs) == 0 {
		return newBaseIDCursor(t)
	}

	return newMultiIDCursor(t, fs)
}

func (t *Transaction[T]) cursor(fs []Filter) (c Cursor[T], err error) {
	if len(fs) == 0 {
		return newBaseCursor(t)
	}

	return newMultiCursor(t, fs)
}

func (t *Transaction[T]) exists(entryID []byte) (ok bool, err error) {
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

func (t *Transaction[T]) insertEntry(entryID []byte, val T) (err error) {
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

func (t *Transaction[T]) deleteEntry(entryID []byte) (err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var bkt backend.Bucket
	if bkt = t.txn.GetBucket(entriesBktKey); bkt == nil {
		return ErrNotInitialized
	}

	return bkt.Delete(entryID)
}

func (t *Transaction[T]) setRelationships(relationships Relationships, entryID []byte) (err error) {
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

func (t *Transaction[T]) setRelationship(relationship, relationshipID, entryID []byte) (err error) {
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

func (t *Transaction[T]) unsetRelationships(relationships Relationships, entryID []byte) (err error) {
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

func (t *Transaction[T]) unsetRelationship(relationship, relationshipID, entryID []byte) (err error) {
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

func (t *Transaction[T]) updateRelationships(entryID []byte, orig, new Relationships) (err error) {
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
func (t *Transaction[T]) getFirst(o *FilteringOpts) (value T, err error) {
	var cur IDCursor
	if cur, err = t.idCursor(o.Filters); err != nil {
		return
	}

	var entryID string
	if entryID, err = getFirstID(cur, o.LastID, false); err == Break {
		err = ErrEntryNotFound
		return
	} else if err != nil {
		return
	}

	return t.get([]byte(entryID))
}

// getLast will attempt to get the last entry which matches the provided filters
// Note: Will return ErrEntryNotFound if no match is found
func (t *Transaction[T]) getLast(o *FilteringOpts) (value T, err error) {
	var cur IDCursor
	if cur, err = t.idCursor(o.Filters); err != nil {
		return
	}

	var entryID string
	if entryID, err = getFirstID(cur, o.LastID, true); err == Break {
		err = ErrEntryNotFound
		return
	} else if err != nil {
		return
	}

	return t.get([]byte(entryID))
}

func (t *Transaction[T]) getFiltered(o *FilteringOpts) (es []T, lastID string, err error) {
	return t.appendFiltered(nil, o)
}

func (t *Transaction[T]) appendFiltered(in []T, o *FilteringOpts) (out []T, lastID string, err error) {
	if o == nil {
		o = defaultFilteringOpts
	}

	if o.Limit == 0 {
		return
	}

	var c Cursor[T]
	if c, err = t.cursor(o.Filters); err != nil {
		return
	}

	var count int64
	out = in
	err = t.forEachWithCursor(c, o, func(entryID string, val T) (err error) {
		out = append(out, val)
		if count++; count == o.Limit {
			lastID = joinSeekID(c.getCurrentRelationshipID(), entryID)
			return Break
		}

		return
	})

	return
}

func (t *Transaction[T]) forEachWithCursor(c Cursor[T], o *FilteringOpts, fn ForEachFn[T]) (err error) {
	var val T
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

func (t *Transaction[T]) new(val T) (created T, err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	index := t.meta.CurrentIndex
	t.setIndex(index + 1)

	// Create a padded entry ID from index value
	entryID := []byte(fmt.Sprintf(t.m.indexFmt, index))

	return t.put(entryID, val)
}

func (t *Transaction[T]) put(entryID []byte, val T) (updated T, err error) {
	if len(entryID) == 0 {
		err = ErrEmptyEntryID
		return
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

	updated = val
	return
}

func (t *Transaction[T]) processBlock(b *kiroku.Block) (err error) {
	switch b.Type {
	case kiroku.TypeWriteAction:
		var val T
		if val, err = t.m.newValueFromBytes(b.Value); err != nil {
			return
		}

		var idx uint64
		if idx, err = parseIDAsIndex(b.Key); err == nil && idx >= t.meta.CurrentIndex {
			t.setIndex(idx + 1)
		}

		_, err = t.editEntry(b.Key, val, true)
		return
	case kiroku.TypeDeleteAction:
		return t.deleteEntry(b.Key)
	}

	return
}

func (t *Transaction[T]) editEntry(entryID []byte, val T, allowInsert bool) (updated T, err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var orig T
	orig, err = t.get(entryID)
	switch {
	case err == nil:
		return t.update(entryID, orig, val)
	case err == ErrEntryNotFound && allowInsert:
		return t.update(entryID, nil, val)

	default:
		return
	}
}

func (t *Transaction[T]) update(entryID []byte, orig Value, val T) (updated T, err error) {
	// Ensure the ID is set as the original ID
	val.SetID(string(entryID))

	var relationships Relationships
	if orig != nil {
		// Original exists, ensure the created at timestamp is set as the original created at
		val.SetCreatedAt(orig.GetCreatedAt())
		relationships = orig.GetRelationships()
	}

	// Update relationships (if needed)
	if err = t.updateRelationships(entryID, relationships, val.GetRelationships()); err != nil {
		err = fmt.Errorf("error updating relationships: %v", err)
		return
	}

	if updated, err = t.put(entryID, val); err != nil {
		err = fmt.Errorf("error putting updated Entry into DB: %v", err)
		return
	}

	return
}

func (t *Transaction[T]) delete(entryID []byte) (deleted T, err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	var val T
	if val, err = t.get(entryID); err != nil {
		err = fmt.Errorf("error finding entry <%s>: %v", entryID, err)
		return
	}

	if err = t.deleteEntry(entryID); err != nil {
		err = fmt.Errorf("error removing entry <%s>: %v", entryID, err)
		return
	}

	if err = t.unsetRelationships(val.GetRelationships(), entryID); err != nil {
		err = fmt.Errorf("error unsetting relationships: %v", err)
		return
	}

	if err = t.bw.AddBlock(kiroku.TypeDeleteAction, entryID, nil); err != nil {
		return
	}

	deleted = val
	return
}

func (t *Transaction[T]) loadMeta() (err error) {
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

func (t *Transaction[T]) storeMeta(meta kiroku.Meta) (err error) {
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

func (t *Transaction[T]) setIndex(index uint64) {
	t.meta.CurrentIndex = index
	t.metaUpdated = true
}

func (t *Transaction[T]) teardown() {
	t.txn = nil
	t.m = nil
}

// New will insert a new entry with the given value and the associated relationships
func (t *Transaction[T]) New(val T) (created T, err error) {
	return t.new(val)
}

// Exists will notiy if an entry exists for a given entry ID
func (t *Transaction[T]) Exists(entryID string) (exists bool, err error) {
	return t.exists([]byte(entryID))
}

// Get will attempt to get an entry by ID
func (t *Transaction[T]) Get(entryID string) (val T, err error) {
	return t.get([]byte(entryID))
}

// GetFiltered will attempt to get all entries associated with a set of given filters
func (t *Transaction[T]) GetFiltered(o *FilteringOpts) (es []T, lastID string, err error) {
	return t.getFiltered(o)
}

// AppendFiltered will attempt to append all entries associated with a set of given filters
func (t *Transaction[T]) AppendFiltered(in []T, o *FilteringOpts) (out []T, lastID string, err error) {
	return t.getFiltered(o)
}

// GetFirst will attempt to get the first entry associated with a set of given filters
// Note: Will return ErrEntryNotFound if no match is found
func (t *Transaction[T]) GetFirst(o *FilteringOpts) (val T, err error) {
	return t.getFirst(o)
}

// GetLast will attempt to get the last entry associated with a set of given filters
// Note: Will return ErrEntryNotFound if no match is found
func (t *Transaction[T]) GetLast(o *FilteringOpts) (val T, err error) {
	return t.getLast(o)
}

// IDCursor will return an ID iterating cursor
func (t *Transaction[T]) IDCursor(fs ...Filter) (c IDCursor, err error) {
	return t.idCursor(fs)
}

// Cursor will return an iterating cursor
func (t *Transaction[T]) Cursor(fs ...Filter) (c Cursor[T], err error) {
	return t.cursor(fs)
}

// ForEach will iterate through entries
func (t *Transaction[T]) ForEach(fn ForEachFn[T], o *FilteringOpts) (err error) {
	if o == nil {
		o = defaultFilteringOpts
	}

	var c Cursor[T]
	if c, err = t.Cursor(o.Filters...); err != nil {
		return
	}

	iterator := getIteratorFunc(c, o.Reverse)

	var val T
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
func (t *Transaction[T]) ForEachID(fn ForEachIDFn, o *FilteringOpts) (err error) {
	if o == nil {
		o = defaultFilteringOpts
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
func (t *Transaction[T]) Put(entryID string, val T) (inserted T, err error) {
	return t.put([]byte(entryID), val)
}

// Edit will attempt to edit an entry by ID
func (t *Transaction[T]) Edit(entryID string, val T) (updated T, err error) {
	return t.editEntry([]byte(entryID), val, false)
}

// Delete will remove an entry and it's related relationship IDs
func (t *Transaction[T]) Delete(entryID string) (deleted T, err error) {
	return t.delete([]byte(entryID))
}

// TransactionFn represents a transaction function
type TransactionFn[T Value] func(*Transaction[T]) error
