package mojura

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mojura/backend"
	"github.com/mojura/enkodo"
	"github.com/mojura/kiroku"
	"github.com/mojura/mojura/action"
	"github.com/mojura/mojura/filters"
)

func newTransaction[T Value](ctx context.Context, m *Mojura[T], txn backend.Transaction, bw action.BlockWriter) (t Transaction[T]) {
	t.m = m
	t.cc = newContextContainer(ctx)
	t.txn = txn
	t.bw = bw
	return
}

// Transaction provides operations within a Mojura transaction callback.
// It and its cursors must not escape the callback or be used concurrently.
type Transaction[T Value] struct {
	m *Mojura[T]

	cc *contextContainer

	txn backend.Transaction
	bw  action.BlockWriter

	meta        metadata
	metaUpdated bool
	layout      *relationshipLayout
}

func (t *Transaction[T]) getRelationshipBucket(relationship []byte) (bkt backend.Bucket, err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}

	if err = t.checkRelationshipLayout(); err != nil {
		return
	}

	var relationshipsBkt backend.Bucket
	if relationshipsBkt = t.txn.GetBucket(t.layout.root()); relationshipsBkt == nil {
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

	if err = t.m.unmarshal(bs, &val); err != nil {
		return
	}

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

	var bs []byte
	if bs, err = t.m.marshal(val); err != nil {
		return
	}

	if err = bkt.Put(entryID, bs); err != nil {
		return
	}

	aw := action.MakeWriter(t.bw)
	return aw.Write(entryID, bs)
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
		err = fmt.Errorf("error getting relationship bucket <%s>: %v", relationship, err)
		return
	}

	var bkt backend.Bucket
	if bkt, err = relationshipBkt.GetOrCreateBucket(relationshipID); err != nil {
		err = fmt.Errorf("error getting bucket for relationship ID <%s>: %v", relationshipID, err)
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

// getFirst finds the first match using non-nil options, or returns ErrEntryNotFound.
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
// The options must be non-nil. It returns ErrEntryNotFound if no match is found.
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

func (t *Transaction[T]) getFilteredIDs(o *FilteringOpts) (IDs []string, lastID string, err error) {
	return t.appendFilteredIDs(nil, o)
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

func (t *Transaction[T]) appendFilteredIDs(in []string, o *FilteringOpts) (out []string, lastID string, err error) {
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
	err = t.forEachWithCursor(c, o, func(entryID string, _ T) (err error) {
		out = append(out, entryID)
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

func (t *Transaction[T]) edit(entryID []byte, fn editFn[T], allowInsert bool) (updated T, err error) {
	if err = t.checkRelationshipLayout(); err != nil {
		return
	}

	if len(entryID) == 0 {
		err = ErrEmptyEntryID
		return
	}

	if err = t.cc.isDone(); err != nil {
		return
	}

	var (
		orig          T
		relationships Relationships
		originalKey   []byte
	)

	orig, err = t.get(entryID)
	switch {
	case err == nil:
		relationships = orig.GetRelationships()
		if originalKey, err = t.membershipKey(entryID, orig); err != nil {
			return
		}
	case err == ErrEntryNotFound && allowInsert:
	default:
		return
	}

	var modified T
	if modified, err = fn(orig); err != nil {
		return
	}

	setEssetialValues(entryID, modified)

	// Update relationships (if needed)
	key, err := t.membershipKey(entryID, modified)
	if err != nil {
		return updated, err
	}

	if originalKey != nil && !bytes.Equal(originalKey, key) {
		if err = t.unsetRelationships(relationships, originalKey); err != nil {
			return
		}

		relationships = nil
	}

	if err = t.updateRelationships(key, relationships, modified.GetRelationships()); err != nil {
		err = fmt.Errorf("error updating relationships: %v", err)
		return
	}

	if err = t.insertEntry(entryID, modified); err != nil {
		return
	}

	updated = modified
	return
}

func (t *Transaction[T]) update(entryID []byte, fn UpdateFn[T]) (updated T, err error) {
	return t.edit(entryID, func(in T) (out T, err error) {
		if err = fn(in); err != nil {
			return
		}

		out = in
		return
	}, false)
}

func (t *Transaction[T]) put(entryID []byte, val T) (updated T, err error) {
	return t.edit(entryID, func(_ T) (out T, err error) { return val, nil }, true)
}

func (t *Transaction[T]) delete(entryID []byte) (deleted T, err error) {
	if err = t.cc.isDone(); err != nil {
		return
	}
	if err = t.checkRelationshipLayout(); err != nil {
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

	key, err := t.membershipKey(entryID, val)
	if err != nil {
		return deleted, err
	}

	if err = t.unsetRelationships(val.GetRelationships(), key); err != nil {
		err = fmt.Errorf("error unsetting relationships: %v", err)
		return
	}

	aw := action.MakeWriter(t.bw)
	if err = aw.Delete(entryID); err != nil {
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

func (t *Transaction[T]) saveMeta() (err error) {
	if !t.metaUpdated {
		return
	}

	var bkt backend.Bucket
	if bkt, err = t.getMetaBucket(); err != nil {
		return
	}

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

func (t *Transaction[T]) processBlock(b kiroku.Block) (err error) {
	var a action.Action
	if err = enkodo.NewReader(bytes.NewReader(b)).Decode(&a); err != nil {
		return
	}

	switch a.Type {
	case action.TypeWrite:
		var val T
		if val, err = t.m.newValueFromBytes(a.Value); err != nil {
			err = fmt.Errorf("processBlock(): error getting new value from bytes: %v", err)
			return
		}

		var idx uint64
		if idx, err = parseIDAsIndex(a.Key); err == nil && idx >= t.meta.CurrentIndex {
			t.setIndex(idx + 1)
		}

		if _, err = t.put(a.Key, val); err != nil {
			err = fmt.Errorf("processBlock(): error putting entry <%s>: %v", string(a.Key), err)
			return
		}

		return
	case action.TypeDelete:
		if t.m.sortKeyWidth > 0 {
			var exists bool
			if exists, err = t.exists(a.Key); err != nil || !exists {
				return
			}

			_, err = t.delete(a.Key)
			return
		}

		if err = t.deleteEntry(a.Key); err != nil {
			err = fmt.Errorf("processBlock(): error deleting entry <%s>: %v", string(a.Key), err)
			return
		}

		return
	}

	return
}

func (t *Transaction[T]) teardown() {
	t.txn = nil
	t.m = nil
}

// New assigns the next generated ID and stores val and its relationship indexes.
func (t *Transaction[T]) New(val T) (created T, err error) {
	return t.new(val)
}

// Exists reports whether an entry is stored at entryID.
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

// GetFilteredIDs will attempt to get all IDs associated with a set of given filters
func (t *Transaction[T]) GetFilteredIDs(o *FilteringOpts) (ids []string, lastID string, err error) {
	return t.getFilteredIDs(o)
}

// AppendFiltered will attempt to append all entries associated with a set of given filters
func (t *Transaction[T]) AppendFiltered(in []T, o *FilteringOpts) (out []T, lastID string, err error) {
	return t.appendFiltered(in, o)
}

// AppendFilteredIDs appends matching entry IDs and returns a continuation token.
func (t *Transaction[T]) AppendFilteredIDs(in []string, o *FilteringOpts) (out []string, lastID string, err error) {
	return t.appendFilteredIDs(in, o)
}

// GetFirst will attempt to get the first entry associated with a set of given filters
// The options must be non-nil. It returns ErrEntryNotFound if no match is found.
func (t *Transaction[T]) GetFirst(o *FilteringOpts) (val T, err error) {
	return t.getFirst(o)
}

// GetLast will attempt to get the last entry associated with a set of given filters
// The options must be non-nil. It returns ErrEntryNotFound if no match is found.
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

// ForEachIntersectedID calls fn with each entry ID that every filter in fs
// holds, in ascending entry-ID order for unsorted values and (sort key, entry ID)
// order for SortedValue. Every filter must be a non-nil Match filter, and at
// least one is required: otherwise ErrIntersectionNeedsMatchFilters. The
// filters' relationship buckets are walked together on their keys: a cursor
// behind the highest current ID steps once, which reaches it where the sets
// interleave, and seeks to it otherwise. A walk costs about a seek per ID of
// its smallest set, and allocates only the IDs it yields. fn returning Break
// ends the walk with nil; another error from fn is returned as is, as is the
// transaction's own, such as its context's cancellation.
func (t *Transaction[T]) ForEachIntersectedID(fn ForEachIDFn, fs ...Filter) error {
	if t.m.sortKeyWidth > 0 {
		return t.forEachOrderedID(func(position OrderedID) error {
			return fn(position.EntryID)
		}, OrderedReadOptions{}, false, fs...)
	}

	if len(fs) == 0 {
		return ErrIntersectionNeedsMatchFilters
	}

	cursors := make([]filterCursor, len(fs))
	for i, f := range fs {
		match, ok := f.(*filters.MatchFilter)
		if !ok || match == nil {
			return ErrIntersectionNeedsMatchFilters
		}

		cursor, err := newMatchCursor(t, match)
		if err != nil {
			return err
		}

		cursors[i] = cursor
	}

	return walkIntersection(cursors, fn)
}

// Put inserts or replaces val at entryID and updates its relationship indexes.
// It mutates val's ID/timestamps and does not advance the generated-ID counter.
// Use Update when the entry must already exist.
func (t *Transaction[T]) Put(entryID string, val T) (inserted T, err error) {
	return t.put([]byte(entryID), val)
}

// Update modifies an existing entry and updates its relationship indexes.
func (t *Transaction[T]) Update(entryID string, fn func(T) error) (updated T, err error) {
	return t.update([]byte(entryID), fn)
}

// Delete removes an existing entry and its relationship memberships.
func (t *Transaction[T]) Delete(entryID string) (deleted T, err error) {
	return t.delete([]byte(entryID))
}

// TransactionFn represents a transaction function
type TransactionFn[T Value] func(*Transaction[T]) error

func (t *Transaction[T]) orderedBound(position *OrderedID) ([]byte, error) {
	if position == nil {
		return nil, nil
	}

	if len(position.SortKey) != t.m.sortKeyWidth {
		return nil, ErrInvalidSortKey
	}

	key := make([]byte, len(position.SortKey)+len(position.EntryID))
	copy(key, position.SortKey)
	copy(key[len(position.SortKey):], position.EntryID)
	return key, nil
}

// ForEachOrderedID intersects exact relationship sets on their persisted sort
// keys. It does not fetch records or sort matches in memory. Bounds survive
// deletion of the resume record. An unsorted value cannot use this API.
func (t *Transaction[T]) ForEachOrderedID(fn func(OrderedID) error, opts OrderedReadOptions, fs ...Filter) error {
	return t.forEachOrderedID(fn, opts, true, fs...)
}

// The ID-only adapter shares traversal but does not need an owned sort key.
func (t *Transaction[T]) forEachOrderedID(fn func(OrderedID) error, opts OrderedReadOptions, includeSortKey bool, fs ...Filter) error {
	if t.m.sortKeyWidth == 0 {
		return ErrInvalidSortKey
	}

	if err := t.checkRelationshipLayout(); err != nil {
		return err
	}

	if fn == nil {
		return errors.New("ordered read requires a visitor")
	}

	lower, err := t.orderedBound(opts.After)
	if err != nil {
		return err
	}

	upper, err := t.orderedBound(opts.Before)
	if err != nil {
		return err
	}

	if len(fs) == 0 {
		return ErrIntersectionNeedsMatchFilters
	}

	for _, f := range fs {
		if match, ok := f.(*filters.MatchFilter); !ok || match == nil {
			return ErrIntersectionNeedsMatchFilters
		}
	}

	cursors := make([]backend.Cursor, len(fs))
	keys := make([][]byte, len(fs))
	empty := false
	for i, f := range fs {
		match := f.(*filters.MatchFilter)
		var parent backend.Bucket
		parent, err = t.getRelationshipBucket([]byte(match.RelationshipKey))
		if err != nil {
			return err
		}

		bucket := parent.GetBucket([]byte(match.RelationshipID))
		if bucket == nil {
			empty = true
			continue
		}

		cursors[i] = bucket.Cursor()
		bound := lower
		if opts.Reverse {
			bound = upper
		}

		keys[i] = orderedStart(cursors[i], bound, opts.Reverse)
	}

	if empty {
		return t.cc.isDone()
	}

	yielded := 0
	for {
		if err = t.cc.isDone(); err != nil {
			return err
		}

		var target []byte
		for _, key := range keys {
			if key == nil {
				return nil
			}

			if len(key) <= t.m.sortKeyWidth {
				return ErrInvalidSortKey
			}

			if target == nil || orderedBehind(target, key, opts.Reverse) {
				target = key
			}
		}

		if (lower != nil && bytes.Compare(target, lower) <= 0) || (upper != nil && bytes.Compare(target, upper) >= 0) {
			return nil
		}

		aligned := true
		for i, key := range keys {
			if bytes.Equal(key, target) {
				continue
			}

			aligned = false
			keys[i] = orderedStepTo(cursors[i], target, opts.Reverse)
		}

		if !aligned {
			continue
		}

		position := OrderedID{EntryID: string(target[t.m.sortKeyWidth:])}
		if includeSortKey {
			position.SortKey = bytes.Clone(target[:t.m.sortKeyWidth])
		}

		if err = fn(position); err != nil {
			return endOfIntersection(err)
		}

		if err = t.cc.isDone(); err != nil {
			return err
		}

		yielded++
		if opts.Limit > 0 && yielded == opts.Limit {
			return nil
		}

		for i, cursor := range cursors {
			keys[i] = orderedNext(cursor, opts.Reverse)
		}
	}
}

func (t *Transaction[T]) checkRelationshipLayout() error {
	if t.layout == nil {
		layout, err := loadRelationshipLayout(t.txn)
		if err != nil {
			return err
		}

		if layout == nil {
			return ErrNotInitialized
		}

		t.layout = layout
	}

	if t.layout.Build != nil {
		return ErrRelationshipRebuildInProgress
	}

	if t.layout.Width != t.m.sortKeyWidth || (t.m.sortKeyWidth > 0 && !t.m.relationshipNamesMatch(t.layout.Keys)) {
		return ErrRelationshipRebuildRequired
	}

	return nil
}

func (t *Transaction[T]) membershipKey(id []byte, value T) ([]byte, error) {
	if t.m.sortKeyWidth == 0 {
		return id, nil
	}

	key := any(value).(SortedValue).GetSortKey()
	if len(key) != t.m.sortKeyWidth {
		return nil, ErrInvalidSortKey
	}

	out := make([]byte, len(key)+len(id))
	copy(out, key)
	copy(out[len(key):], id)
	return out, nil
}
