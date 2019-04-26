package core

import (
	"reflect"

	"github.com/boltdb/bolt"
)

func newTransaction(c *Core, txn *bolt.Tx) (t Transaction) {
	t.c = c
	t.txn = txn
	return
}

// Transaction manages a core transaction
type Transaction struct {
	c *Core

	txn *bolt.Tx
}

// New will insert a new entry with the given value and the associated relationships
func (t *Transaction) New(val Value) (entryID string, err error) {
	var id []byte
	if id, err = t.c.new(t.txn, val); err != nil {
		return
	}

	entryID = string(id)
	return
}

// Exists will notiy if an entry exists for a given entry ID
func (t *Transaction) Exists(entryID string) (exists bool, err error) {
	return t.c.exists(t.txn, []byte(entryID))
}

// Get will attempt to get an entry by ID
func (t *Transaction) Get(entryID string, val Value) (err error) {
	return t.c.get(t.txn, []byte(entryID), val)
}

// GetByRelationship will attempt to get all entries associated with a given relationship
func (t *Transaction) GetByRelationship(relationship, relationshipID string, entries interface{}) (err error) {
	var es reflect.Value
	if es, err = getReflectedSlice(t.c.entryType, entries); err != nil {
		return
	}

	return t.c.getByRelationship(t.txn, []byte(relationship), []byte(relationshipID), es)
}

// GetFirstByRelationship will attempt to get the first entry associated with a given relationship and relationship ID
func (t *Transaction) GetFirstByRelationship(relationship, relationshipID string, val Value) (err error) {
	return t.c.getFirstByRelationship(t.txn, []byte(relationship), []byte(relationshipID), val)
}

// GetLastByRelationship will attempt to get the last entry associated with a given relationship and relationship ID
func (t *Transaction) GetLastByRelationship(relationship, relationshipID string, val Value) (err error) {
	return t.c.getLastByRelationship(t.txn, []byte(relationship), []byte(relationshipID), val)
}

// ForEach will iterate through each of the entries
func (t *Transaction) ForEach(fn ForEachFn) (err error) {
	return t.c.forEach(t.txn, fn)
}

// ForEachRelationship will iterate through each of the entries for a given relationship and relationship ID
func (t *Transaction) ForEachRelationship(relationship, relationshipID string, fn ForEachFn) (err error) {
	return t.c.forEachRelationship(t.txn, []byte(relationship), []byte(relationshipID), fn)
}

// Cursor will return an iterating cursor
func (t *Transaction) Cursor(fn CursorFn) (err error) {
	if err = t.c.cursor(t.txn, fn); err == Break {
		err = nil
	}

	return
}

// CursorRelationship will return an iterating cursor for a given relationship and relationship ID
func (t *Transaction) CursorRelationship(relationship, relationshipID string, fn CursorFn) (err error) {
	if err = t.c.cursorRelationship(t.txn, []byte(relationship), []byte(relationshipID), fn); err == Break {
		err = nil
	}

	return
}

// Edit will attempt to edit an entry by ID
func (t *Transaction) Edit(entryID string, val Value) (err error) {
	return t.c.edit(t.txn, []byte(entryID), val)
}

// Remove will remove a relationship ID and it's related relationship IDs
func (t *Transaction) Remove(entryID string) (err error) {
	return t.c.remove(t.txn, []byte(entryID))
}

// SetLookup will set a lookup value
func (t *Transaction) SetLookup(lookup, lookupID, key string) (err error) {
	return t.c.setLookup(t.txn, []byte(lookup), []byte(lookupID), []byte(key))
}

// GetLookup will retrieve the matching lookup keys
func (t *Transaction) GetLookup(lookup, lookupID string) (keys []string, err error) {
	keys, err = t.c.getLookupKeys(t.txn, []byte(lookup), []byte(lookupID))
	return
}

// GetLookupKey will retrieve the first lookup key
func (t *Transaction) GetLookupKey(lookup, lookupID string) (key string, err error) {
	var keys []string
	if keys, err = t.c.getLookupKeys(t.txn, []byte(lookup), []byte(lookupID)); err != nil {
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
	return t.c.removeLookup(t.txn, []byte(lookup), []byte(lookupID), []byte(key))
}
