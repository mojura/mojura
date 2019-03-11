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
func (t *Transaction) New(val Value, relationshipIDs ...string) (entryID string, err error) {
	var id []byte
	if id, err = t.c.new(t.txn, val, relationshipIDs); err != nil {
		return
	}

	entryID = string(id)
	return
}

// Get will attempt to get an entry by ID
func (t *Transaction) Get(entryID string, val Value) (err error) {
	return t.c.get(t.txn, []byte(entryID), val)
}

// GetByRelationship will attempt to get all entries associated with a given relationship
func (t *Transaction) GetByRelationship(relationship, relationshipID string, entries interface{}) (err error) {
	var es reflect.Value
	if es, err = getReflectedSlice(entries); err != nil {
		return
	}

	return t.c.getByRelationship(t.txn, []byte(relationship), []byte(relationshipID), es)
}

// Edit will attempt to edit an entry by ID
func (t *Transaction) Edit(entryID string, val Value) (err error) {
	return t.c.edit(t.txn, []byte(entryID), val)
}

// Remove will remove a relationship ID and it's related relationship IDs
func (t *Transaction) Remove(entryID string, relationshipIDs ...string) (err error) {
	return t.c.remove(t.txn, []byte(entryID), relationshipIDs)
}
