package mojura

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strconv"

	"github.com/mojura/backend"
)

func getReflectedSlice(t reflect.Type, v interface{}) (slice reflect.Value, err error) {
	ptr := reflect.ValueOf(v)
	if ptr.Kind() != reflect.Ptr {
		return
	}

	slice = ptr.Elem()
	if slice.Kind() != reflect.Slice {
		err = ErrInvalidEntries
		return
	}

	if !isType(slice, t) {
		err = ErrInvalidType
		return
	}

	return
}

func getMojuraType(v interface{}) (t reflect.Type) {
	if t = reflect.TypeOf(v); !isPointer(t) {
		return
	}

	return t.Elem()
}

func isPointer(t reflect.Type) (ok bool) {
	return t.Kind() == reflect.Ptr
}

func isType(v reflect.Value, t reflect.Type) (ok bool) {
	e := v.Type().Elem()
	if !isPointer(e) {
		return false
	}

	return e.Elem() == t
}

func isSliceMatch(a, b []string) (match bool) {
	if len(a) != len(b) {
		return
	}

	for i := range a {
		if a[i] != b[i] {
			return
		}
	}

	return true
}

func getLogKey(bucket, key []byte) (logKey []byte) {
	logKey = make([]byte, 0, len(bucket)+len(key)+2)
	logKey = append(logKey, bucket...)
	logKey = append(logKey, "::"...)
	logKey = append(logKey, key...)
	return
}

func parseLogKey(logKey []byte) (bucket, key []byte, err error) {
	spl := bytes.Split(logKey, []byte("::"))
	if len(spl) != 2 {
		err = ErrInvalidLogKey
		return
	}

	bucket = spl[0]
	key = spl[1]
	return
}

func recoverCall(txn *Transaction, fn TransactionFn) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic caught: %v", err)
		}
	}()

	return fn(txn)
}

func isDone(ctx context.Context) (done bool) {
	touch, ok := ctx.(*TouchContext)
	if ok {
		// We've encountered a touch Context, perform touch and return the inverse of it's state
		return !touch.Touch()
	}

	select {
	case <-ctx.Done():
		done = true
	default:
	}

	return
}

// ForEachFn is called during iteration
type ForEachFn func(key string, val Value) error

func (f ForEachFn) toEntryIteratingFn(txn *Transaction) entryIteratingFn {
	return func(entryID, entryValue []byte) (err error) {
		var val Value
		if val, err = txn.m.newValueFromBytes(entryValue); err != nil {
			return
		}

		return f(string(entryID), val)
	}
}

func (f ForEachEntryIDFn) toIDIteratingFn() idIteratingFn {
	return func(entryID []byte) (err error) {
		return f(string(entryID))
	}
}

// ForEachEntryIDFn is called during iteration
type ForEachEntryIDFn func(entryID string) error

// CursorFn is called during cursor iteration
type CursorFn func(*Cursor) error

type forEachEntryIDBytesFn func(entryID []byte) error

func toForEachEntryIDBytesFn(fn ForEachEntryIDFn) forEachEntryIDBytesFn {
	return func(entryID []byte) (err error) {
		return fn(string(entryID))
	}
}

func newEntryIteratingFn(fn entryIteratingFn, t *Transaction, fs []Filter) entryIteratingFn {
	if len(fs) == 0 {
		// No additional filters exist, no wrapping is necessary
		return fn
	}

	return func(entryID, entryValue []byte) (err error) {
		var isMatch bool
		if isMatch, err = t.matchesAllPairs(fs, entryID); !isMatch || err != nil {
			return
		}

		return fn(entryID, entryValue)
	}
}

type entryIteratingFn func(entryID, entryValue []byte) (err error)

func (e entryIteratingFn) toIDIteratingFn(txn *Transaction) idIteratingFn {
	return func(entryID []byte) (err error) {
		var entryValue []byte
		if entryValue, err = txn.getBytes(entryID); err != nil {
			return
		}

		return e(entryID, entryValue)
	}
}

func newIDIteratingFn(fn idIteratingFn, t *Transaction, fs []Filter) idIteratingFn {
	if len(fs) == 0 {
		// No additional filters exist, no wrapping is necessary
		return fn
	}

	return func(entryID []byte) (err error) {
		var isMatch bool
		if isMatch, err = t.matchesAllPairs(fs, entryID); !isMatch || err != nil {
			return
		}

		return fn(entryID)
	}
}

type idIteratingFn func(entryID []byte) (err error)

func (i idIteratingFn) toEntryIteratingFn() entryIteratingFn {
	return func(entryID, _ []byte) (err error) {
		return i(entryID)
	}
}

func getPartedFilters(fs []Filter) (primary Filter, remaining []Filter, err error) {
	// Set primary as the first entry
	if primary = fs[0]; primary.InverseComparison {
		// Primary filter cannot be set as an inverse comparison, return error
		err = ErrInversePrimaryFilter
		return
	}

	// Set remaining values
	remaining = fs[1:]
	return
}

func stripLeadingZeros(bs []byte) (out []byte) {
	for i, b := range bs {
		if b != '0' {
			return bs[i:]
		}

	}

	return
}

func parseIDAsIndex(id []byte) (index uint64, err error) {
	var stripped []byte
	if stripped = stripLeadingZeros(id); len(stripped) == 0 {
		return
	}

	if index, err = strconv.ParseUint(string(stripped), 10, 64); err != nil {
		err = fmt.Errorf("error parsing ID \"%s\": %v", string(id), err)
		return
	}

	return
}

func getFirstPair(c backend.Cursor, seekTo []byte, reverse bool) (k, v []byte) {
	switch {
	case !reverse && len(seekTo) == 0:
		// Current request is a forward-direction cursor AND has not provided a seek value.
		// Seek to the first entry within the cursor set.
		return c.First()
	case reverse && len(seekTo) == 0:
		// Current request is a reverse-direction cursor AND has not provided a seek value.
		// Seek to the last entry within the cursor set.
		return c.Last()
	}

	// Seek to seekTo value, check to see if provided seekTo is a direct match
	if k, v = c.Seek(seekTo); bytes.Compare(k, seekTo) != 0 {
		// seekTo is not a direct match, return current K/V pair
		return
	}

	// seekTo is a direct match, we must move to the next sibling

	switch reverse {
	case false:
		// Forward-direction cursor, call Next
		return c.Next()
	case true:
		// Reverse-direction cursor, call Prev
		return c.Prev()
	}

	return
}

func getIteratorFunc(c backend.Cursor, reverse bool) (fn func() (k, v []byte)) {
	if !reverse {
		// Current request is a forward-direction cursor, return cursor.Next (incrementing)
		return c.Next
	}

	// Current request is a reverse-direction cursor, return cursor.Prev (decrementing)
	return c.Prev
}
