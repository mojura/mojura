package dbl

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
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

func getCoreType(v interface{}) (t reflect.Type) {
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
	touch, ok := ctx.(*Context)
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

// ForEachFn are called during iteration
type ForEachFn func(key string, val Value) error

// CursorFn is called during cursor iteration
type CursorFn func(*Cursor) error
