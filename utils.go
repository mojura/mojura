package mojura

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
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

func getPartedFilters(fs []Filter) (primary Filter, remaining []Filter, err error) {
	// Set primary as the first entry
	primary = fs[0]
	if len(fs) == 1 {
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

func getFirstID(c IDCursor, seekTo string, reverse bool) (entryID string, err error) {
	switch {
	case len(seekTo) > 0 && !reverse:
		return c.Seek(seekTo)
	case len(seekTo) > 0 && reverse:
		return c.SeekReverse(seekTo)

	// Seek to does not exist
	case reverse:
		return c.First()
	case !reverse:
		return c.Last()
	}

	return
}

func getFirst(c Cursor, seekTo string, reverse bool) (v Value, err error) {
	switch {
	case len(seekTo) > 0 && !reverse:
		return c.Seek(seekTo)
	case len(seekTo) > 0 && reverse:
		return c.SeekReverse(seekTo)

	// Seek to does not exist
	case reverse:
		return c.First()
	case !reverse:
		return c.Last()
	}

	return
}

func splitSeekID(in string) (relationshipID, seekID string) {
	if len(in) == 0 {
		return
	}

	spl := strings.SplitN(in, "::", 2)
	relationshipID = spl[0]
	seekID = spl[1]
	return
}
