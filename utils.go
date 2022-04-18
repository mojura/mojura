package mojura

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/mojura/backend"
	"github.com/mojura/kiroku"
)

var nopBW = &nopBlockWriter{}

func recoverCall[T Value](txn *Transaction[T], fn TransactionFn[T]) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic caught: %v", err)
		}
	}()

	return fn(txn)
}

func isDone(ctx context.Context) (done bool) {
	select {
	case <-ctx.Done():
		done = true
	default:
	}

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

func getFirstID(c IDCursor, lastID string, reverse bool) (entryID string, err error) {
	// If last ID is set, we need to seek
	isSeeking := len(lastID) > 0

	switch {
	case isSeeking && !reverse:
		if _, err = c.Seek(lastID); err != nil {
			return
		}

		return c.Next()
	case isSeeking && reverse:
		if _, err = c.SeekReverse(lastID); err != nil {
			return
		}

		return c.Prev()

	// Seek to does not exist
	case !reverse:
		return c.First()
	case reverse:
		return c.Last()
	}

	return
}

func getFirst[T Value](c Cursor[T], lastID string, reverse bool) (v T, err error) {
	switch {
	case len(lastID) > 0 && !reverse:
		if _, err = c.Seek(lastID); err != nil {
			return
		}

		return c.Next()
	case len(lastID) > 0 && reverse:
		if _, err = c.SeekReverse(lastID); err != nil {
			return
		}

		return c.Prev()

	// Seek to does not exist
	case !reverse:
		return c.First()
	case reverse:
		return c.Last()
	}

	return
}

func splitSeekID(seekID []byte) (relationshipID, entryID []byte) {
	if len(seekID) == 0 {
		return
	}

	spl := bytes.SplitN(seekID, []byte("::"), 2)

	// Set relationship ID
	relationshipID = spl[0]

	if len(spl) == 2 {
		// Split is a length of 2, set entry ID
		entryID = spl[1]
	}

	return
}

func joinSeekID(relationshipID, entryID string) (seekID string) {
	return strings.Join([]string{relationshipID, entryID}, "::")
}

func hasEntries(bkt backend.Bucket) (ok bool) {
	k, _ := bkt.Cursor().First()
	return len(k) > 0
}

func getRelationshipsAsBytes(relationships []string) (out [][]byte) {
	for _, relationship := range relationships {
		rbs := []byte(relationship)
		out = append(out, rbs)
	}

	return
}

type blockWriter interface {
	AddBlock(t kiroku.Type, key, value []byte) error
	Meta() kiroku.Meta
}

type nopBlockWriter struct{}

func (n *nopBlockWriter) AddBlock(t kiroku.Type, key, value []byte) error {
	return nil
}

func (n *nopBlockWriter) Meta() (m kiroku.Meta) {
	return
}

func closeSema(c chan struct{}) {
	if c == nil {
		return
	}

	close(c)
}
