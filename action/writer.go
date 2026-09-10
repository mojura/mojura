package action

import (
	"bytes"

	"github.com/mojura/enkodo"
)

// MakeWriter creates an action encoder writing blocks to w.
func MakeWriter(w BlockWriter) (a Writer) {
	a.buf = bytes.NewBuffer(nil)
	a.w = w
	return
}

// Writer encodes write and delete actions. It is not safe for concurrent use.
type Writer struct {
	buf *bytes.Buffer
	w   BlockWriter
}

// Write appends an action containing entryID and an already encoded value.
func (w *Writer) Write(entryID, value []byte) (err error) {
	return w.addBlock(TypeWrite, entryID, value)
}

// Delete appends a deletion action for entryID.
func (w *Writer) Delete(entryID []byte) (err error) {
	return w.addBlock(TypeDelete, entryID, nil)
}

func (w *Writer) addBlock(t Type, entryID, value []byte) (err error) {
	var a Action
	a.Key = entryID
	a.Value = value
	a.Type = t

	w.buf.Reset()
	if err = enkodo.NewWriter(w.buf).Encode(&a); err != nil {
		return
	}

	return w.w.Write(w.buf.Bytes())
}
