package action

import (
	"bytes"

	"github.com/mojura/enkodo"
)

func MakeWriter(w BlockWriter) (a Writer) {
	a.buf = bytes.NewBuffer(nil)
	a.w = w
	return
}

type Writer struct {
	buf *bytes.Buffer
	w   BlockWriter
}

func (w *Writer) Write(entryID, value []byte) (err error) {
	return w.addBlock(TypeWrite, entryID, value)
}

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
