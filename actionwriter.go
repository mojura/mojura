package mojura

import (
	"bytes"

	"github.com/mojura/enkodo"
)

func makeActionwriter(w blockWriter) (a actionwriter) {
	a.buf = bytes.NewBuffer(nil)
	a.w = w
	return
}

type actionwriter struct {
	buf *bytes.Buffer
	w   blockWriter
}

func (w *actionwriter) Write(entryID, value []byte) (err error) {
	return w.addBlock(actiontypeWrite, entryID, value)
}

func (w *actionwriter) Delete(entryID []byte) (err error) {
	return w.addBlock(actiontypeDelete, entryID, nil)
}

func (w *actionwriter) addBlock(t actiontype, entryID, value []byte) (err error) {
	var a action
	a.Key = entryID
	a.Value = value
	a.Type = t

	w.buf.Reset()
	if err = enkodo.NewWriter(w.buf).Encode(&a); err != nil {
		return
	}

	return w.w.Write(w.buf.Bytes())
}
