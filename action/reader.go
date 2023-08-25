package action

import (
	"bytes"
	"io"

	"github.com/mojura/enkodo"
	"github.com/mojura/kiroku"
)

func MakeReader(kr *kiroku.Reader) (r Reader) {
	r.r = kr
	return
}

type Reader struct {
	r *kiroku.Reader
}

func (r *Reader) ForEach(fn func(Action) error) (err error) {
	err = r.r.ForEach(0, func(b kiroku.Block) (err error) {
		var a Action
		rdr := bytes.NewReader(b)
		if err = enkodo.NewReader(rdr).Decode(&a); err != nil {
			return
		}

		return fn(a)
	})

	if err == io.EOF {
		return nil
	}

	return
}
