package scratch

import (
	"context"

	"github.com/hatchify/errors"
	"github.com/mojura/mojura"
)

const errCancel = errors.Error("scratch cancel")

// New will initialize a new instance of Scratch
func New(name, dir string, example mojura.Value) (sp *Scratch, err error) {
	var s Scratch
	if s.m, err = mojura.New(name, dir, example); err != nil {
		return
	}

	sp = &s
	return
}

// Scratch manages a scratch DB
type Scratch struct {
	m *mojura.Mojura
}

// Transaction will initialize a new transaction
// Note: The transaction data does not persist
func (s *Scratch) Transaction(ctx context.Context, fn mojura.TransactionFn) (err error) {
	err = s.m.Transaction(ctx, func(txn *mojura.Transaction) (err error) {
		if err = fn(txn); err != nil {
			return
		}

		return errCancel
	})

	if err == errCancel {
		return nil
	}

	return
}

// Close will close Scratch and it's underlying instance of Mojura
func (s *Scratch) Close() (err error) {
	return s.m.Close()
}
