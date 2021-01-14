package mojura

type filterCursor interface {
	SeekForward(relationshipKey, seekID []byte) (entryID []byte, err error)
	SeekReverse(relationshipKey, seekID []byte) (entryID []byte, err error)

	First() (entryID []byte, err error)
	Last() (entryID []byte, err error)
	Next() (entryID []byte, err error)
	Prev() (entryID []byte, err error)

	HasForward(entryID []byte) (ok bool, err error)
	HasReverse(entryID []byte) (ok bool, err error)
}
