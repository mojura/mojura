package mojura

// IDCursor is used to iterate through entry IDs
type IDCursor interface {
	Seek(seekID string) (entryID string, err error)
	SeekReverse(seekID string) (entryID string, err error)
	First() (entryID string, err error)
	Last() (entryID string, err error)
	Next() (entryID string, err error)
	Prev() (entryID string, err error)
}
