package mojura

// IDCursor traverses entry IDs without decoding values within a transaction.
// Exhaustion returns Break. Seek positions follow the same rules as Cursor.
// Do not retain a cursor beyond its transaction callback or share it concurrently.
type IDCursor interface {
	Seek(seekID string) (entryID string, err error)
	SeekReverse(seekID string) (entryID string, err error)
	First() (entryID string, err error)
	Last() (entryID string, err error)
	Next() (entryID string, err error)
	Prev() (entryID string, err error)

	getCurrentRelationshipID() (relationshipID string)

	seek(seekID []byte) (entryID []byte, err error)
	seekReverse(seekID []byte) (entryID []byte, err error)
	first() (entryID []byte, err error)
	last() (entryID []byte, err error)
	next() (entryID []byte, err error)
	prev() (entryID []byte, err error)
	teardown()
}
