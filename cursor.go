package mojura

// Cursor is used to iterate through entries
type Cursor[T Value] interface {
	Seek(seekID string) (value T, err error)
	SeekReverse(seekID string) (value T, err error)

	First() (value T, err error)
	Last() (value T, err error)
	Next() (value T, err error)
	Prev() (value T, err error)

	getCurrentRelationshipID() (relationshipID string)
	teardown()
}
