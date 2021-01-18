package mojura

// Cursor is used to iterate through entries
type Cursor interface {
	Seek(seekID string) (value Value, err error)
	SeekReverse(seekID string) (value Value, err error)

	First() (value Value, err error)
	Last() (value Value, err error)
	Next() (value Value, err error)
	Prev() (value Value, err error)

	getCurrentRelationshipID() (relationshipID string)
}
