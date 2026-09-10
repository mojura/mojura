package mojura

// Cursor traverses values within its owning transaction callback.
// Exhaustion returns Break. Unfiltered seeks accept raw entry IDs; filtered seeks
// use relationshipID::entryID positions. SeekReverse is not consistently a floor
// seek for missing IDs. Do not retain a cursor beyond its callback or share it.
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
