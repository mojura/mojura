package mojura

var (
	nopC filterCursor = &nopCursor{}
)

type nopCursor struct{}

// SeekForward will seek the provided ID
func (c *nopCursor) SeekForward(relationshipID, seekID []byte) (entryID []byte, err error) {
	return c.seek(seekID)
}

// SeekReverse will seek the provided ID
func (c *nopCursor) SeekReverse(relationshipID, seekID []byte) (entryID []byte, err error) {
	return c.seek(seekID)
}

// First will return the first entry
func (c *nopCursor) First() (entryID []byte, err error) {
	err = Break
	return
}

// Last will return the last entry
func (c *nopCursor) Last() (entryID []byte, err error) {
	err = Break
	return
}

// Next will return the next entry
func (c *nopCursor) Next() (entryID []byte, err error) {
	err = Break
	return
}

// Prev will return the previous entry
func (c *nopCursor) Prev() (entryID []byte, err error) {
	err = Break
	return
}

// HasForward will determine if an entry exists in a forward direction
func (c *nopCursor) HasForward(entryID []byte) (ok bool, err error) {
	return
}

// HasReverse will determine if an entry exists in a reverse direction
func (c *nopCursor) HasReverse(entryID []byte) (ok bool, err error) {
	return
}

func (c *nopCursor) teardown() {
}

func (c *nopCursor) seek(id []byte) (entryID []byte, err error) {
	err = Break
	return
}

func (c *nopCursor) getCurrentRelationshipID() (relationshipID string) {
	return
}
