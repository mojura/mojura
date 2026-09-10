package mojura

// Entry supplies the Value metadata methods and empty relationships.
// Embed it by value in a concrete type and override GetRelationships for indexes.
type Entry struct {
	// ID is the storage key, assigned by New or Put.
	ID string `json:"id"`
	// CreatedAt is the creation time in Unix seconds; writes fill it only when zero.
	CreatedAt int64 `json:"createdAt"`
	// UpdatedAt is refreshed to Unix seconds on writes, including history replay.
	UpdatedAt int64 `json:"updatedAt"`
}

// GetID returns the entry ID.
func (e *Entry) GetID() (id string) {
	return e.ID
}

// GetCreatedAt will get the created at timestamp
func (e *Entry) GetCreatedAt() (createdAt int64) {
	return e.CreatedAt
}

// GetUpdatedAt will get the updated at timestamp
func (e *Entry) GetUpdatedAt() (updatedAt int64) {
	return e.UpdatedAt
}

// GetRelationshipIDs will get the associated relationship IDs
// Deprecated: This method is now deprecated. The method has been kept and the signature
// has been changed to ensure previous use of this method would be easily caught by
// the compiler. If this method was removed, the fear is that the prior use of the
// unused method would still continue (since the interface would still technically match).
func (e *Entry) GetRelationshipIDs() (deprecated string) {
	return
}

// GetRelationships will get the associated relationships
// Note: This will have to be replaced by the including entry if relationships are needed
func (e *Entry) GetRelationships() (r Relationships) {
	return
}

// SetID sets the entry ID.
func (e *Entry) SetID(id string) {
	e.ID = id
}

// SetCreatedAt sets the creation timestamp in Unix seconds.
func (e *Entry) SetCreatedAt(createdAt int64) {
	e.CreatedAt = createdAt
}

// SetUpdatedAt sets the update timestamp in Unix seconds.
func (e *Entry) SetUpdatedAt(updatedAt int64) {
	e.UpdatedAt = updatedAt
}
