package dbl

// Entry is a base entry type for dbl Entries. This type contains all
// the base methods required by dbl. This struct can be included to decrease
// the time of initial integration
type Entry struct {
	// Entry ID
	ID string `json:"id"`
	// Unix timestamp of Entry creation time
	CreatedAt int64 `json:"createdAt"`
	// Unix timestamp of last Entry update
	UpdatedAt int64 `json:"updatedAt"`
}

// GetID will get the message ID
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

// SetID will get the message ID
func (e *Entry) SetID(id string) {
	e.ID = id
}

// SetCreatedAt will get the created at timestamp
func (e *Entry) SetCreatedAt(createdAt int64) {
	e.CreatedAt = createdAt
}

// SetUpdatedAt will get the updated at timestamp
func (e *Entry) SetUpdatedAt(updatedAt int64) {
	e.UpdatedAt = updatedAt
}
