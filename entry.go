package core

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
// Note: This will have to be replaced by the including entry if relationships are needed
func (e *Entry) GetRelationshipIDs() (ids []string) {
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
