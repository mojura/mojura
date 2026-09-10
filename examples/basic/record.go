package main

import "github.com/mojura/mojura"

// record stores a message indexed by owner and tags.
type record struct {
	mojura.Entry
	OwnerID string   `json:"ownerID"`
	Tags    []string `json:"tags"`
	Message string   `json:"message"`
}

// GetRelationships returns owner and tag slots in the order supplied to New.
func (r *record) GetRelationships() (relationships mojura.Relationships) {
	relationships.Append(r.OwnerID)
	// Copy the tag IDs so an Update callback cannot mutate the old index snapshot.
	relationships.Append(append([]string(nil), r.Tags...)...)
	return relationships
}
