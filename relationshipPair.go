package dbl

// NewRelationshipPair will return a new relationship pair
func NewRelationshipPair(relationship, id string) *RelationshipPair {
	var r RelationshipPair
	r.Relationship = relationship
	r.ID = id
	return &r
}

// RelationshipPair represents a relationship key and ID
type RelationshipPair struct {
	Relationship string `json:"relationship"`
	ID           string `json:"ID"`
}

func (r *RelationshipPair) relationship() []byte {
	return []byte(r.Relationship)
}

func (r *RelationshipPair) id() []byte {
	return []byte(r.ID)
}
