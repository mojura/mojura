package mojura

// MakeFilter create a relationship pair
func MakeFilter(relationship, relationshipID string, inverseComparison bool) (f Filter) {
	f.Relationship = relationship
	f.RelationshipID = relationshipID
	f.InverseComparison = inverseComparison
	return
}

// Filter represents a relationship key and ID
type Filter struct {
	// Relationship represents the relationship to target
	Relationship string `json:"relationship"`
	// RelationshipID represents the ID of the corasponding relationship
	RelationshipID string `json:"ID"`
	// InverseComparison represents whether or not the filter is an inverse comparison
	InverseComparison bool `json:"inverseComparison"`
}

func (r *Filter) relationship() []byte {
	return []byte(r.Relationship)
}

func (r *Filter) id() []byte {
	return []byte(r.RelationshipID)
}
