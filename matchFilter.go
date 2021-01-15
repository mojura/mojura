package mojura

// MakeMatchFilter create a relationship pair
func MakeMatchFilter(relationship, relationshipID string) (f Filter) {
	var m MatchFilter
	m.Relationship = relationship
	m.RelationshipID = relationshipID
	f = &m
	return
}

// MatchFilter represents a relationship key and ID
type MatchFilter struct {
	// Relationship represents the relationship to target
	Relationship string `json:"relationship"`
	// RelationshipID represents the ID of the corasponding relationship
	RelationshipID string `json:"ID"`
}

func (m *MatchFilter) relationship() []byte {
	return []byte(m.Relationship)
}

func (m *MatchFilter) id() []byte {
	return []byte(m.RelationshipID)
}

func (m *MatchFilter) cursor(txn *Transaction) (filterCursor, error) {
	return newMatchCursor(txn, []byte(m.Relationship), []byte(m.RelationshipID))
}
