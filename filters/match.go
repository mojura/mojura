package filters

// Match creates a new match filter
func Match(relationshipKey, relationshipID string) *MatchFilter {
	var m MatchFilter
	m.RelationshipKey = relationshipKey
	m.RelationshipID = relationshipID
	return &m
}

// MatchFilter will match against a relationship key and relationship ID
type MatchFilter struct {
	// Relationship represents the relationship to target
	RelationshipKey string `json:"relationshipKey"`
	// RelationshipID represents the ID of the corasponding relationship
	RelationshipID string `json:"relationshipID"`
}
