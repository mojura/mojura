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
	// RelationshipKey names the index to target.
	RelationshipKey string `json:"relationshipKey"`
	// RelationshipID is the indexed membership value.
	RelationshipID string `json:"relationshipID"`
}

// InverseMatch creates an inverse membership filter. As a primary filter, Mojura
// scans other relationship-ID buckets; as a secondary filter, it rejects entries
// in the excluded bucket. Results can differ for absent or multiple memberships.
func InverseMatch(relationshipKey, relationshipID string) *InverseMatchFilter {
	var m InverseMatchFilter
	m.RelationshipKey = relationshipKey
	m.RelationshipID = relationshipID
	return &m
}

// InverseMatchFilter will inverse match against a relationship key and relationship ID
type InverseMatchFilter struct {
	// RelationshipKey names the index to target.
	RelationshipKey string `json:"relationshipKey"`
	// RelationshipID is the indexed membership value.
	RelationshipID string `json:"relationshipID"`
}
