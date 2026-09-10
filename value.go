package mojura

// Value is the metadata and relationship contract for a stored value.
// Prefer a concrete pointer type embedding Entry by value. GetRelationships must
// work on a newly allocated value and return fixed slots in constructor-key order.
// GetRelationshipIDs remains required for compatibility but is not used for indexing.
type Value interface {
	GetID() string
	GetCreatedAt() int64
	GetUpdatedAt() int64
	GetRelationships() Relationships
	// Deprecated: This method is now deprecated. The method has been kept and the signature
	// has been changed to ensure previous use of this method would be easily caught by
	// the compiler. If this method was removed, the fear is that the prior use of the
	// unused method would still continue (since the interface would still technically match).
	GetRelationshipIDs() (deprecated string)

	SetID(string)
	SetCreatedAt(int64)
	SetUpdatedAt(int64)
}
