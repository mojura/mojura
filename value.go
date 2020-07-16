package dbl

// Value represents an entry value
type Value interface {
	GetID() string
	GetCreatedAt() int64
	GetUpdatedAt() int64
	GetRelationshipIDs() []string

	SetID(string)
	SetCreatedAt(int64)
	SetUpdatedAt(int64)
}
