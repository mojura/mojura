package core

// Value represents an entry value
type Value interface {
	SetID(string)
	SetCreatedAt(int64)
	SetUpdatedAt(int64)
}
