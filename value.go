package core

// Value represents an entry value
type Value interface {
	GetID() string
	GetCreatedAt() int64
	GetUpdatedAt() int64

	SetID(string)
	SetCreatedAt(int64)
	SetUpdatedAt(int64)
}
