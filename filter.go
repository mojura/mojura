package mojura

// Filter can generate a filter cursor
type Filter interface {
	cursor(*Transaction) (filterCursor, error)
}
