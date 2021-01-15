package mojura

// MakeComparisonFilter create a relationship pair
func MakeComparisonFilter(o ComparisonOpts) (f Filter) {
	var c ComparisonFilter
	c.Opts = o
	f = &c
	return
}

// ComparisonFilter represents a relationship key and ID
type ComparisonFilter struct {
	Opts ComparisonOpts
}

func (m *ComparisonFilter) cursor(txn *Transaction) (filterCursor, error) {
	return newComparisonCursor(txn, m.Opts)
}
