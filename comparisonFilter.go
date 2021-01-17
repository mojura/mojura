package mojura

var _ Filter = &ComparisonFilter{}

// NewComparisonFilter create a relationship pair
func NewComparisonFilter(relationshipKey string, comparison ComparisonFn) (f Filter) {
	opts := makeComparisonOpts(relationshipKey, comparison)
	return newComparisonFilter(opts)
}

// NewComparisonFilterWithRange create a relationship pair with range options
func NewComparisonFilterWithRange(relationshipKey, rangeStart, rangeEnd string, comparison ComparisonFn) (f Filter) {
	opts := makeComparisonOpts(relationshipKey, comparison)
	opts.RangeStart = []byte(rangeStart)
	opts.RangeEnd = []byte(rangeEnd)
	return newComparisonFilter(opts)
}

func newComparisonFilter(opts ComparisonOpts) *ComparisonFilter {
	var c ComparisonFilter
	c.Opts = opts
	return &c
}

// ComparisonFilter represents a relationship key and ID
type ComparisonFilter struct {
	Opts ComparisonOpts
}

func (c *ComparisonFilter) cursor(txn *Transaction) (filterCursor, error) {
	return newComparisonCursor(txn, c.Opts)
}
