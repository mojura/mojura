package filters

// Comparison creates a new comparison Filter
func Comparison(relationshipKey string, comparison ComparisonFn) *ComparisonFilter {
	return ComparisonWithRange(relationshipKey, "", "", comparison)
}

// ComparisonWithRange creates a new comparison Filter with a range
func ComparisonWithRange(relationshipKey, rangeStart, rangeEnd string, comparison ComparisonFn) *ComparisonFilter {
	var c ComparisonFilter
	c.RelationshipKey = relationshipKey
	c.RangeStart = rangeStart
	c.RangeEnd = rangeEnd
	c.Comparison = comparison
	return &c
}

// ComparisonFilter represents a relationship key and ID
type ComparisonFilter struct {
	RelationshipKey string `json:"relationshipKey"`
	// TODO: implement MQL here when available
	Comparison ComparisonFn `json:"-"`

	RangeStart string `json:"rangeStart"`
	RangeEnd   string `json:"rangeEnd"`
}

// ComparisonFn is used for comparison filters
type ComparisonFn func(relationshipID string) (ok bool, err error)
