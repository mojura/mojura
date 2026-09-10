package filters

// Comparison compares indexed relationship IDs, or entry IDs when relationshipKey is empty.
func Comparison(relationshipKey string, comparison ComparisonFn) *ComparisonFilter {
	return ComparisonWithRange(relationshipKey, "", "", comparison)
}

// ComparisonWithRange adds inclusive lexical traversal bounds; empty endpoints
// are unbounded. Current Mojura cursors have boundary-positioning defects, so use
// Comparison with an explicit predicate when strict membership bounds are needed.
func ComparisonWithRange(relationshipKey, rangeStart, rangeEnd string, comparison ComparisonFn) *ComparisonFilter {
	var c ComparisonFilter
	c.RelationshipKey = relationshipKey
	c.RangeStart = rangeStart
	c.RangeEnd = rangeEnd
	c.Comparison = comparison
	return &c
}

// ComparisonFilter selects IDs using a non-nil callback and optional lexical bounds.
type ComparisonFilter struct {
	RelationshipKey string `json:"relationshipKey"`
	// TODO: implement MQL here when available
	Comparison ComparisonFn `json:"-"`

	RangeStart string `json:"rangeStart"`
	RangeEnd   string `json:"rangeEnd"`
}

// ComparisonFn tests an indexed relationship ID (or an entry ID for an empty key).
type ComparisonFn func(relationshipID string) (ok bool, err error)
