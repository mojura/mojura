package mojura

func makeComparisonOpts(relationshipKey string, comparison ComparisonFn) (o ComparisonOpts) {
	o.RelationshipKey = []byte(relationshipKey)
	o.Comparison = comparison
	return
}

// ComparisonOpts are the options used to initialize a new comparison filter
type ComparisonOpts struct {
	RelationshipKey []byte
	Comparison      ComparisonFn

	// Optional
	RangeStart []byte
	RangeEnd   []byte
}
