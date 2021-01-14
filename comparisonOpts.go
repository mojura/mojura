package mojura

// MakeComparisonOpts will create a new set of comparison opts with the required fields
// Note: RangeStart and RangeEnd can be set after creation
func MakeComparisonOpts(relationshipKey []byte, comparison ComparisonFn) (o ComparisonOpts) {
	o.RelationshipKey = relationshipKey
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
