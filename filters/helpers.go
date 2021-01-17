package filters

// LessThan is an alias func for a less than comparison
func LessThan(relationshipKey, lessThan string) *ComparisonFilter {
	fn := func(relationshipID string) (ok bool, err error) {
		// TODO: Find clean solution to break early without importing mojura.Break
		ok = relationshipID < lessThan
		return
	}

	return ComparisonWithRange(relationshipKey, "", lessThan, fn)
}

// LessThanOrEqualTo is an alias func for a less than or equal to comparison
func LessThanOrEqualTo(relationshipKey, lessThanOrEqualto string) *ComparisonFilter {
	fn := func(relationshipID string) (ok bool, err error) {
		ok = relationshipID <= lessThanOrEqualto
		return
	}

	return ComparisonWithRange(relationshipKey, "", lessThanOrEqualto, fn)
}

// GreaterThan is an alias func for a greater than comparison
func GreaterThan(relationshipKey, greaterThan string) *ComparisonFilter {
	fn := func(relationshipID string) (ok bool, err error) {
		// TODO: Find clean solution to break early without importing mojura.Break
		ok = relationshipID > greaterThan
		return
	}

	return ComparisonWithRange(relationshipKey, greaterThan, "", fn)
}

// GreaterThanOrEqualTo is an alias func for a greater than or equal to comparison
func GreaterThanOrEqualTo(relationshipKey, greaterThanOrEqualTo string) *ComparisonFilter {
	fn := func(relationshipID string) (ok bool, err error) {
		ok = relationshipID >= greaterThanOrEqualTo
		return
	}

	return ComparisonWithRange(relationshipKey, greaterThanOrEqualTo, "", fn)
}

// Range is an alias func for range comparison
func Range(relationshipKey, rangeStart, rangeEnd string) *ComparisonFilter {
	return ComparisonWithRange(relationshipKey, rangeStart, rangeEnd, nopComparisonFn)
}
