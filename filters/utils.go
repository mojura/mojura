package filters

func nopComparisonFn(relationshipID string) (ok bool, err error) {
	return true, nil
}
