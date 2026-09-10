package mojura

// Filter holds a supported filter description. Current dispatch accepts only
// *filters.MatchFilter, *filters.InverseMatchFilter, and *filters.ComparisonFilter;
// arbitrary implementations return an unsupported-filter error.
type Filter interface {
}
