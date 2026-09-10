package mojura

var defaultFilteringOpts = &FilteringOpts{Limit: -1}

// NewFilteringOpts creates query options with the supplied filters and unlimited results.
func NewFilteringOpts(fs ...Filter) *FilteringOpts {
	var f FilteringOpts
	f.Filters = fs
	f.Limit = defaultFilteringOpts.Limit
	return &f
}

// FilteringOpts controls collection and iteration. Use NewFilteringOpts for an
// unlimited query; the zero value collects no results. GetFirst/GetLast require
// non-nil options, unlike collection and ForEach methods.
type FilteringOpts struct {
	// LastID resumes after a cursor position using the same filters and direction.
	// Filtered queries use composite tokens; unfiltered cursors currently require
	// a raw entry ID despite collection methods returning composite tokens.
	LastID string
	// Reverse selects backward collection/iteration; GetFirst/GetLast ignore it.
	Reverse bool
	// Filters are intersected, with the first driving traversal order.
	Filters []Filter

	// Limit caps newly collected results; negative is unlimited and zero is empty.
	// ForEach, ForEachID, GetFirst, and GetLast ignore it.
	Limit int64
}
