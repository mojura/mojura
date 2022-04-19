package mojura

var defaultFilteringOpts = &FilteringOpts{Limit: -1}

// NewFilteringOpts will initialize a new instance of Filtering Opts
func NewFilteringOpts(fs ...Filter) *FilteringOpts {
	var f FilteringOpts
	f.Filters = fs
	f.Limit = defaultFilteringOpts.Limit
	return &f
}

// FilteringOpts represents iterating options
type FilteringOpts struct {
	LastID  string
	Reverse bool
	Filters []Filter

	// Note: Limit is only utilized for Filtering, it is ignored for ForEach statements
	Limit int64
}
