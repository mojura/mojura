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
	IteratingOpts
	Limit int64
}
