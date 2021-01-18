package mojura

var defaultIteratingOpts = &IteratingOpts{}

// NewIteratingOpts will initialize a new instance of Iterating Opts
func NewIteratingOpts(fs ...Filter) *IteratingOpts {
	var i IteratingOpts
	i.Filters = fs
	return &i
}

// IteratingOpts represents iterating options
type IteratingOpts struct {
	LastID  string
	Reverse bool
	Filters []Filter
}
