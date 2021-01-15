package mojura

// IteratingOpts represents iterating options
type IteratingOpts struct {
	SeekTo  string
	Reverse bool
	Filters []Filter
}

func (i *IteratingOpts) isReverse() bool {
	if i == nil {
		return false
	}

	return i.Reverse
}

func (i *IteratingOpts) seekTo() string {
	if i == nil {
		return ""
	}

	return i.SeekTo
}

func (i *IteratingOpts) filters() []Filter {
	if i == nil {
		return nil
	}

	return i.Filters
}
