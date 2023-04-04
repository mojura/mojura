package mojura

// ForEachFn is called during iteration
type ForEachFn[T Value] func(entryID string, value T) error

// ForEachIDFn is called during iteration
type ForEachIDFn func(entryID string) error
