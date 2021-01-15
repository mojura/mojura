package mojura

// ForEachFn is called during iteration
type ForEachFn func(entryID string, val Value) error

// ForEachIDFn is called during iteration
type ForEachIDFn func(entryID string) error
