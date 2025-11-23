package mojura

// Encoder represents an encoder for Mojura Entries
type Encoder interface {
	Marshal(any) ([]byte, error)
	Unmarshal([]byte, any) error
}
