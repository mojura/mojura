package mojura

// Encoder serializes entry payloads, excluding indexes and metadata.
// Unmarshal must support the destination supplied by Mojura, including a pointer
// to a generic pointer value, as encoding/json does.
type Encoder interface {
	Marshal(any) ([]byte, error)
	Unmarshal([]byte, any) error
}
