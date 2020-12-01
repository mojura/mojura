package dbl

import "encoding/json"

// Encoder represents an encoder for DBL Entries
type Encoder interface {
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
}

// JSONEncoder represents a JSON encoder
type JSONEncoder struct{}

// Marshal is an encoding helper method
func (j *JSONEncoder) Marshal(value interface{}) (bs []byte, err error) {
	return json.Marshal(value)
}

// Unmarshal is a decoding helper method
func (j *JSONEncoder) Unmarshal(bs []byte, val interface{}) (err error) {
	return json.Unmarshal(bs, val)
}
