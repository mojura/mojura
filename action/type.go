package action

import "fmt"

const (
	// TypeUnset represents an unset block type
	TypeUnset Type = iota
	// TypeWrite represents a write action block
	TypeWrite
	// TypeDelete represets a delete action block
	TypeDelete
	// TypeComment represents a comment block
	TypeComment
)

const invalidactiontypeLayout = "invalid type, <%d> is not supported"

// actiontype represents a block type
type Type uint8

// Validate will ensure a type is valid
func (t Type) Validate() (err error) {
	switch t {
	case TypeWrite:
	case TypeDelete:
	case TypeComment:

	default:
		// Currently set as an unsupported type, return error
		return fmt.Errorf(invalidactiontypeLayout, t)
	}

	return
}

// Validate will ensure a type is valid
func (t Type) String() string {
	switch t {
	case TypeUnset:
		return "unset"
	case TypeWrite:
		return "write"
	case TypeDelete:
		return "delete"
	case TypeComment:
		return "comment"

	default:
		// Current type is not supported, return invalid
		return "invalid"
	}
}
