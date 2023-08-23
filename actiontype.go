package mojura

import "fmt"

const (
	// actiontypeUnset represents an unset block type
	actiontypeUnset actiontype = iota
	// actiontypeWrite represents a write action block
	actiontypeWrite
	// actiontypeDelete represets a delete action block
	actiontypeDelete
	// actiontypeComment represents a comment block
	actiontypeComment
)

const invalidactiontypeLayout = "invalid type, <%d> is not supported"

// actiontype represents a block type
type actiontype uint8

// Validate will ensure a type is valid
func (t actiontype) Validate() (err error) {
	switch t {
	case actiontypeWrite:
	case actiontypeDelete:
	case actiontypeComment:

	default:
		// Currently set as an unsupported type, return error
		return fmt.Errorf(invalidactiontypeLayout, t)
	}

	return
}

// Validate will ensure a type is valid
func (t actiontype) String() string {
	switch t {
	case actiontypeUnset:
		return "unset"
	case actiontypeWrite:
		return "write"
	case actiontypeDelete:
		return "delete"
	case actiontypeComment:
		return "comment"

	default:
		// Currently st as an unsupported type, return error
		return "invalid"
	}
}
