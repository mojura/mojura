package action

// BlockWriter accepts an encoded history action block.
type BlockWriter interface {
	Write(value []byte) error
}
