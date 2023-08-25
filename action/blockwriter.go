package action

type BlockWriter interface {
	Write(value []byte) error
}
