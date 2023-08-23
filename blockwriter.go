package mojura

type blockWriter interface {
	Write(value []byte) error
}

type nopBlockWriter struct{}

func (n *nopBlockWriter) Write(value []byte) error {
	return nil
}
