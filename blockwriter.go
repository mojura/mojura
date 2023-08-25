package mojura

type nopBlockWriter struct{}

func (n *nopBlockWriter) Write(value []byte) error {
	return nil
}
