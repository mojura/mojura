package mojura

import (
	"fmt"
	"io"
	"time"
)

var errInvalidLogKeyFmt = "invalid log, expecting key for index %d and received a value of %+v"

func makeLogLine(level, message string, args []any) (l logLine) {
	l.level = level
	l.message = message
	l.args = args
	return
}

type logLine struct {
	time    time.Time
	level   string
	message string
	args    []any

	partsCount int
}

type logWriter interface {
	io.StringWriter
	io.ByteWriter
}

func (l *logLine) writePart(w logWriter, key, value string) (err error) {
	if l.partsCount > 0 {
		if err = w.WriteByte(' '); err != nil {
			return
		}
	}

	part := fmt.Sprintf("%s=%s", key, value)
	if _, err = w.WriteString(part); err != nil {
		return
	}

	l.partsCount++
	return
}

func (l *logLine) write(w logWriter) (err error) {
	l.writePart(w, "time", time.Now().UTC().Format(time.RFC3339))
	l.writePart(w, "level", l.level)
	l.writePart(w, "msg", fmt.Sprintf(`"%s"`, l.message))

	// structured fields
	for i := 0; i+1 < len(l.args); i += 2 {
		key, ok := l.args[i].(string)
		if !ok {
			return fmt.Errorf(errInvalidLogKeyFmt, i, l.args[i])
		}

		val := fmt.Sprint(l.args[i+1])
		l.writePart(w, key, val)
	}

	return nil
}
