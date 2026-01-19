package mojura

import (
	"log"
	"strings"
	"sync"
)

func NewLogger() Logger {
	return &logger{}
}

type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type logger struct {
	mux sync.Mutex
}

func (l *logger) Info(msg string, args ...any) {
	l.log("INFO", msg, args...)
}

func (l *logger) Warn(msg string, args ...any) {
	l.log("WARN", msg, args...)
}

func (l *logger) Error(msg string, args ...any) {
	l.log("ERROR", msg, args...)
}

func (l *logger) log(level, msg string, args ...any) {
	var b strings.Builder
	line := makeLogLine(level, msg, args)
	err := line.write(&b)

	l.mux.Lock()
	defer l.mux.Unlock()

	if err != nil {
		log.Println(err)
		return
	}

	log.Println(b.String())
}
