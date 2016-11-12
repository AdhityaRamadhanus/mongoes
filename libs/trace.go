package libs

import (
	"fmt"
	"io"
)

type Tracer interface {
	Trace(...interface{})
}

type tracer struct {
	writer io.Writer
}

type niltracer struct{}

func (t *tracer) Trace(a ...interface{}) {
	t.writer.Write([]byte(fmt.Sprint(a...)))
	t.writer.Write([]byte("\n"))
}

func (t *niltracer) Trace(a ...interface{}) {}

func NewTracer(w io.Writer) Tracer {
	return &tracer{writer: w}
}

func OffTracer() Tracer {
	return &niltracer{}
}
