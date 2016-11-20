package mongoes

import (
	"fmt"
	"io"
)

// Tracer / logger
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

// NewTracer return normal tracer
func NewTracer(w io.Writer) Tracer {
	return &tracer{writer: w}
}

// OffTracer return tracer that do nothing
func OffTracer() Tracer {
	return &niltracer{}
}
