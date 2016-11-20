package mongoes_test

import (
	"bytes"
	"github.com/AdhityaRamadhanus/mongoes"
	"testing"
)

func TestNew(t *testing.T) {
	var buf bytes.Buffer
	tracer := mongoes.NewTracer(&buf)
	if tracer == nil {
		t.Error("Return from New should not be nil")
	} else {
		tracer.Trace("Hello trace package.")
		if buf.String() != "Hello trace package.\n" {
			t.Errorf("Trace should not write '%s'.", buf.String())
		}
	}
}

func TestOff(t *testing.T) {
	var silentTracer mongoes.Tracer = mongoes.OffTracer()
	silentTracer.Trace("Nothing")
}
