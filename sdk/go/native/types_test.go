package native

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestLosslessCodec(t *testing.T) {
	for _, v := range []Value{Integer(math.MaxInt64), Real(51.248178375505404), Real(math.Copysign(0, -1)), Text("<&\u2028"), Boolean(true), Null()} {
		b, e := json.Marshal(v)
		if e != nil {
			t.Fatal(e)
		}
		var got Value
		if e = json.Unmarshal(b, &got); e != nil {
			t.Fatal(e)
		}
		if got.kind != v.kind || got.integer != v.integer || math.Float64bits(got.real) != math.Float64bits(v.real) || got.text != v.text || got.boolean != v.boolean {
			t.Fatalf("roundtrip: %s", b)
		}
	}
	b, _ := json.Marshal(Vertex{ID: math.MaxUint64})
	var v Vertex
	if e := json.Unmarshal(b, &v); e != nil || v.ID != math.MaxUint64 {
		t.Fatal(string(b), e)
	}
	b, _ = json.Marshal(Bytes{0, 255})
	var out []byte
	if e := json.Unmarshal(b, &out); e != nil || len(out) != 2 || out[1] != 255 {
		t.Fatal(string(b), e)
	}
}
func TestRejectsAmbiguousOrNullWireVariants(t *testing.T) {
	for _, b := range []string{`{"Integer":null}`, `{"Integer":1,"Integer":2}`, `{"Real":null}`} {
		var v Value
		if json.Unmarshal([]byte(b), &v) == nil {
			t.Fatalf("accepted %s", b)
		}
	}
	for _, b := range []string{`{"Vertex":null}`, `{"Path":null}`, `{"Vertex":1,"Vertex":2}`} {
		var v Row
		if json.Unmarshal([]byte(b), &v) == nil {
			t.Fatalf("accepted %s", b)
		}
	}
}

func TestExecutionOutcomePreservesCommitAndContextCause(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		if got := executionError(CodeOK, "", cause); got != nil {
			t.Fatal(got)
		}
		got := executionError(CodeCancelled, "stopped", cause)
		if !errors.Is(got, cause) {
			t.Fatal(got, cause)
		}
		for _, code := range []Code{CodeOutcomeUnknown, CodeConflict, CodeIO, CodeLimit} {
			var ne *Error
			got := executionError(code, "native outcome", cause)
			if !errors.As(got, &ne) || ne.Code != code || errors.Is(got, cause) {
				t.Fatal(got, cause)
			}
		}
	}
}
