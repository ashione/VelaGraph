package native

import (
	"math"
	"unicode/utf8"
)

// Conservative wire estimate before json.Marshal, including worst-case escapes.
// It bounds SDK encoding, not memory already owned by the application.
type estimate uint64

func (e *estimate) add(n uint64) error {
	if n > MaxInputBytes || uint64(*e) > MaxInputBytes-n {
		return &Error{Code: CodeLimit, Message: "request wire estimate exceeds ceiling"}
	}
	*e += estimate(n)
	return nil
}
func checkString(s string) error {
	if !utf8.ValidString(s) {
		return &Error{Code: CodeInvalidArgument, Message: "native strings must be UTF-8"}
	}
	if len(s) > MaxInputBytes/6 {
		return &Error{Code: CodeLimit, Message: "native string exceeds wire estimate"}
	}
	return nil
}
func (e *estimate) text(s string) error {
	if err := checkString(s); err != nil {
		return err
	}
	return e.add(uint64(len(s))*6 + 3)
}
func (e *estimate) props(p map[string]Value) error {
	for k, v := range p {
		if err := e.add(96); err != nil {
			return err
		}
		if err := e.text(k); err != nil {
			return err
		}
		if v.kind == "Text" {
			if err := e.text(v.text); err != nil {
				return err
			}
		}
		if v.kind == "Real" && (math.IsNaN(v.real) || math.IsInf(v.real, 0)) {
			return &Error{Code: CodeInvalidArgument, Message: "non-finite native Real"}
		}
	}
	return nil
}
func (e *estimate) vertex(v Vertex) error {
	if err := e.add(128); err != nil {
		return err
	}
	for _, label := range v.Labels {
		if err := e.text(label); err != nil {
			return err
		}
	}
	return e.props(v.Properties)
}
func (e *estimate) edge(v Edge) error {
	if err := e.add(160); err != nil {
		return err
	}
	if err := e.text(v.Label); err != nil {
		return err
	}
	return e.props(v.Properties)
}
func checkMemory(o MemoryOptions) error {
	if o.Mode != "" && o.Mode != "cow" && o.Mode != "locked" {
		return &Error{Code: CodeInvalidArgument, Message: "unknown memory mode"}
	}
	if len(o.Vertices)+len(o.Edges) > 1_000_000 || len(o.Metadata) > 65536 {
		return &Error{Code: CodeLimit, Message: "memory load ceiling"}
	}
	e := estimate(1024)
	if err := e.text(o.Identity); err != nil {
		return err
	}
	if err := e.text(o.RestorePath); err != nil {
		return err
	}
	if err := e.add(uint64(len(o.Metadata)) * 4); err != nil {
		return err
	}
	for _, v := range o.Vertices {
		if err := e.vertex(v); err != nil {
			return err
		}
	}
	for _, v := range o.Edges {
		if err := e.edge(v); err != nil {
			return err
		}
	}
	return nil
}
func checkBatch(b WriteBatch) error {
	if len(b.Mutations) > 10_000 || len(b.Metadata) > 65536 {
		return &Error{Code: CodeLimit, Message: "mutation batch ceiling"}
	}
	e := estimate(512)
	if err := e.add(uint64(len(b.Metadata)) * 4); err != nil {
		return err
	}
	for _, m := range b.Mutations {
		if err := e.add(192); err != nil {
			return err
		}
		if m.Vertex != nil {
			if err := e.vertex(*m.Vertex); err != nil {
				return err
			}
		}
		if m.Edge != nil {
			if err := e.edge(*m.Edge); err != nil {
				return err
			}
		}
	}
	return nil
}
func checkQueries(qs []Query) error {
	if len(qs) == 0 || len(qs) > MaxQueries {
		return &Error{Code: CodeLimit, Message: "query batch must contain 1..64 queries"}
	}
	e := estimate(1024)
	for _, q := range qs {
		if (q.Prepared != nil && q.Text != "") || (q.Prepared == nil && q.Text == "") {
			return &Error{Code: CodeInvalidArgument, Message: "select query text or prepared plan"}
		}
		if len(q.Text) > 16*1024 || len(q.StartIDs) > 64 || len(q.Values) > 64 {
			return &Error{Code: CodeLimit, Message: "query binding ceiling"}
		}
		if err := e.add(512 + uint64(len(q.StartIDs))*24 + uint64(len(q.Values))*96); err != nil {
			return err
		}
		if err := e.text(q.Text); err != nil {
			return err
		}
		for _, v := range q.Values {
			if v.kind == "Text" {
				if err := e.text(v.text); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
