// Package native provides the optional VelaGraph native SDK. Enable cgo and the
// velagraph_native build tag, and link the matching ABI1 native library.
package native

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"time"
)

const ABIVersion uint32 = 1
const MaxInputBytes = 64 << 20
const MaxOutputBytes = 64 << 20
const DefaultOutputBytes = 8 << 20
const MaxQueries = 64

type Code uint32

const (
	CodeOK Code = iota
	CodeInvalidArgument
	CodeInvalidHandle
	CodeUnsupported
	CodeLimit
	CodeConflict
	CodeCancelled
	CodeDeadline
	CodeIO
	CodeInternal
	CodeOutcomeUnknown
	CodeBufferTooSmall
	CodeBusy
	CodeCorrupt
)

var ErrUnavailable = errors.New("native backend requires cgo, velagraph_native tag and ABI1 library")
var ErrClosed = errors.New("native handle is closed")

// Error preserves the native outcome. OutcomeUnknown must not be automatically retried.
type Error struct {
	Code    Code
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("velagraph native (%d): %s", e.Code, e.Message) }
func (e *Error) Is(target error) bool {
	return (e.Code == CodeCancelled && target == context.Canceled) || (e.Code == CodeDeadline && target == context.DeadlineExceeded)
}

// A native cancellation may have been requested by the context deadline watcher.
// Reclassify only that rejection; successful/uncertain writes keep their outcome.
func executionError(code Code, message string, contextError error) error {
	if code == CodeOK {
		return nil
	}
	if code == CodeCancelled && errors.Is(contextError, context.DeadlineExceeded) {
		code = CodeDeadline
	}
	return &Error{Code: code, Message: message}
}

// Value is a lossless scalar. Integer and IDs never pass through float64.
type Value struct {
	kind    string
	integer int64
	real    float64
	text    string
	boolean bool
}

func Null() Value           { return Value{} }
func Integer(v int64) Value { return Value{kind: "Integer", integer: v} }
func Real(v float64) Value  { return Value{kind: "Real", real: v} }
func Text(v string) Value   { return Value{kind: "Text", text: v} }
func Boolean(v bool) Value  { return Value{kind: "Boolean", boolean: v} }
func (v Value) Kind() string {
	if v.kind == "" {
		return "Null"
	}
	return v.kind
}
func (v Value) Int64() (int64, bool)     { return v.integer, v.kind == "Integer" }
func (v Value) Float64() (float64, bool) { return v.real, v.kind == "Real" }
func (v Value) String() (string, bool)   { return v.text, v.kind == "Text" }
func (v Value) Bool() (bool, bool)       { return v.boolean, v.kind == "Boolean" }
func (v Value) MarshalJSON() ([]byte, error) {
	var x any
	switch v.Kind() {
	case "Null":
		return []byte(`"Null"`), nil
	case "Integer":
		x = v.integer
	case "Real":
		x = v.real
	case "Text":
		x = v.text
	case "Boolean":
		x = v.boolean
	default:
		return nil, errors.New("unknown native value kind")
	}
	b, e := json.Marshal(x)
	if e != nil {
		return nil, e
	}
	return append(append([]byte(`{"`+v.Kind()+`":`), b...), '}'), nil
}
func decodeVariant(b []byte) (string, json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	t, e := d.Token()
	if e != nil || t != json.Delim('{') {
		return "", nil, errors.New("expected tagged native object")
	}
	if !d.More() {
		return "", nil, errors.New("empty native variant")
	}
	t, e = d.Token()
	k, ok := t.(string)
	if e != nil || !ok {
		return "", nil, errors.New("invalid native tag")
	}
	var v json.RawMessage
	if e = d.Decode(&v); e != nil {
		return "", nil, e
	}
	if d.More() {
		return "", nil, errors.New("duplicate or multiple native tags")
	}
	if _, e = d.Token(); e != nil {
		return "", nil, e
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return "", nil, errors.New("trailing native data")
	}
	if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
		return "", nil, errors.New("null native payload")
	}
	return k, v, nil
}
func (v *Value) UnmarshalJSON(b []byte) error {
	if bytes.Equal(bytes.TrimSpace(b), []byte(`"Null"`)) {
		*v = Null()
		return nil
	}
	k, b, e := decodeVariant(b)
	if e != nil {
		return e
	}
	result := Value{kind: k}
	switch k {
	case "Integer":
		e = json.Unmarshal(b, &result.integer)
	case "Real":
		e = json.Unmarshal(b, &result.real)
	case "Text":
		e = json.Unmarshal(b, &result.text)
	case "Boolean":
		e = json.Unmarshal(b, &result.boolean)
	default:
		return errors.New("unknown native value kind")
	}
	if e == nil {
		*v = result
	}
	return e
}

type Vertex struct {
	ID         uint64           `json:"id"`
	Labels     []string         `json:"labels"`
	Properties map[string]Value `json:"properties"`
}

func (v Vertex) MarshalJSON() ([]byte, error) {
	type plain Vertex
	if v.Labels == nil {
		v.Labels = []string{}
	}
	if v.Properties == nil {
		v.Properties = map[string]Value{}
	}
	return json.Marshal(plain(v))
}

type Edge struct {
	ID         uint64           `json:"id"`
	From       uint64           `json:"from"`
	To         uint64           `json:"to"`
	Label      string           `json:"label"`
	Properties map[string]Value `json:"properties"`
}

func (v Edge) MarshalJSON() ([]byte, error) {
	type plain Edge
	if v.Properties == nil {
		v.Properties = map[string]Value{}
	}
	return json.Marshal(plain(v))
}

// Bytes has an ABI1 JSON integer-array encoding, not Go's default base64 encoding.
type Bytes []byte

func (b Bytes) MarshalJSON() ([]byte, error) {
	out := make([]byte, 0, len(b)*4+2)
	out = append(out, '[')
	for i, v := range b {
		if i > 0 {
			out = append(out, ',')
		}
		out = strconv.AppendUint(out, uint64(v), 10)
	}
	return append(out, ']'), nil
}

type MemoryLimits struct {
	MaxRecords              uint64 `json:"max_records,omitempty"`
	MaxAccountedBytes       uint64 `json:"max_accounted_bytes,omitempty"`
	MaxBatchOperations      uint64 `json:"max_batch_operations,omitempty"`
	MaxBatchBytes           uint64 `json:"max_batch_bytes,omitempty"`
	MaxPinnedVersions       uint64 `json:"max_pinned_versions,omitempty"`
	MaxPinnedAccountedBytes uint64 `json:"max_pinned_accounted_bytes,omitempty"`
}
type MemoryOptions struct {
	Identity    string
	Mode        string // "cow" (default) or "locked"
	Vertices    []Vertex
	Edges       []Edge
	Metadata    []byte
	RestorePath string
	Limits      *MemoryLimits
}
type Interval struct {
	From int64  `json:"from"`
	To   *int64 `json:"to"`
}
type Mutation struct {
	Op     string    `json:"op"`
	Vertex *Vertex   `json:"vertex,omitempty"`
	Edge   *Edge     `json:"edge,omitempty"`
	ID     uint64    `json:"id,omitempty"`
	Valid  *Interval `json:"valid,omitempty"`
}

func PutVertex(v Vertex) Mutation     { return Mutation{Op: "put_vertex", Vertex: &v} }
func PutEdge(e Edge) Mutation         { return Mutation{Op: "put_edge", Edge: &e} }
func DeleteVertex(id uint64) Mutation { return Mutation{Op: "delete_vertex", ID: id} }
func DeleteEdge(id uint64) Mutation   { return Mutation{Op: "delete_edge", ID: id} }

// MarshalJSON preserves ID zero on deletion while rejecting invalid field combinations.
func (m Mutation) MarshalJSON() ([]byte, error) {
	switch m.Op {
	case "put_vertex":
		if m.Vertex == nil || m.Edge != nil || m.ID != 0 {
			return nil, errors.New("invalid vertex mutation")
		}
		return json.Marshal(struct {
			Op     string    `json:"op"`
			Vertex *Vertex   `json:"vertex"`
			Valid  *Interval `json:"valid,omitempty"`
		}{m.Op, m.Vertex, m.Valid})
	case "put_edge":
		if m.Edge == nil || m.Vertex != nil || m.ID != 0 {
			return nil, errors.New("invalid edge mutation")
		}
		return json.Marshal(struct {
			Op    string    `json:"op"`
			Edge  *Edge     `json:"edge"`
			Valid *Interval `json:"valid,omitempty"`
		}{m.Op, m.Edge, m.Valid})
	case "delete_vertex", "delete_edge":
		if m.Vertex != nil || m.Edge != nil {
			return nil, errors.New("invalid deletion")
		}
		return json.Marshal(struct {
			Op    string    `json:"op"`
			ID    uint64    `json:"id"`
			Valid *Interval `json:"valid,omitempty"`
		}{m.Op, m.ID, m.Valid})
	default:
		return nil, errors.New("unknown mutation")
	}
}

type WriteBatch struct {
	ExpectedVersion uint64     `json:"expected_version"`
	Mutations       []Mutation `json:"mutations"`
	Metadata        Bytes      `json:"metadata,omitempty"`
}
type PrepareOptions struct {
	BindStartIDs bool
	ValueSteps   []uint64
}
type Query struct {
	Text     string
	Prepared *Prepared
	StartIDs []uint64
	Values   []Value
}
type QueryLimits struct {
	MaxQueries        uint64 `json:"max_queries,omitempty"`
	MaxRows           uint64 `json:"max_rows,omitempty"`
	MaxWorkOperations uint64 `json:"max_work_operations,omitempty"`
	MaxWorkBytes      uint64 `json:"max_work_bytes,omitempty"`
	MaxLiveBytes      uint64 `json:"max_live_bytes,omitempty"`
}
type QueryOptions struct {
	Version         *uint64
	ValidTimeMicros *int64
	Limits          *QueryLimits
	MaxOutputBytes  uint64
	Timeout         time.Duration
}
type Info struct {
	Mode              *string  `json:"mode"`
	MaxGraphBytes     *uint64  `json:"max_graph_bytes"`
	ABIVersion        uint32   `json:"abi_version"`
	Backend           string   `json:"backend"`
	Identity          *string  `json:"identity"`
	Version           uint64   `json:"version"`
	Metadata          []byte   `json:"metadata"`
	Capabilities      []string `json:"capabilities"`
	MaxBatchMutations uint64   `json:"max_batch_mutations"`
	MaxQueryWork      uint64   `json:"max_query_work"`
}
type PathElement struct {
	Kind string
	ID   uint64
}

func (p *PathElement) UnmarshalJSON(b []byte) error {
	k, b, e := decodeVariant(b)
	if e != nil {
		return e
	}
	if k != "Vertex" && k != "Edge" {
		return errors.New("unknown path element")
	}
	var id uint64
	if e = json.Unmarshal(b, &id); e != nil {
		return e
	}
	*p = PathElement{Kind: k, ID: id}
	return nil
}

type Row struct {
	Kind  string
	ID    uint64
	Count uint64
	Value Value
	Path  []PathElement
}

func (r *Row) UnmarshalJSON(b []byte) error {
	k, b, e := decodeVariant(b)
	if e != nil {
		return e
	}
	result := Row{Kind: k}
	switch k {
	case "Vertex", "Edge":
		e = json.Unmarshal(b, &result.ID)
	case "Count":
		e = json.Unmarshal(b, &result.Count)
	case "Value":
		e = json.Unmarshal(b, &result.Value)
	case "Path":
		e = json.Unmarshal(b, &result.Path)
	default:
		return errors.New("unknown native row")
	}
	if e == nil {
		*r = result
	}
	return e
}

type Result struct {
	Rows []Row `json:"rows"`
}
type BatchResult struct {
	Version  uint64   `json:"version"`
	Metadata []byte   `json:"metadata"`
	Results  []Result `json:"results"`
}

// Store owns a native store handle. Close retires it; admitted calls may finish.
type Store struct{ id atomic.Uint64 }

// Prepared owns an immutable, store-independent query plan.
type Prepared struct{ id atomic.Uint64 }

func (p *Prepared) Query(ids []uint64, values ...Value) Query {
	return Query{Prepared: p, StartIDs: ids, Values: values}
}

// RuntimeStats reports native reservations, not RSS. Quiesce calls for leak checks.
type RuntimeStats struct{ RegisteredHandles, WireReservedBytes, PlanReservedBytes, ActiveCalls uint64 }
