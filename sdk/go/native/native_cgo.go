//go:build cgo && velagraph_native

package native

/*
#cgo LDFLAGS: -lvelagraph_ffi
#include "velagraph.h"
*/
import "C"
import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"time"
	"unsafe"
)

const (
	opOpen        = 1
	opApply       = 2
	opQuery       = 3
	opInfo        = 4
	opCheckpoint  = 5
	opPrepare     = 6
	opQueryPacked = 7
)

var admission = make(chan struct{}, 16)

func Available() bool { return uint32(C.vg_abi_version()) == ABIVersion }
func statusError(s C.uint32_t, msg string) error {
	if s == 0 {
		return nil
	}
	return &Error{Code: Code(s), Message: msg}
}
func invokeRaw(ctx context.Context, op uint32, handle uint64, payload any, timeout time.Duration, maxOutput uint64) (uint64, uint64, error) {
	if e := ctx.Err(); e != nil {
		return 0, 0, e
	}
	if !Available() {
		return 0, 0, ErrUnavailable
	}
	select {
	case admission <- struct{}{}:
		defer func() { <-admission }()
	default:
		return 0, 0, &Error{Code: CodeBusy, Message: "Go native work admission exceeded"}
	}
	if timeout < 0 {
		return 0, 0, &Error{Code: CodeInvalidArgument, Message: "negative timeout"}
	}
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	if parent, ok := ctx.Deadline(); ok && (deadline.IsZero() || parent.Before(deadline)) {
		deadline = parent
	}
	var encoded []byte
	var e error
	if payload != nil {
		encoded, e = json.Marshal(payload)
		if e != nil {
			return 0, 0, fmt.Errorf("encode native request: %w", e)
		}
	}
	if len(encoded) > MaxInputBytes {
		return 0, 0, &Error{Code: CodeLimit, Message: "encoded request exceeds input ceiling"}
	}
	if e = ctx.Err(); e != nil {
		return 0, 0, e
	}
	var millis uint64
	if !deadline.IsZero() {
		left := time.Until(deadline)
		if left <= 0 {
			return 0, 0, context.DeadlineExceeded
		}
		if left > time.Minute {
			left = time.Minute
		}
		millis = uint64((left + time.Millisecond - 1) / time.Millisecond)
	}
	var token C.uint64_t
	if ctx.Done() != nil {
		if s := C.vg_control_new(&token); s != 0 {
			return 0, 0, statusError(s, "create cancellation control")
		}
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			select {
			case <-ctx.Done():
				C.vg_cancel(token)
			case <-stop:
			}
		}()
		defer func() { close(stop); <-done; C.vg_release(token) }()
	}
	var reply C.vg_reply
	status := C.vg_call(C.uint32_t(ABIVersion), C.uint32_t(op), C.uint64_t(handle), (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(encoded))), C.uint64_t(len(encoded)), token, C.uint64_t(millis), C.uint64_t(maxOutput), &reply)
	runtime.KeepAlive(encoded)
	// Do not overwrite a commit acknowledgment/outcome-unknown with ctx.Err().
	if status != 0 {
		return 0, 0, executionError(Code(status), C.GoString(&reply.error[0]), ctx.Err())
	}
	return uint64(reply.value), uint64(reply.buffer), nil
}
func invoke(ctx context.Context, op uint32, handle uint64, payload any, timeout time.Duration, maxOutput uint64) (uint64, []byte, error) {
	value, buffer, e := invokeRaw(ctx, op, handle, payload, timeout, maxOutput)
	if e != nil {
		return 0, nil, e
	}
	if buffer == 0 {
		return value, nil, nil
	}
	defer C.vg_release(C.uint64_t(buffer))
	var size C.uint64_t
	if s := C.vg_buffer_size(C.uint64_t(buffer), &size); s != 0 {
		return value, nil, statusError(s, "read native result size")
	}
	if uint64(size) > MaxOutputBytes {
		return value, nil, &Error{Code: CodeCorrupt, Message: "invalid native result length"}
	}
	out := make([]byte, int(size))
	if s := C.vg_buffer_copy(C.uint64_t(buffer), (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(out))), size); s != 0 {
		return value, nil, statusError(s, "copy native result")
	}
	runtime.KeepAlive(out)
	return value, out, nil
}
func OpenMemory(ctx context.Context, o MemoryOptions) (*Store, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	if e := checkMemory(o); e != nil {
		return nil, e
	}
	mode := o.Mode
	if mode == "" {
		mode = "cow"
	}
	vertices := o.Vertices
	if vertices == nil {
		vertices = []Vertex{}
	}
	edges := o.Edges
	if edges == nil {
		edges = []Edge{}
	}
	request := struct {
		Backend  string        `json:"backend"`
		Mode     string        `json:"mode"`
		Identity string        `json:"identity"`
		Vertices []Vertex      `json:"vertices"`
		Edges    []Edge        `json:"edges"`
		Metadata Bytes         `json:"metadata"`
		Restore  string        `json:"restore,omitempty"`
		Limits   *MemoryLimits `json:"limits,omitempty"`
	}{"memory", mode, o.Identity, vertices, edges, Bytes(o.Metadata), o.RestorePath, o.Limits}
	id, _, e := invoke(ctx, opOpen, 0, request, 0, 0)
	if e != nil {
		return nil, e
	}
	s := &Store{}
	s.id.Store(id)
	return s, nil
}
func OpenTemporal(ctx context.Context, path string) (*Store, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	if e := checkString(path); e != nil {
		return nil, e
	}
	id, _, e := invoke(ctx, opOpen, 0, struct {
		Backend string `json:"backend"`
		Path    string `json:"path"`
	}{"temporal", path}, 0, 0)
	if e != nil {
		return nil, e
	}
	s := &Store{}
	s.id.Store(id)
	return s, nil
}
func Prepare(ctx context.Context, text string, o PrepareOptions) (*Prepared, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	if len(text) > 16*1024 || len(o.ValueSteps) > 64 {
		return nil, &Error{Code: CodeLimit, Message: "prepared query size limit"}
	}
	if e := checkString(text); e != nil {
		return nil, e
	}
	steps := o.ValueSteps
	if steps == nil {
		steps = []uint64{}
	}
	request := struct {
		Text  string   `json:"text"`
		Bind  bool     `json:"bind_start_ids"`
		Steps []uint64 `json:"value_steps"`
	}{text, o.BindStartIDs, steps}
	id, _, e := invoke(ctx, opPrepare, 0, request, 0, 0)
	if e != nil {
		return nil, e
	}
	p := &Prepared{}
	p.id.Store(id)
	return p, nil
}
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	id := s.id.Swap(0)
	if id == 0 {
		return nil
	}
	return statusError(C.vg_release(C.uint64_t(id)), "close native store")
}
func (p *Prepared) Close() error {
	if p == nil {
		return nil
	}
	id := p.id.Swap(0)
	if id == 0 {
		return nil
	}
	return statusError(C.vg_release(C.uint64_t(id)), "close prepared plan")
}
func (s *Store) handle() (uint64, error) {
	if s == nil {
		return 0, ErrClosed
	}
	id := s.id.Load()
	if id == 0 {
		return 0, ErrClosed
	}
	return id, nil
}
func (s *Store) Info(ctx context.Context) (Info, error) {
	id, e := s.handle()
	if e != nil {
		return Info{}, e
	}
	_, out, e := invoke(ctx, opInfo, id, nil, 0, DefaultOutputBytes)
	if e != nil {
		return Info{}, e
	}
	var info Info
	e = json.Unmarshal(out, &info)
	return info, e
}
func (s *Store) Apply(ctx context.Context, b WriteBatch) (uint64, error) {
	id, e := s.handle()
	if e != nil {
		return 0, e
	}
	if e = checkBatch(b); e != nil {
		return 0, e
	}
	if b.Mutations == nil {
		b.Mutations = []Mutation{}
	}
	v, _, e := invoke(ctx, opApply, id, b, 0, 0)
	return v, e
}
func (s *Store) Checkpoint(ctx context.Context, path string) error {
	id, e := s.handle()
	if e != nil {
		return e
	}
	if e = checkString(path); e != nil {
		return e
	}
	_, _, e = invoke(ctx, opCheckpoint, id, struct {
		Path string `json:"path"`
	}{path}, 0, 0)
	return e
}
func (s *Store) QueryView(ctx context.Context, queries []Query, o QueryOptions) (*BatchView, error) {
	id, e := s.handle()
	if e != nil {
		return nil, e
	}
	if e = checkQueries(queries); e != nil {
		return nil, e
	}
	type wireQuery struct {
		Text     *string   `json:"text,omitempty"`
		Prepared *uint64   `json:"prepared,omitempty"`
		IDs      *[]uint64 `json:"start_ids,omitempty"`
		Values   []Value   `json:"values,omitempty"`
	}
	qs := make([]wireQuery, 0, len(queries))
	for _, q := range queries {
		w := wireQuery{Values: q.Values}
		if q.StartIDs != nil {
			w.IDs = &q.StartIDs
		}
		if q.Prepared != nil {
			p := q.Prepared.id.Load()
			if p == 0 {
				return nil, ErrClosed
			}
			w.Prepared = &p
		} else {
			w.Text = &q.Text
		}
		qs = append(qs, w)
	}
	request := struct {
		Queries []wireQuery  `json:"queries"`
		Version *uint64      `json:"version,omitempty"`
		Time    *int64       `json:"valid_time_micros,omitempty"`
		Limits  *QueryLimits `json:"limits,omitempty"`
	}{qs, o.Version, o.ValidTimeMicros, o.Limits}
	version, buffer, e := invokeRaw(ctx, opQueryPacked, id, request, o.Timeout, o.MaxOutputBytes)
	if e != nil {
		return nil, e
	}
	defer C.vg_release(C.uint64_t(buffer))
	var lease C.vg_buffer_view
	if code := C.vg_buffer_acquire(C.uint64_t(buffer), &lease); code != 0 {
		return nil, statusError(code, "acquire result lease")
	}
	release := func() error { return statusError(C.vg_release(lease.lease), "close result lease") }
	if uint64(lease.len) > MaxOutputBytes || lease.data == nil {
		release()
		return nil, corruptPacked()
	}
	data := unsafe.Slice((*byte)(unsafe.Pointer(lease.data)), int(lease.len))
	view, e := newBatchView(data, release)
	if e != nil {
		release()
		return nil, e
	}
	if packedWord(data, 8) != version {
		view.Close()
		return nil, corruptPacked()
	}
	return view, nil
}

func (s *Store) Query(ctx context.Context, queries []Query, o QueryOptions) (BatchResult, error) {
	view, e := s.QueryView(ctx, queries, o)
	if e != nil {
		return BatchResult{}, e
	}
	defer view.Close()
	return view.Materialize()
}
func Stats() (RuntimeStats, error) {
	var s C.vg_stats
	code := C.vg_runtime_stats(&s)
	if code != 0 {
		return RuntimeStats{}, statusError(code, "read native statistics")
	}
	return RuntimeStats{uint64(s.registered_handles), uint64(s.wire_reserved_bytes), uint64(s.plan_reserved_bytes), uint64(s.active_calls)}, nil
}
