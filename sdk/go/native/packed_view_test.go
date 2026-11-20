//go:build cgo && velagraph_native

package native

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueryViewOwnsResultBeyondStoreAndClose(t *testing.T) {
	before, _ := Stats()
	ctx := context.Background()
	s, e := OpenMemory(ctx, MemoryOptions{Identity: "view", Vertices: []Vertex{{ID: 1, Properties: map[string]Value{"text": Text("图\x00🦀")}}}, Metadata: []byte{0, 255}})
	if e != nil {
		t.Fatal(e)
	}
	queries := []Query{{Text: "g.V(1).values('text')"}, {Text: "g.V(2)"}}
	expected, e := jsonReference(s, queries, QueryOptions{})
	if e != nil {
		t.Fatal(e)
	}
	v, e := s.QueryView(ctx, queries, QueryOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	s.Close()
	got, e := v.Materialize()
	if e != nil || !reflect.DeepEqual(got, expected) {
		t.Fatal(got, expected, e)
	}
	if e = v.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = v.Materialize(); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatal("owned data changed after Close")
	}
	after, _ := Stats()
	if before != after {
		t.Fatal(before, after)
	}
}

// Preserve the ABI1 JSON decoder as an independent wire oracle for packed results.
func jsonReference(s *Store, qs []Query, o QueryOptions) (BatchResult, error) {
	queries := make([]map[string]any, len(qs))
	for i, q := range qs {
		queries[i] = map[string]any{"text": q.Text}
	}
	request := map[string]any{"queries": queries}
	if o.ValidTimeMicros != nil {
		request["valid_time_micros"] = *o.ValidTimeMicros
	}
	if o.Version != nil {
		request["version"] = *o.Version
	}
	id, e := s.handle()
	if e != nil {
		return BatchResult{}, e
	}
	_, data, e := invoke(context.Background(), opQuery, id, request, 0, DefaultOutputBytes)
	if e != nil {
		return BatchResult{}, e
	}
	var r BatchResult
	e = json.Unmarshal(data, &r)
	return r, e
}
func TestPackedAllKindsMatchJSON(t *testing.T) {
	ctx := context.Background()
	props := map[string]Value{"i": Integer(math.MinInt64), "r": Real(math.Copysign(0, -1)), "b": Boolean(true), "n": Null(), "t": Text("图\x00🦀"), "empty": Text("")}
	s, e := OpenMemory(ctx, MemoryOptions{Identity: "all-kinds", Vertices: []Vertex{{ID: 1, Properties: props}, {ID: 2}, {ID: math.MaxUint64}}, Edges: []Edge{{ID: 7, From: 1, To: 2, Label: "L"}}, Metadata: []byte{0, 255}})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	qs := []Query{{Text: "g.V()"}, {Text: "g.E(7)"}, {Text: "g.V(1).out('L').path()"}, {Text: "g.V().count()"}, {Text: "g.V(99)"}}
	for _, key := range []string{"i", "r", "b", "n", "t", "empty"} {
		qs = append(qs, Query{Text: "g.V(1).values('" + key + "')"})
	}
	qs = append(qs, Query{Text: "g.V(1,1)"})
	expected, e := jsonReference(s, qs, QueryOptions{})
	if e != nil {
		t.Fatal(e)
	}
	v, e := s.QueryView(ctx, qs, QueryOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	got, e := v.Materialize()
	if e != nil || !reflect.DeepEqual(got, expected) {
		t.Fatal(got, expected, e)
	}
	integer, _ := v.Row(5, 0)
	if n, e := integer.Int64(); e != nil || n != math.MinInt64 {
		t.Fatal(n, e)
	}
	real, _ := v.Row(6, 0)
	if n, e := real.Float64(); e != nil || math.Float64bits(n) != math.Float64bits(math.Copysign(0, -1)) {
		t.Fatal(n, e)
	}
	boolean, _ := v.Row(7, 0)
	if n, e := boolean.Bool(); e != nil || !n {
		t.Fatal(n, e)
	}
	null, _ := v.Row(8, 0)
	if k, e := null.Kind(); e != nil || k != KindNull {
		t.Fatal(k, e)
	}
	text, _ := v.Row(9, 0)
	if match, e := text.TextEquals("图\x00🦀"); e != nil || !match {
		t.Fatal(match, e)
	}
	if match, e := text.TextEquals("different"); e != nil || match {
		t.Fatal(match, e)
	}
	owned, e := text.Text()
	if e != nil {
		t.Fatal(e)
	}
	path, _ := v.Row(2, 0)
	if n, e := path.PathLen(); e != nil || n != 3 {
		t.Fatal(n, e)
	}
	for i, want := range []PathElement{{Kind: "Vertex", ID: 1}, {Kind: "Edge", ID: 7}, {Kind: "Vertex", ID: 2}} {
		if got, e := path.PathElement(i); e != nil || got != want {
			t.Fatal(got, e)
		}
	}
	vertex, _ := v.Row(0, 2)
	if id, e := vertex.ID(); e != nil || id != math.MaxUint64 {
		t.Fatal(id, e)
	}
	count, _ := v.Row(3, 0)
	if n, e := count.Count(); e != nil || n != 3 {
		t.Fatal(n, e)
	}
	if _, e := count.Text(); e == nil {
		t.Fatal("wrong type accepted")
	}
	if _, e := v.Row(4, 0); e == nil {
		t.Fatal("empty result index accepted")
	}
	if _, e := path.PathElement(3); e == nil {
		t.Fatal("path bound accepted")
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		_, _ = integer.Int64()
		_, _ = vertex.ID()
		_, _ = text.TextEquals("图\x00🦀")
		_, _ = path.PathElement(1)
	}); allocations != 0 {
		t.Fatalf("view access allocated %v", allocations)
	}
	v.Close()
	if owned != "图\x00🦀" {
		t.Fatal(owned)
	}
	for _, row := range []RowView{integer, text, path, vertex, count} {
		if _, e := row.Kind(); !errors.Is(e, ErrClosed) {
			t.Fatal(e)
		}
	}
}
func TestViewConcurrentCloseRetainsNoNativeResources(t *testing.T) {
	before, _ := Stats()
	ctx := context.Background()
	s, e := OpenMemory(ctx, MemoryOptions{Identity: "view-race", Vertices: []Vertex{{ID: 1, Properties: map[string]Value{"s": Text("shared"), "n": Integer(1)}}}})
	if e != nil {
		t.Fatal(e)
	}
	for round := 0; round < 50; round++ {
		v, e := s.QueryView(ctx, []Query{{Text: "g.V(1).values('s')"}, {Text: "g.V(1).values('n')"}, {Text: "g.V(1).path()"}}, QueryOptions{})
		if e != nil {
			t.Fatal(e)
		}
		row, _ := v.Row(0, 0)
		integer, _ := v.Row(1, 0)
		path, _ := v.Row(2, 0)
		alias := *v
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for j := 0; j < 50; j++ {
					ok, e := row.TextEquals("shared")
					if e != nil && !errors.Is(e, ErrClosed) {
						t.Error(e)
					}
					if e == nil && !ok {
						t.Error("torn payload")
					}
					if n, e := integer.Int64(); e == nil && n != 1 {
						t.Error("torn integer")
					} else if e != nil && !errors.Is(e, ErrClosed) {
						t.Error(e)
					}
					if p, e := path.PathElement(0); e == nil && p.ID != 1 {
						t.Error("torn path")
					} else if e != nil && !errors.Is(e, ErrClosed) {
						t.Error(e)
					}
					if r, e := v.Materialize(); e == nil && len(r.Results) != 3 {
						t.Error("torn result")
					} else if e != nil && !errors.Is(e, ErrClosed) {
						t.Error(e)
					}

				}
			}()
		}
		close(start)
		if e := alias.Close(); e != nil {
			t.Fatal(e)
		}
		wg.Wait()
		v.Close()
	}
	s.Close()
	after, _ := Stats()
	if before != after {
		t.Fatal(before, after)
	}
}
func TestTemporalViewKeepsCapturedDataAndMatchesJSON(t *testing.T) {
	ctx := context.Background()
	s, e := OpenTemporal(ctx, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	m := PutVertex(Vertex{ID: 1, Properties: map[string]Value{"n": Integer(1)}})
	m.Valid = &Interval{From: 0}
	if _, e = s.Apply(ctx, WriteBatch{Mutations: []Mutation{m}}); e != nil {
		t.Fatal(e)
	}
	at := int64(0)
	qs := []Query{{Text: "g.V(1).values('n')"}}
	options := QueryOptions{ValidTimeMicros: &at}
	expected, e := jsonReference(s, qs, options)
	if e != nil {
		t.Fatal(e)
	}
	v, e := s.QueryView(ctx, qs, options)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	m.Vertex.Properties["n"] = Integer(2)
	if _, e = s.Apply(ctx, WriteBatch{ExpectedVersion: 1, Mutations: []Mutation{m}}); e != nil {
		t.Fatal(e)
	}
	s.Close()
	got, e := v.Materialize()
	if e != nil || !reflect.DeepEqual(got, expected) || got.Metadata != nil {
		t.Fatal(got, expected, e)
	}
}

func TestViewCloseWaitsForAnAdmittedNativeRead(t *testing.T) {
	before, _ := Stats()
	ctx := context.Background()
	s, e := OpenMemory(ctx, MemoryOptions{Identity: "held-read", Vertices: []Vertex{{ID: 1}}})
	if e != nil {
		t.Fatal(e)
	}
	v, e := s.QueryView(ctx, []Query{{Text: "g.V(1)"}}, QueryOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	s.Close()
	var releases atomic.Int32
	release := v.owner.release
	v.owner.release = func() error { releases.Add(1); return release() }
	v.owner.mu.RLock()
	held := true
	defer func() {
		if held {
			v.owner.mu.RUnlock()
		}
	}()
	done := make(chan error, 1)
	go func() { done <- v.Close() }()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case e := <-done:
			t.Fatalf("Close completed during admitted read: %v", e)
		case <-timer.C:
			t.Fatal("Close did not reach owner lock")
		default:
		}
		if !v.owner.mu.TryRLock() {
			break
		}
		v.owner.mu.RUnlock()
		runtime.Gosched()
	}
	// A pending writer has reached the lock. The admitted read still owns live C memory.
	if releases.Load() != 0 {
		t.Fatal("native lease released under reader")
	}
	if packedWord(v.owner.data, v.owner.rowStart+8) != 1 {
		t.Fatal("native bytes changed")
	}
	during, _ := Stats()
	if during.WireReservedBytes <= before.WireReservedBytes {
		t.Fatal("buffer reservation released")
	}
	held = false
	v.owner.mu.RUnlock()
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if releases.Load() != 1 {
		t.Fatal("incorrect release count")
	}
	v.Close()
	if releases.Load() != 1 {
		t.Fatal("double release")
	}
	after, _ := Stats()
	if before != after {
		t.Fatal(before, after)
	}
}
