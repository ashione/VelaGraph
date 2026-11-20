//go:build cgo && velagraph_native

package native

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"
)

func TestNativeRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, e := OpenMemory(ctx, MemoryOptions{Identity: "go-test", Vertices: []Vertex{{ID: math.MaxInt64, Properties: map[string]Value{"score": Integer(math.MaxInt64)}}}})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	p, e := Prepare(ctx, "g.V(1).values('score')", PrepareOptions{BindStartIDs: true})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	r, e := s.Query(ctx, []Query{p.Query([]uint64{math.MaxInt64})}, QueryOptions{})
	if e != nil {
		t.Fatal(e)
	}
	n, ok := r.Results[0].Rows[0].Value.Int64()
	if !ok || n != math.MaxInt64 {
		t.Fatalf("%+v", r)
	}
	v, e := s.Apply(ctx, WriteBatch{ExpectedVersion: 0, Mutations: []Mutation{}, Metadata: Bytes{0, 255}})
	if e != nil || v != 1 {
		t.Fatal(v, e)
	}
	path := filepath.Join(t.TempDir(), "graph.bin")
	if e = s.Checkpoint(ctx, path); e != nil {
		t.Fatal(e)
	}
	restored, e := OpenMemory(ctx, MemoryOptions{Identity: "go-test", RestorePath: path})
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close()
	info, e := restored.Info(ctx)
	if e != nil || info.Version != 1 || len(info.Metadata) != 2 {
		t.Fatal(info, e)
	}
}

func TestTemporalAndCancellation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "temporal")
	s, e := OpenTemporal(ctx, path)
	if e != nil {
		t.Fatal(e)
	}
	end := int64(20)
	m := PutVertex(Vertex{ID: 1})
	m.Valid = &Interval{From: 10, To: &end}
	if v, e := s.Apply(ctx, WriteBatch{ExpectedVersion: 0, Mutations: []Mutation{m}}); e != nil || v != 1 {
		t.Fatal(v, e)
	}
	for _, tc := range []struct {
		at    int64
		count int
	}{{9, 0}, {10, 1}, {20, 0}} {
		r, e := s.Query(ctx, []Query{{Text: "g.V(1)"}}, QueryOptions{ValidTimeMicros: &tc.at})
		if e != nil || len(r.Results[0].Rows) != tc.count {
			t.Fatal(r, e)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e = s.Apply(cancelled, WriteBatch{ExpectedVersion: 1}); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if e = s.Checkpoint(ctx, filepath.Join(t.TempDir(), "unsupported")); e == nil {
		t.Fatal("temporal checkpoint unexpectedly supported")
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = OpenTemporal(ctx, path)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	info, e := s.Info(ctx)
	if e != nil || info.Version != 1 {
		t.Fatal(info, e)
	}
}

func TestConcurrentQueriesWritesAndResourceRelease(t *testing.T) {
	ctx := context.Background()
	before, e := Stats()
	if e != nil {
		t.Fatal(e)
	}
	s, e := OpenMemory(ctx, MemoryOptions{Identity: "concurrent", Vertices: []Vertex{{ID: 1, Properties: map[string]Value{"generation": Integer(0)}}}})
	if e != nil {
		t.Fatal(e)
	}
	p, e := Prepare(ctx, "g.V(1).values('generation')", PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r, e := s.Query(ctx, []Query{p.Query(nil), p.Query(nil)}, QueryOptions{})
				if e != nil {
					errs <- e
					return
				}
				for _, q := range r.Results {
					n, ok := q.Rows[0].Value.Int64()
					if !ok || uint64(n) != r.Version {
						errs <- fmt.Errorf("mixed snapshot %+v", r)
						return
					}
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(1); i <= 100; i++ {
			if v, e := s.Apply(ctx, WriteBatch{ExpectedVersion: i - 1, Mutations: []Mutation{PutVertex(Vertex{ID: 1, Properties: map[string]Value{"generation": Integer(int64(i))}})}}); e != nil || v != i {
				errs <- fmt.Errorf("write %d/%d: %v", i, v, e)
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if e = p.Close(); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	after, e := Stats()
	if e != nil || after != before {
		t.Fatalf("resources: before=%+v after=%+v err=%v", before, after, e)
	}
}

func TestCloseRacesQueriesWithoutUseAfterFree(t *testing.T) {
	ctx := context.Background()
	before, _ := Stats()
	s, e := OpenMemory(ctx, MemoryOptions{Identity: "close", Vertices: []Vertex{{ID: 1}}})
	if e != nil {
		t.Fatal(e)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 100; j++ {
				_, e := s.Query(ctx, []Query{{Text: "g.V(1)"}}, QueryOptions{})
				var ne *Error
				if e != nil && !errors.Is(e, ErrClosed) && !(errors.As(e, &ne) && ne.Code == CodeInvalidHandle) {
					errs <- e
					return
				}
			}
		}()
	}
	close(start)
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	after, _ := Stats()
	if after != before {
		t.Fatalf("resources before=%+v after=%+v", before, after)
	}
}

func TestLosslessNativeValuesAndZeroDeletion(t *testing.T) {
	ctx := context.Background()
	props := map[string]Value{"min": Integer(math.MinInt64), "zero": Real(math.Copysign(0, -1)), "real": Real(51.248178375505404), "text": Text("图🦀\x00"), "nil": Null(), "bool": Boolean(true)}
	s, e := OpenMemory(ctx, MemoryOptions{Identity: "numbers", Vertices: []Vertex{{ID: math.MaxUint64, Properties: props}, {ID: 0}}, Metadata: []byte{9}})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	r, e := s.Query(ctx, []Query{{Text: "g.V()"}}, QueryOptions{})
	if e != nil || len(r.Results[0].Rows) != 2 || r.Results[0].Rows[1].ID != math.MaxUint64 {
		t.Fatal(r, e)
	}
	for key, expected := range props {
		r, e := s.Query(ctx, []Query{{Text: fmt.Sprintf("g.V().values('%s')", key)}}, QueryOptions{})
		if e != nil || len(r.Results[0].Rows) != 1 {
			t.Fatal(r, e)
		}
		got := r.Results[0].Rows[0].Value
		if got != expected || (key == "zero" && !math.Signbit(got.real)) {
			t.Fatalf("%s: %+v != %+v", key, got, expected)
		}
	}
	if _, e = s.Apply(ctx, WriteBatch{ExpectedVersion: 0, Mutations: []Mutation{DeleteVertex(0)}}); e != nil {
		t.Fatal(e)
	}
	info, e := s.Info(ctx)
	if e != nil || len(info.Metadata) != 0 {
		t.Fatal(info, e)
	}
	r, e = s.Query(ctx, []Query{{Text: "g.V()"}}, QueryOptions{})
	if e != nil || len(r.Results[0].Rows) != 1 || r.Results[0].Rows[0].ID != math.MaxUint64 {
		t.Fatal(r, e)
	}
	p, e := Prepare(ctx, "g.V(1)", PrepareOptions{BindStartIDs: true})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	for _, ids := range [][]uint64{nil, {}, {math.MaxUint64}} {
		if _, e = s.Query(ctx, []Query{p.Query(ids)}, QueryOptions{}); e == nil {
			t.Fatal("invalid binding accepted", ids)
		}
	}
}

func TestPreparedCloseAndContextRacesReleaseResources(t *testing.T) {
	ctx := context.Background()
	before, e := Stats()
	if e != nil {
		t.Fatal(e)
	}
	s, e := OpenMemory(ctx, MemoryOptions{Identity: "plan-close", Vertices: []Vertex{{ID: 1}}})
	if e != nil {
		t.Fatal(e)
	}
	p, e := Prepare(ctx, "g.V(1)", PrepareOptions{})
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 50; j++ {
				c, cancel := context.WithCancel(ctx)
				var done sync.WaitGroup
				done.Add(1)
				go func() { defer done.Done(); cancel() }()
				_, e := s.Query(c, []Query{p.Query(nil)}, QueryOptions{})
				done.Wait()
				var ne *Error
				if e != nil && !errors.Is(e, context.Canceled) && !errors.Is(e, ErrClosed) && !(errors.As(e, &ne) && ne.Code == CodeInvalidHandle) {
					t.Error(e)
					return
				}
			}
		}()
	}
	close(start)
	if e = p.Close(); e != nil {
		t.Fatal(e)
	}
	wg.Wait()
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	after, e := Stats()
	if e != nil || before != after {
		t.Fatal(before, after, e)
	}
}

func TestNativeEdgePathAndCountRows(t *testing.T) {
	ctx := context.Background()
	s, e := OpenMemory(ctx, MemoryOptions{Identity: "row-shapes", Vertices: []Vertex{{ID: 1}, {ID: 2}}, Edges: []Edge{{ID: 7, From: 1, To: 2, Label: "L"}}})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	r, e := s.Query(ctx, []Query{{Text: "g.E(7)"}, {Text: "g.V(1).out('L').path()"}, {Text: "g.V(1).out('L').count()"}}, QueryOptions{})
	if e != nil {
		t.Fatal(e)
	}
	if r.Results[0].Rows[0].Kind != "Edge" || r.Results[0].Rows[0].ID != 7 {
		t.Fatal(r)
	}
	path := r.Results[1].Rows[0].Path
	expected := []PathElement{{Kind: "Vertex", ID: 1}, {Kind: "Edge", ID: 7}, {Kind: "Vertex", ID: 2}}
	if len(path) != len(expected) {
		t.Fatal(path)
	}
	for i := range path {
		if path[i] != expected[i] {
			t.Fatal(path)
		}
	}
	if r.Results[2].Rows[0].Kind != "Count" || r.Results[2].Rows[0].Count != 1 {
		t.Fatal(r)
	}
}
