//go:build cgo && velagraph_native

package native

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkNativePrepared(b *testing.B) {
	for _, count := range []int{1, 16, 256} {
		b.Run(fmt.Sprintf("rows%d", count), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			vertices := make([]Vertex, count)
			for i := range vertices {
				vertices[i] = Vertex{ID: uint64(i + 1), Properties: map[string]Value{"n": Integer(int64(i + 1))}}
			}
			// Global start permits a generated scan above the explicit-start-ID ceiling.
			store, e := OpenMemory(ctx, MemoryOptions{Identity: "native-bench", Vertices: vertices})
			if e != nil {
				b.Fatal(e)
			}
			defer store.Close()
			plan, e := Prepare(ctx, fmt.Sprintf("g.V().values('n').limit(%d)", count), PrepareOptions{})
			if e != nil {
				b.Fatal(e)
			}
			defer plan.Close()
			queries := []Query{plan.Query(nil)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, e := store.Query(ctx, queries, QueryOptions{})
				if e != nil {
					b.Fatal(e)
				}
				if len(result.Results[0].Rows) != count {
					b.Fatal("rows")
				}
			}
		})
	}
}

// Includes numeric consumption and Close, so a view cannot appear faster by
// skipping the work of its consumer or deferring resource release past timing.
func BenchmarkNativeView(b *testing.B) {
	for _, count := range []int{1, 16, 256} {
		b.Run(fmt.Sprintf("rows%d", count), func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			vertices := make([]Vertex, count)
			for i := range vertices {
				vertices[i] = Vertex{ID: uint64(i + 1), Properties: map[string]Value{"n": Integer(int64(i + 1))}}
			}
			store, e := OpenMemory(ctx, MemoryOptions{Identity: "native-bench", Vertices: vertices})
			if e != nil {
				b.Fatal(e)
			}
			defer store.Close()
			plan, e := Prepare(ctx, fmt.Sprintf("g.V().values('n').limit(%d)", count), PrepareOptions{})
			if e != nil {
				b.Fatal(e)
			}
			defer plan.Close()
			qs := []Query{plan.Query(nil)}
			want := int64(count * (count + 1) / 2)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v, e := store.QueryView(ctx, qs, QueryOptions{})
				if e != nil {
					b.Fatal(e)
				}
				n, e := v.RowCount(0)
				if e != nil || n != count {
					b.Fatal(n, e)
				}
				var sum int64
				for j := 0; j < n; j++ {
					row, e := v.Row(0, j)
					if e != nil {
						b.Fatal(e)
					}
					value, e := row.Int64()
					if e != nil {
						b.Fatal(e)
					}
					sum += value
				}
				if e := v.Close(); e != nil {
					b.Fatal(e)
				}
				if sum != want {
					b.Fatal(sum, want)
				}
			}
		})
	}
}

// BenchmarkNativeTemporalRevisions crosses the packed native ABI for every
// query. FirstRead opens a new store per iteration to reset its revision cache;
// it leaves the operating system's file cache alone. Open and Close are untimed.
func BenchmarkNativeTemporalRevisions(b *testing.B) {
	ctx := context.Background()
	path := b.TempDir()
	const version uint64 = 512
	writer, err := OpenTemporal(ctx, path)
	if err != nil {
		b.Fatal(err)
	}
	defer writer.Close()
	for n := uint64(0); n < version; n++ {
		end := int64(300)
		if n == version-1 {
			end = 200
		}
		m := PutVertex(Vertex{ID: 1, Properties: map[string]Value{"n": Integer(int64(n))}})
		m.Valid = &Interval{From: 0, To: &end}
		if v, err := writer.Apply(ctx, WriteBatch{ExpectedVersion: n, Mutations: []Mutation{m}}); err != nil || v != n+1 {
			b.Fatalf("commit %d: got %d, %v", n+1, v, err)
		}
	}
	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}
	plan, err := Prepare(ctx, "g.V(1).values('n')", PrepareOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer plan.Close()
	queries := []Query{plan.Query(nil)}
	open := func(b *testing.B) *Store {
		s, err := OpenTemporal(ctx, path)
		if err != nil {
			b.Fatal(err)
		}
		info, err := s.Info(ctx)
		if err != nil || info.Backend != "temporal" || info.Version != version {
			s.Close()
			b.Fatalf("reopened backend: %+v, %v", info, err)
		}
		return s
	}
	head := version
	for _, tc := range []struct {
		name     string
		at, want int64
	}{{"Newest", 150, 511}, {"Previous", 255, 510}} {
		opts := QueryOptions{Version: &head, ValidTimeMicros: &tc.at}
		read := func(b *testing.B, s *Store) {
			r, err := s.Query(ctx, queries, opts)
			if err != nil || r.Version != version || len(r.Results) != 1 || len(r.Results[0].Rows) != 1 {
				b.Fatal(r, err)
			}
			n, ok := r.Results[0].Rows[0].Value.Int64()
			if !ok || n != tc.want {
				b.Fatalf("got %d, want %d", n, tc.want)
			}
		}
		b.Run("Warm/"+tc.name, func(b *testing.B) {
			s := open(b)
			defer s.Close()
			// A miss before all intervals populates the overlapping revision prefix.
			before := int64(-1)
			r, err := s.Query(ctx, queries, QueryOptions{Version: &head, ValidTimeMicros: &before})
			if err != nil || r.Version != version || len(r.Results) != 1 || len(r.Results[0].Rows) != 0 {
				b.Fatal(r, err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				read(b, s)
			}
			b.StopTimer()
		})
		b.Run("FirstRead/"+tc.name, func(b *testing.B) {
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				s := open(b)
				b.StartTimer()
				read(b, s)
				b.StopTimer()
				if err := s.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
