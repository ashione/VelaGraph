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
