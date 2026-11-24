// Generated benchmark and embedding example. No external data or service needed.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	native "github.com/ashione/VelaGraph/sdk/go/native"
	"os"
	"sort"
	"time"
)

const children = 256
const warmup = 100
const rounds = 1000

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vertices := make([]native.Vertex, children+1)
	edges := make([]native.Edge, children)
	for i := range vertices {
		id := uint64(i + 1)
		vertices[i] = native.Vertex{ID: id, Properties: map[string]native.Value{"n": native.Integer(int64(id))}}
	}
	for i := range edges {
		id := uint64(i + 2)
		edges[i] = native.Edge{ID: id, From: 1, To: id, Label: "LINK"}
	}
	store, e := native.OpenMemory(ctx, native.MemoryOptions{Identity: "native-example", Vertices: vertices, Edges: edges})
	if e != nil {
		return e
	}
	defer store.Close()
	batch := make([]string, 16)
	for i := range batch {
		batch[i] = fmt.Sprintf("g.V(%d).values('n')", i+1)
	}
	for _, test := range []struct {
		name  string
		texts []string
		rows  int
	}{{"point", []string{"g.V(1).values('n')"}, 1}, {"batch16", batch, 16}, {"fanout256", []string{"g.V(1).out('LINK').values('n').limit(256)"}, 256}} {
		for _, prepared := range []bool{false, true} {
			queries := make([]native.Query, len(test.texts))
			plans := make([]*native.Prepared, 0, len(queries))
			for i, text := range test.texts {
				if prepared {
					p, e := native.Prepare(ctx, text, native.PrepareOptions{})
					if e != nil {
						return e
					}
					plans = append(plans, p)
					defer p.Close() // Also release earlier plans if a later operation fails.
					queries[i] = p.Query(nil)
				} else {
					queries[i] = native.Query{Text: text}
				}
			}
			samples := make([]int64, 0, rounds)
			for i := 0; i < warmup+rounds; i++ {
				start := time.Now()
				result, e := store.Query(ctx, queries, native.QueryOptions{})
				elapsed := time.Since(start).Nanoseconds()
				if e != nil {
					return e
				}
				if result.Version != 0 {
					return fmt.Errorf("unexpected version %d", result.Version)
				}
				n := 0
				for _, r := range result.Results {
					for _, row := range r.Rows {
						expected := int64(n + 1)
						if test.name == "fanout256" {
							expected++
						}
						value, ok := row.Value.Int64()
						if !ok || value != expected {
							return fmt.Errorf("incorrect result at %d", n)
						}
						n++
					}
				}
				if n != test.rows {
					return fmt.Errorf("rows %d != %d", n, test.rows)
				}
				if i >= warmup {
					samples = append(samples, elapsed)
				}
			}
			for _, p := range plans {
				if e := p.Close(); e != nil {
					return e
				}
			}
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			var sum int64
			for _, n := range samples {
				sum += n
			}
			lane := "go-ffi-text"
			if prepared {
				lane = "go-ffi-prepared"
			}
			if e := json.NewEncoder(os.Stdout).Encode(map[string]any{"lane": lane, "workload": test.name, "rounds": rounds, "rows": test.rows, "p50_ns": samples[rounds/2], "p95_ns": samples[rounds*95/100], "mean_ns": sum / rounds}); e != nil {
				return e
			}
		}
	}
	return nil
}
