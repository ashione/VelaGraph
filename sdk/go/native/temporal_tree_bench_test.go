//go:build cgo && velagraph_native

package native

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// Use a six-way tree of depth three, numbered breadth first. Every timed call
// decodes the packed response and verifies each level's complete vertex set.
func BenchmarkNativeTemporalTree(b *testing.B) {
	ctx := context.Background()
	path := b.TempDir()
	writer, err := OpenTemporal(ctx, path)
	if err != nil {
		b.Fatal(err)
	}
	defer writer.Close()
	const vertices = 259
	valid := Interval{From: 0}
	var mutations []Mutation
	for id := uint64(1); id <= vertices; id++ {
		m := PutVertex(Vertex{ID: id})
		m.Valid = &valid
		mutations = append(mutations, m)
	}
	for id := uint64(2); id <= vertices; id++ {
		m := PutEdge(Edge{ID: id, From: (id-2)/6 + 1, To: id, Label: "child"})
		m.Valid = &valid
		mutations = append(mutations, m)
	}
	var version uint64
	for len(mutations) > 0 {
		n := min(128, len(mutations))
		next, err := writer.Apply(ctx, WriteBatch{ExpectedVersion: version, Mutations: mutations[:n]})
		if err != nil || next != version+1 {
			b.Fatalf("write version %d: %d, %v", version, next, err)
		}
		version = next
		mutations = mutations[n:]
	}
	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}
	store, err := OpenTemporal(ctx, path)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	info, err := store.Info(ctx)
	if err != nil || info.Backend != "temporal" || info.Version != version {
		b.Fatalf("reopen: %+v, %v", info, err)
	}
	at := int64(1)
	opts := QueryOptions{Version: &version, ValidTimeMicros: &at}
	type queryLevel struct {
		text         string
		first, count uint64
	}
	levels := func(root uint64, depth int) []queryLevel {
		text := fmt.Sprintf("g.V(%d)", root)
		first, count := root, uint64(1)
		var out []queryLevel
		for level := 0; level <= depth; level++ {
			// Leave room for an unexpected extra row and bypass the default 100-row cap.
			bounded := fmt.Sprintf("%s.limit(%d)", text, vertices+1)
			out = append(out, queryLevel{bounded, first, count})
			text += ".out('child')"
			first = (first-1)*6 + 2
			count *= 6
		}
		return out
	}
	var leaves []string
	for i := 0; i < 16; i++ {
		leaves = append(leaves, strconv.Itoa(44+6*i))
	}
	incoming := "g.V(" + strings.Join(leaves, ",") + ").in('child')"
	for _, tc := range []struct {
		name   string
		levels []queryLevel
	}{
		{"Parents16", []queryLevel{{incoming, 8, 16}}},
		{"Subtree43", levels(2, 2)},
		{"Whole259", levels(1, 3)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var queries []Query
			for _, level := range tc.levels {
				p, err := Prepare(ctx, level.text, PrepareOptions{})
				if err != nil {
					b.Fatal(err)
				}
				defer p.Close()
				queries = append(queries, p.Query(nil))
			}
			read := func() {
				r, err := store.Query(ctx, queries, opts)
				if err != nil || r.Version != version || len(r.Results) != len(tc.levels) {
					b.Fatalf("query response: %v", err)
				}
				for i, level := range tc.levels {
					rows := r.Results[i].Rows
					if uint64(len(rows)) != level.count {
						b.Fatalf("level %d: got %d rows, want %d", i, len(rows), level.count)
					}
					var seen [vertices + 1]bool
					for _, row := range rows {
						if row.Kind != "Vertex" || row.ID < level.first || row.ID >= level.first+level.count || seen[row.ID] {
							b.Fatalf("invalid or duplicate vertex %d at level %d", row.ID, i)
						}
						seen[row.ID] = true
					}
				}
			}
			read()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				read()
			}
			b.StopTimer()
		})
	}
}
