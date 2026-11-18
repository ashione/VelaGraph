package main

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ashione/VelaGraph/sdk/go"
	pb "github.com/ashione/VelaGraph/sdk/go/gen/managedv1"
)

func verifyVertices(rows []*pb.Row, partition uint32, ids []uint64) {
	check(len(rows) == len(ids), "expected %d vertices, got %d", len(ids), len(rows))
	for i, row := range rows {
		v := row.GetElement().GetVertex()
		check(v != nil && v.Partition == partition && v.Id == ids[i], "wrong qualified vertex at offset %d: %v", i, row)
	}
}

func (c *client) benchmarkDistinctIDs(g velagraph.Client, partition uint32, first uint64) {
	const width = 32
	start := first + 4 // The ordinary fixture reserves first through first+3.
	if start > math.MaxInt64-(width-1) {
		start = first - width
	}
	ids := make([]uint64, width)
	for i := range ids {
		ids[i] = start + uint64(i)
		c.create(partition, ids[i])
	}
	slices.Reverse(ids) // A sorted response must fail the order check.
	snapshot := execute(c, g.V(partition, ids...))
	verifyVertices(snapshot.Data.Rows, partition, ids)
	for _, n := range []int{2, 8, 16, width} {
		c.measureVertexBatch(g.At(snapshot.Version), snapshot.Version, partition, ids[:n])
	}
}

// Report time to the last result. The serial baseline uses the same warmed
// connection and exact V for each individual RPC. Text session cleanup is untimed.
func (c *client) measureVertexBatch(g velagraph.Client, version uint64, partition uint32, ids []uint64) {
	batch := g.V(partition, ids...)
	singles := make([]velagraph.Traversal, len(ids))
	args := make([]string, len(ids))
	for i, id := range ids {
		singles[i] = g.V(partition, id)
		args[i] = strconv.FormatUint(id, 10)
	}
	query := &pb.Query{Partition: partition, Text: "g.V(" + strings.Join(args, ",") + ")"}
	read := &pb.ReadSelector{Kind: &pb.ReadSelector_Exact{Exact: version}}
	cases := []struct {
		name string
		run  func() time.Duration
	}{
		{"SDK batch", func() time.Duration {
			begin := time.Now()
			result := execute(c, batch)
			elapsed := time.Since(begin)
			verifyVertices(result.Data.Rows, partition, ids)
			return elapsed
		}},
		{"text Open", func() time.Duration {
			begin := time.Now()
			req := &pb.OpenRequest{Context: c.envelope(), Read: read, TtlMillis: 5000, Query: query, PageSize: uint32(len(ids))}
			res, err := c.query.Open(c.ctx, req)
			elapsed := time.Since(begin)
			check(err == nil, "multi-ID text Open: %v", err)
			identity(req.Context.Identity, res.Identity)
			check(res.Session != nil && res.Session.Version == version && res.Page != nil && res.Page.Next == nil && res.Page.Total == uint32(len(ids)) && res.Page.Completeness == pb.Completeness_UNVERIFIED, "incomplete multi-ID text result")
			verifyVertices(res.Page.Rows, partition, ids)
			_, err = c.query.Close(c.ctx, &pb.CloseRequest{Context: c.envelope(), Session: res.Session})
			check(err == nil, "multi-ID text Close: %v", err)
			return elapsed
		}},
		{"SDK serial", func() time.Duration {
			results := make([]*pb.GraphExecuteResponse, len(ids))
			begin := time.Now()
			for i, q := range singles {
				results[i] = execute(c, q)
			}
			elapsed := time.Since(begin)
			for i, res := range results {
				verifyVertices(res.Data.Rows, partition, ids[i:i+1])
			}
			return elapsed
		}},
	}
	const warmups, samples = 2, 20
	times := make([][]time.Duration, len(cases))
	for round := 0; round < warmups+samples; round++ {
		for offset := range cases {
			i := (round + offset) % len(cases)
			elapsed := cases[i].run()
			if round >= warmups {
				times[i] = append(times[i], elapsed)
			}
		}
	}
	for i, method := range cases {
		slices.Sort(times[i])
		fmt.Printf("distinct IDs=%d %-10s n=%d p50=%s p95=%s\n", len(ids), method.name, samples, times[i][(samples-1)/2], times[i][(samples*95+99)/100-1])
	}
}
