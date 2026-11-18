package main

import (
	"fmt"
	"os"
	"slices"
	"time"

	pb "example.com/velagraph/managed/gen/managedv1"
	"example.com/velagraph/managed/graph"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func execute(c *client, q graph.Traversal) *pb.GraphExecuteResponse {
	res, err := q.Execute(c.ctx)
	check(err == nil, "graph Execute: %v", err)
	return res
}

func verifyCount(res *pb.GraphExecuteResponse, want uint64) {
	rows := res.GetData().GetRows()
	check(len(rows) == 1, "count shape: %v", rows)
	count, ok := rows[0].Kind.(*pb.Row_Count)
	check(ok && count.Count == want, "got %v, want count %d", rows[0], want)
}

func (c *client) graphChecks(rpc pb.GraphServiceClient, partition uint32, first uint64) {
	g := graph.New(rpc, c.binding)
	// Send an oversized plan directly to test gateway admission independently of the SDK.
	req := &pb.GraphExecuteRequest{Context: c.envelope(), Read: &pb.ReadSelector{Kind: &pb.ReadSelector_Latest{Latest: &pb.Empty{}}}, Traversal: &pb.GraphTraversal{Version: 1, Partition: partition, VertexIds: make([]uint64, 33)}}
	_, err := rpc.Execute(c.ctx, req)
	check(status.Code(err) == codes.ResourceExhausted, "oversized typed plan admitted or wrong status: %v", err)

	ids := []uint64{first + 1, first, first + 1}
	result := execute(c, g.V(partition, ids...))
	check(len(result.Data.Rows) == len(ids), "explicit vertex bag shape")
	for i, row := range result.Data.Rows {
		v := row.GetElement().GetVertex()
		check(v != nil && v.Partition == partition && v.Id == ids[i], "wrong vertex order at %d: %v", i, row)
	}
	verifyCount(execute(c, g.At(0).V(partition, first).Count()), 0)
	var other uint32
	found := false
	for _, m := range c.binding.Members {
		if m.Shard != uint64(partition) && m.Shard <= uint64(^uint32(0)) {
			other, found = uint32(m.Shard), true
			break
		}
	}
	check(found, "cross-shard graph check requires a second fixture member")
	target := first + 3
	c.create(other, target)
	from, to := &pb.Entity{Partition: partition, Id: first}, &pb.Entity{Partition: other, Id: target}
	createEdge := func(id uint64) {
		c.commit("GoEdge01", id, &pb.Mutation{Kind: &pb.Mutation_CreateEdge{CreateEdge: &pb.CreateEdge{Edge: &pb.Edge{Id: id, From: from, To: to, Label: "reports"}}}})
	}
	createEdge(first)
	base := func(client graph.Client) graph.Traversal { return client.V(partition, first, first).Out("reports") }
	oneEdge := execute(c, base(g).Path())
	check(len(oneEdge.Data.Rows) == 2, "duplicate starts must yield two paths: %v", oneEdge)
	for _, row := range oneEdge.Data.Rows {
		path := row.GetPath().GetElements()
		check(len(path) == 3 && proto.Equal(path[0].GetVertex(), from) && proto.Equal(path[1].GetEdge(), from) && proto.Equal(path[2].GetVertex(), to), "wrong cross-shard path: %v", path)
	}
	createEdge(first + 1)
	verifyCount(execute(c, base(g.At(oneEdge.Version)).In("reports").Count()), 2)
	newer := execute(c, base(g.AtLeast(oneEdge.Version+1)).In("reports").Count())
	verifyCount(newer, 8)
	starts := make([]uint64, 16)
	for i := range starts {
		starts[i] = first
	}
	many := g.At(newer.Version).V(partition, starts...).Out("reports").In("reports").Out("reports")
	verifyCount(execute(c, many.Count()), 128)
	elements := execute(c, many)
	check(len(elements.Data.Rows) == 100, "default element result cap: got %d", len(elements.Data.Rows))
	for _, r := range elements.Data.Rows {
		check(proto.Equal(r.GetElement().GetVertex(), to), "wrong capped element: %v", r)
	}
	if os.Getenv("VELAGRAPH_GRAPH_BENCH") == "1" {
		c.benchmarkGraph(base(g.At(newer.Version)).In("reports").Count(), partition, first, newer.Version)
	}
}

// Benchmark the public calls on the same warmed connection, certified V, graph
// and ordered bag. Text Open returns a retained page. Measure its Close separately.
func (c *client) benchmarkGraph(q graph.Traversal, partition uint32, first, version uint64) {
	text := &pb.Query{Partition: partition, Text: fmt.Sprintf("g.V(%d,%d).out('reports').in('reports').count()", first, first)}
	read := &pb.ReadSelector{Kind: &pb.ReadSelector_Exact{Exact: version}}
	typed := func() time.Duration {
		start := time.Now()
		res := execute(c, q)
		elapsed := time.Since(start)
		verifyCount(res, 8)
		return elapsed
	}
	open := func() (time.Duration, time.Duration) {
		start := time.Now()
		req := &pb.OpenRequest{Context: c.envelope(), Read: read, TtlMillis: 5000, Query: text, PageSize: 1}
		res, err := c.query.Open(c.ctx, req)
		elapsed := time.Since(start)
		check(err == nil, "benchmark text Open: %v", err)
		identity(req.Context.Identity, res.Identity)
		check(proto.Equal(res.Query, text) && res.Session != nil && res.Session.Version == version && res.Page != nil && res.Page.Total == 1 && res.Page.Next == nil && len(res.Page.Rows) == 1 && res.Page.Rows[0].GetCount() == 8 && res.Page.Completeness == pb.Completeness_UNVERIFIED, "benchmark text result: %v", res)
		closeStart := time.Now()
		_, err = c.query.Close(c.ctx, &pb.CloseRequest{Context: c.envelope(), Session: res.Session})
		check(err == nil, "benchmark text Close: %v", err)
		return elapsed, time.Since(closeStart)
	}
	const samples = 40
	var graphTimes, openTimes, closeTimes []time.Duration
	for i := -4; i < samples; i++ {
		var graphTime, openTime, closeTime time.Duration
		if i%2 == 0 {
			graphTime = typed()
			openTime, closeTime = open()
		} else {
			openTime, closeTime = open()
			graphTime = typed()
		}
		if i >= 0 {
			graphTimes = append(graphTimes, graphTime)
			openTimes = append(openTimes, openTime)
			closeTimes = append(closeTimes, closeTime)
		}
	}
	for i, data := range [][]time.Duration{graphTimes, openTimes, closeTimes} {
		slices.Sort(data)
		fmt.Printf("graph bench %-14s n=%d p50=%s p95=%s\n", []string{"typed Execute", "text Open", "text Close"}[i], len(data), data[(len(data)-1)/2], data[(len(data)*95+99)/100-1])
	}
}
