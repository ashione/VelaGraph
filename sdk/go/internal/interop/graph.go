package main

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ashione/VelaGraph/sdk/go"
	pb "github.com/ashione/VelaGraph/sdk/go/gen/managedv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func execute(c *client, q velagraph.Traversal) *pb.GraphExecuteResponse {
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

// Check what the v1 server receives when a newer client encodes an additional
// oneof member. If the step were skipped, the shortened query would return 1.
func (c *client) graphProtocolChecks(rpc pb.GraphServiceClient, partition uint32, first, version uint64) {
	req := &pb.GraphExecuteRequest{
		Context:   c.envelope(),
		Read:      &pb.ReadSelector{Kind: &pb.ReadSelector_Exact{Exact: version}},
		Traversal: &pb.GraphTraversal{Version: 1, Partition: partition, VertexIds: []uint64{first}},
	}
	out := &pb.GraphStep{Kind: &pb.GraphStep_Out{Out: &pb.GraphHop{}}}
	count := &pb.GraphStep{Kind: &pb.GraphStep_Count{Count: &pb.Empty{}}}
	req.Traversal.Steps = []*pb.GraphStep{out, count}
	res, err := rpc.Execute(c.ctx, req)
	check(err == nil, "protocol control query: %v", err)
	verifyCount(res, 1)

	// The generated binding emits expanded IDs. Append a packed segment for
	// the same known field to exercise the other legal encoding on the wire.
	mixed := proto.Clone(req).(*pb.GraphExecuteRequest)
	mixed.Context = c.envelope()
	mixed.Traversal.Steps = nil
	packed := protowire.AppendVarint(nil, first+1)
	packed = protowire.AppendVarint(packed, first)
	tagIDs := protowire.AppendTag(nil, 3, protowire.BytesType)
	mixed.Traversal.ProtoReflect().SetUnknown(protowire.AppendBytes(tagIDs, packed))
	res, err = rpc.Execute(c.ctx, mixed)
	check(err == nil, "mixed vertex ID encodings: %v", err)
	identity(mixed.Context.Identity, res.Identity)
	check(res.Version == version, "mixed vertex IDs returned version %d, want %d", res.Version, version)
	verifyVertices(res.GetData().GetRows(), partition, []uint64{first, first + 1, first})

	// Field 5 is an unrecognized length-delimited operation in GraphStep v1.
	future := &pb.GraphStep{}
	tag := protowire.AppendTag(nil, 5, protowire.BytesType)
	future.ProtoReflect().SetUnknown(protowire.AppendBytes(tag, nil))
	reject := func(name string) {
		res, err := rpc.Execute(c.ctx, req)
		check(status.Code(err) == codes.InvalidArgument && res == nil, "%s: got result %v, error %v", name, res, err)
	}
	for _, tc := range []struct {
		name string
		step *pb.GraphStep
	}{{"unknown step", future}, {"empty step", &pb.GraphStep{}}} {
		req.Context = c.envelope()
		req.Traversal.Steps = []*pb.GraphStep{out, tc.step, count}
		reject(tc.name)
	}

	// GraphStep uses a oneof. Exercise an actual enum on the same request.
	req.Traversal.Steps = []*pb.GraphStep{out, count}
	req.Context = c.envelope()
	req.Context.Identity.Binding = proto.Clone(c.binding).(*pb.Binding)
	req.Context.Identity.Binding.CapacityProfile = pb.CapacityProfile(99)
	reject("unknown capacity profile")
}

func (c *client) graphChecks(conn grpc.ClientConnInterface, partition uint32, first uint64) {
	g, err := velagraph.NewClient(conn, c.binding)
	check(err == nil, "graph client: %v", err)
	// Send an oversized plan directly to test gateway admission independently of the SDK.
	req := &pb.GraphExecuteRequest{Context: c.envelope(), Read: &pb.ReadSelector{Kind: &pb.ReadSelector_Latest{Latest: &pb.Empty{}}}, Traversal: &pb.GraphTraversal{Version: 1, Partition: partition, VertexIds: make([]uint64, 33)}}
	_, err = pb.NewGraphServiceClient(conn).Execute(c.ctx, req)
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
	base := func(client velagraph.Client) velagraph.Traversal {
		return client.V(partition, first, first).Out("reports")
	}
	oneEdge := execute(c, base(g).Path())
	c.graphProtocolChecks(pb.NewGraphServiceClient(conn), partition, first, oneEdge.Version)
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
		c.benchmarkGraphs(g.At(newer.Version), partition, starts, newer.Version)
	}
	if os.Getenv("VELAGRAPH_MULTI_ID_BENCH") == "1" {
		c.benchmarkDistinctIDs(g, partition, first)
	}
}

// All starts refer to the same vertex. Each hop crosses one of two parallel
// edges, doubling the independent expected count for this fixture.
func (c *client) benchmarkGraphs(g velagraph.Client, partition uint32, starts []uint64, version uint64) {
	for _, shape := range []struct{ starts, hops int }{{1, 0}, {2, 0}, {16, 0}, {2, 1}, {2, 2}} {
		ids := starts[:shape.starts]
		args := make([]string, len(ids))
		for i, id := range ids {
			args[i] = strconv.FormatUint(id, 10)
		}
		q := g.V(partition, ids...)
		text := "g.V(" + strings.Join(args, ",") + ")"
		if shape.hops >= 1 {
			q = q.Out("reports")
			text += ".out('reports')"
		}
		if shape.hops == 2 {
			q = q.In("reports")
			text += ".in('reports')"
		}
		name := fmt.Sprintf("starts=%d hops=%d", shape.starts, shape.hops)
		c.benchmarkGraph(name, q.Count(), &pb.Query{Partition: partition, Text: text + ".count()"}, version, uint64(len(ids))<<shape.hops)
	}
}

// Benchmark the public calls on the same warmed connection, certified V, graph
// and ordered bag. Text Open returns a retained page. Measure its Close separately.
func (c *client) benchmarkGraph(name string, q velagraph.Traversal, text *pb.Query, version, want uint64) {
	read := &pb.ReadSelector{Kind: &pb.ReadSelector_Exact{Exact: version}}
	typed := func() time.Duration {
		start := time.Now()
		res := execute(c, q)
		elapsed := time.Since(start)
		verifyCount(res, want)
		return elapsed
	}
	open := func() (time.Duration, time.Duration) {
		start := time.Now()
		req := &pb.OpenRequest{Context: c.envelope(), Read: read, TtlMillis: 5000, Query: text, PageSize: 1}
		res, err := c.query.Open(c.ctx, req)
		elapsed := time.Since(start)
		check(err == nil, "benchmark text Open: %v", err)
		identity(req.Context.Identity, res.Identity)
		check(proto.Equal(res.Query, text) && res.Session != nil && res.Session.Version == version && res.Page != nil && res.Page.Total == 1 && res.Page.Next == nil && len(res.Page.Rows) == 1 && res.Page.Rows[0].GetCount() == want && res.Page.Completeness == pb.Completeness_UNVERIFIED, "benchmark text result: %v", res)
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
		fmt.Printf("graph bench %s %-14s n=%d p50=%s p95=%s\n", name, []string{"typed Execute", "text Open", "text Close"}[i], len(data), data[(len(data)-1)/2], data[(len(data)*95+99)/100-1])
	}
}
