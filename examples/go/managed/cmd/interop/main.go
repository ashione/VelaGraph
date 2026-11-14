// Command interop is a fail-closed integration check against a real Rust gateway.
// It never automatically resubmits an uncertain write.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"time"

	pb "example.com/velagraph/managed/gen/managedv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func check(ok bool, format string, args ...any) {
	if !ok {
		panic(fmt.Sprintf(format, args...))
	}
}

type client struct {
	ctx     context.Context
	binding *pb.Binding
	write   pb.WriteServiceClient
	query   pb.QueryServiceClient
}

func (c *client) envelope() *pb.Context {
	nonce := make([]byte, 16)
	_, err := rand.Read(nonce)
	check(err == nil, "nonce: %v", err)
	return &pb.Context{Version: 2, TimeoutMillis: 5000, Identity: &pb.CallIdentity{RequestId: nonce, Binding: c.binding}}
}
func identity(want, got *pb.CallIdentity) {
	check(proto.Equal(want, got), "response identity mismatch: want %v got %v", want, got)
}
func expectError(err error, code codes.Code, reason pb.ErrorDetail_Reason) {
	check(err != nil, "expected error %v/%v", code, reason)
	st, ok := status.FromError(err)
	check(ok && st.Code() == code, "status: got %v want %v", err, code)
	details := st.Details()
	check(len(details) == 1, "expected one standard google.rpc.Status Any detail, got %#v", details)
	detail, ok := details[0].(*pb.ErrorDetail)
	check(ok, "standard status.Details did not decode public ErrorDetail: %#v", details[0])
	check(detail.Reason == reason, "reason got %v want %v", detail.Reason, reason)
}
func scalarProps() []*pb.Property {
	return []*pb.Property{
		{Key: "nil", Value: &pb.Scalar{Kind: &pb.Scalar_Null{Null: &pb.Empty{}}}},
		{Key: "integer", Value: &pb.Scalar{Kind: &pb.Scalar_Integer{Integer: -42}}},
		{Key: "real", Value: &pb.Scalar{Kind: &pb.Scalar_Real{Real: 1.25}}},
		{Key: "text", Value: &pb.Scalar{Kind: &pb.Scalar_Text{Text: "Go → Rust"}}},
		{Key: "boolean", Value: &pb.Scalar{Kind: &pb.Scalar_Boolean{Boolean: true}}},
	}
}
func (c *client) status(id []byte, mutation *pb.Mutation) *pb.WriteResponse {
	req := &pb.WriteRequest{Context: c.envelope(), ExternalId: id, Mutation: mutation}
	res, err := c.write.Status(c.ctx, req)
	check(err == nil, "Status: %v", err)
	identity(req.Context.Identity, res.Identity)
	check(bytes.Equal(res.ExternalId, id), "Status external ID")
	return res
}
func (c *client) create(partition uint32, id uint64) {
	external := make([]byte, 16)
	copy(external, []byte("GoVela01"))
	binary.BigEndian.PutUint64(external[8:], id)
	mutation := &pb.Mutation{Kind: &pb.Mutation_CreateVertex{CreateVertex: &pb.CreateVertex{
		Partition: partition, Vertex: &pb.Vertex{Id: id, Labels: []string{"GoInterop"}, Properties: scalarProps()},
	}}}
	check(c.status(external, mutation).GetAbsent() != nil, "fixture is not fresh: external ID already exists")
	req := &pb.WriteRequest{Context: c.envelope(), ExternalId: external, Mutation: mutation}
	res, err := c.write.Submit(c.ctx, req)
	check(err == nil, "Submit (outcome unknown, do not automatically retry): %v", err)
	identity(req.Context.Identity, res.Identity)
	check(bytes.Equal(res.ExternalId, external), "Submit external ID")
	for res.GetPending() != nil || (res.GetCommitted() != nil && !res.GetCommitted().Visible) {
		select {
		case <-c.ctx.Done():
			panic("pending write deadline; outcome unknown")
		case <-time.After(20 * time.Millisecond):
		}
		res = c.status(external, mutation)
	}
	check(res.GetCommitted() != nil && res.GetCommitted().Visible, "write not visible committed: %v", res)
	check(len(res.TransactionId) == 16, "transaction ID must be 16 bytes")
	terminal := c.status(external, mutation)
	check(bytes.Equal(terminal.TransactionId, res.TransactionId) && proto.Equal(terminal.GetCommitted(), res.GetCommitted()), "Status changed terminal decision")
	// Deliberate idempotency assertion only AFTER a confirmed commit, not transport retry.
	req.Context = c.envelope()
	replay, err := c.write.Submit(c.ctx, req)
	check(err == nil, "confirmed replay: %v", err)
	identity(req.Context.Identity, replay.Identity)
	check(bytes.Equal(replay.TransactionId, res.TransactionId) && proto.Equal(replay.GetCommitted(), res.GetCommitted()), "replay changed terminal decision")
}

func rows(page *pb.Page, partition uint32, first uint64, seen map[uint64]bool) {
	check(page != nil && page.Completeness == pb.Completeness_SINGLE_PARTITION, "missing/incomplete page")
	for _, r := range page.Rows {
		m := r.GetPropertyMap()
		check(m != nil && m.Source != nil && m.Source.GetVertex() != nil, "expected typed vertex property map: %v", r)
		src := m.Source.GetVertex()
		check(src.Partition == partition && (src.Id == first || src.Id == first+1) && !seen[src.Id], "wrong/duplicate row %v", src)
		seen[src.Id] = true
		values := map[string][]*pb.Scalar{}
		for _, p := range m.Properties {
			_, exists := values[p.Key]
			check(!exists, "duplicate property")
			values[p.Key] = p.Values
		}
		// Absence is not a null scalar. An absent key may be omitted or have zero values.
		check(len(values["absent"]) == 0, "absent property synthesized a value")
		for _, p := range scalarProps() {
			got := values[p.Key]
			check(len(got) == 1 && proto.Equal(got[0], p.Value), "property %s: got %v want %v", p.Key, got, p.Value)
		}
	}
}
func (c *client) pages(partition uint32, first uint64) {
	q := &pb.Query{Partition: partition, Text: fmt.Sprintf("g.V(%d,%d).valueMap('nil','integer','real','text','boolean','absent')", first, first+1)}
	req := &pb.OpenRequest{Context: c.envelope(), Read: &pb.ReadSelector{Kind: &pb.ReadSelector_Latest{Latest: &pb.Empty{}}}, TtlMillis: 10000, Query: q, PageSize: 1}
	opened, err := c.query.Open(c.ctx, req)
	check(err == nil, "Open: %v", err)
	identity(req.Context.Identity, opened.Identity)
	check(opened.Session != nil && len(opened.Session.Token) == 16 && len(opened.Session.Incarnation) == 16, "session shape")
	check(proto.Equal(opened.Query, q) && opened.PageSize == 1, "Open query/page-size correlation")
	seen := map[uint64]bool{}
	page := opened.Page
	check(page != nil && page.Total == 2 && page.Offset == 0 && len(page.Rows) == 1 && page.Next != nil, "first page not paginated: %v", page)
	rows(page, partition, first, seen)
	fetch := &pb.FetchRequest{Context: c.envelope(), Session: opened.Session, Query: q, Cursor: page.Next, PageSize: 1}
	next, err := c.query.Fetch(c.ctx, fetch)
	check(err == nil, "Fetch: %v", err)
	identity(fetch.Context.Identity, next.Identity)
	check(proto.Equal(next.Session, opened.Session) && proto.Equal(next.Query, q) && proto.Equal(next.Cursor, fetch.Cursor) && next.PageSize == 1, "Fetch session/version/query/cursor changed")
	check(next.Page != nil && next.Page.Offset == 1 && next.Page.Total == 2 && next.Page.Next == nil, "last page shape: %v", next.Page)
	rows(next.Page, partition, first, seen)
	check(len(seen) == 2, "missing results")
	closeReq := &pb.CloseRequest{Context: c.envelope(), Session: opened.Session}
	closed, err := c.query.Close(c.ctx, closeReq)
	check(err == nil, "Close: %v", err)
	identity(closeReq.Context.Identity, closed.Identity)
	check(proto.Equal(closed.Session, opened.Session), "Close session")
	fetch.Context = c.envelope()
	_, err = c.query.Fetch(c.ctx, fetch)
	expectError(err, codes.FailedPrecondition, pb.ErrorDetail_SESSION_UNAVAILABLE)
}
func run() {
	endpoint := flag.String("endpoint", "", "loopback Rust public gRPC address")
	bindingPath := flag.String("binding-proto", "", "protobuf binary Binding from Rust fixture")
	partition := flag.Uint("partition", 7, "fixture partition")
	first := flag.Uint64("first-id", 900001, "two fresh vertex IDs")
	flag.Parse()
	check(*endpoint != "" && *bindingPath != "", "endpoint and binding-proto are required")
	check(*partition <= uint(^uint32(0)) && *first > 0 && *first < ^uint64(0), "invalid IDs")
	data, err := os.ReadFile(*bindingPath)
	check(err == nil, "binding: %v", err)
	binding := new(pb.Binding)
	err = proto.Unmarshal(data, binding)
	check(err == nil, "binding protobuf: %v", err)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<20), grpc.MaxCallSendMsgSize(1<<20)))
	check(err == nil, "client: %v", err)
	defer conn.Close()
	c := client{ctx: ctx, binding: binding, write: pb.NewWriteServiceClient(conn), query: pb.NewQueryServiceClient(conn)}
	invalid := &pb.OpenRequest{Context: c.envelope(), Read: &pb.ReadSelector{Kind: &pb.ReadSelector_Latest{Latest: &pb.Empty{}}}, TtlMillis: 10000, Query: &pb.Query{Partition: uint32(*partition), Text: "g.V("}, PageSize: 1}
	_, err = c.query.Open(ctx, invalid)
	expectError(err, codes.InvalidArgument, pb.ErrorDetail_QUERY_SYNTAX)
	invalid.Query.Text = "g.V(1)"
	invalid.Context = c.envelope()
	invalid.Context.Version = 3
	_, err = c.query.Open(ctx, invalid)
	expectError(err, codes.InvalidArgument, pb.ErrorDetail_INVALID_ARGUMENT)
	// An internal service must not be exposed through the public listener.
	err = conn.Invoke(ctx, "/velagraph.internal.managed.v1.ManagedService/Call", &emptypb.Empty{}, &emptypb.Empty{})
	check(status.Code(err) == codes.Unimplemented, "public listener exposed internal service: %v", err)
	c.create(uint32(*partition), *first)
	c.create(uint32(*partition), *first+1)
	c.pages(uint32(*partition), *first)
	fmt.Println("PASS: Go generated typed client; version/query rich errors; public/internal isolation; Submit/Status/confirmed replay; scalar/null/absence; immutable paginated Open/Fetch/Close")
}
func main() {
	defer func() {
		if v := recover(); v != nil {
			fmt.Fprintln(os.Stderr, "interop failed:", v)
			os.Exit(1)
		}
	}()
	run()
}
