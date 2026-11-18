package graph

import (
	"context"
	"errors"
	"slices"
	"testing"

	pb "example.com/velagraph/managed/gen/managedv1"
	"google.golang.org/grpc"
)

type recorder struct {
	requests []*pb.GraphExecuteRequest
	version  uint64
}

func (r *recorder) Execute(_ context.Context, req *pb.GraphExecuteRequest, _ ...grpc.CallOption) (*pb.GraphExecuteResponse, error) {
	r.requests = append(r.requests, req)
	return &pb.GraphExecuteResponse{Version: r.version, Data: &pb.QueryData{Completeness: pb.Completeness_UNVERIFIED}, Identity: req.Context.Identity}, nil
}

func TestChainBuildsLocallyAndBranches(t *testing.T) {
	rpc := &recorder{}
	ids := []uint64{4, 2, 4}
	binding := &pb.Binding{AuthorityShard: 9}
	client := New(rpc, binding)
	base := client.At(0).V(7, ids...).Out("reports")
	path, count := base.In().Path(), base.Count()
	ids[0], binding.AuthorityShard = 99, 80
	if len(rpc.requests) != 0 {
		t.Fatal("RPC during construction")
	}
	for _, query := range []Traversal{path, count} {
		if _, err := query.Execute(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(rpc.requests) != 2 {
		t.Fatalf("got %d RPCs for two Execute calls", len(rpc.requests))
	}
	req := rpc.requests[0]
	plan := req.Traversal
	if _, exact := req.Read.Kind.(*pb.ReadSelector_Exact); !exact || req.Read.GetExact() != 0 || plan.Version != 1 || plan.Partition != 7 || !slices.Equal(plan.VertexIds, []uint64{4, 2, 4}) || req.Context.Identity.Binding.AuthorityShard != 9 {
		t.Fatalf("wrong envelope: %v", req)
	}
	if len(plan.Steps) != 3 || plan.Steps[0].GetOut().GetLabel() != "reports" || plan.Steps[1].GetIn() == nil || plan.Steps[1].GetIn().Label != nil || plan.Steps[2].GetPath() == nil {
		t.Fatalf("wrong path steps: %v", plan.Steps)
	}
	other := rpc.requests[1].Traversal.Steps
	if len(other) != 2 || other[1].GetCount() == nil {
		t.Fatalf("branch was overwritten: %v", other)
	}
}

func TestExecuteRejectsInvalidInputAndWrongVersion(t *testing.T) {
	rpc := &recorder{version: 1}
	client := New(rpc, &pb.Binding{})
	for _, query := range []Traversal{client.V(1), client.V(1, make([]uint64, 33)...), client.V(1, 2).Out("a", "b")} {
		if _, err := query.Execute(context.Background()); err == nil {
			t.Fatal("invalid builder succeeded")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.V(1, 2).Execute(ctx); !errors.Is(err, context.Canceled) || len(rpc.requests) != 0 {
		t.Fatal("invalid or canceled request reached transport")
	}
	for _, c := range []Client{client.At(0), client.AtLeast(2)} {
		if _, err := c.V(1, 2).Execute(context.Background()); err == nil {
			t.Fatal("wrong version accepted")
		}
	}
}
