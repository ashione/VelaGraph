package velagraph_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/ashione/VelaGraph/sdk/go"
	pb "github.com/ashione/VelaGraph/sdk/go/gen/managedv1"
	"google.golang.org/grpc"
)

type recorder struct {
	requests []*pb.GraphExecuteRequest
	version  uint64
}

func (r *recorder) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("unexpected streaming RPC")
}

func (r *recorder) Invoke(_ context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	if method != pb.GraphService_Execute_FullMethodName {
		return errors.New("unexpected unary RPC")
	}
	req := args.(*pb.GraphExecuteRequest)
	r.requests = append(r.requests, req)
	res := reply.(*pb.GraphExecuteResponse)
	res.Version, res.Identity = r.version, req.Context.Identity
	res.Data = &pb.QueryData{Completeness: pb.Completeness_UNVERIFIED}
	return nil
}

func TestChainBuildsLocallyAndBranches(t *testing.T) {
	rpc := &recorder{}
	ids := []uint64{4, 2, 4}
	binding := &pb.Binding{AuthorityShard: 9}
	client, err := velagraph.NewClient(rpc, binding)
	if err != nil {
		t.Fatal(err)
	}
	base := client.At(0).V(7, ids...).Out("reports")
	path, count := base.In().Path(), base.Count()
	ids[0], binding.AuthorityShard = 99, 80
	if len(rpc.requests) != 0 {
		t.Fatal("RPC during construction")
	}
	for _, query := range []velagraph.Traversal{path, count} {
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
	client, err := velagraph.NewClient(rpc, &pb.Binding{})
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []velagraph.Traversal{client.V(1), client.V(1, make([]uint64, 33)...), client.V(1, 2).Out("a", "b")} {
		if _, err := query.Execute(context.Background()); err == nil {
			t.Fatal("invalid builder succeeded")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.V(1, 2).Execute(ctx); !errors.Is(err, context.Canceled) || len(rpc.requests) != 0 {
		t.Fatal("invalid or canceled request reached transport")
	}
	for _, c := range []velagraph.Client{client.At(0), client.AtLeast(2)} {
		if _, err := c.V(1, 2).Execute(context.Background()); err == nil {
			t.Fatal("wrong version accepted")
		}
	}
}
