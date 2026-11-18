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
	rows     []*pb.Row
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
	res.Data = &pb.QueryData{Completeness: pb.Completeness_UNVERIFIED, Rows: r.rows}
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
	if _, err := path.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	rpc.rows = []*pb.Row{{Kind: &pb.Row_Count{Count: 0}}}
	if _, err := count.Execute(context.Background()); err != nil {
		t.Fatal(err)
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

func TestCountResponseShape(t *testing.T) {
	zero := &pb.Row{Kind: &pb.Row_Count{Count: 0}}
	for _, tc := range []struct {
		name  string
		rows  []*pb.Row
		want  uint64
		valid bool
	}{
		{name: "zero", rows: []*pb.Row{zero}, valid: true},
		{name: "positive", rows: []*pb.Row{{Kind: &pb.Row_Count{Count: 7}}}, want: 7, valid: true},
		{name: "missing"},
		{name: "multiple", rows: []*pb.Row{zero, zero}},
		{name: "nil row", rows: []*pb.Row{nil}},
		{name: "unset kind", rows: []*pb.Row{{}}},
		{name: "path", rows: []*pb.Row{{Kind: &pb.Row_Path{Path: &pb.Path{}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := velagraph.NewClient(&recorder{rows: tc.rows}, &pb.Binding{})
			if err != nil {
				t.Fatal(err)
			}
			result, err := client.V(1, 2).Count().Execute(context.Background())
			if tc.valid {
				if err != nil || result.Data.Rows[0].GetCount() != tc.want {
					t.Fatalf("want count %d, got %v, error %v", tc.want, result, err)
				}
			} else if err == nil || result != nil {
				t.Fatalf("invalid count response accepted: %v, error %v", result, err)
			}
		})
	}
}
