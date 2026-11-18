// Package velagraph provides the Go client for certified graph traversals on a managed VelaGraph gateway.
package velagraph

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"time"

	pb "github.com/ashione/VelaGraph/sdk/go/gen/managedv1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Client reuses the supplied GraphService transport and pins its cohort binding.
type Client struct {
	rpc     pb.GraphServiceClient
	binding *pb.Binding
	read    *pb.ReadSelector
}

// NewClient uses a caller-owned gRPC connection and copies the cohort binding.
// Reads default to the latest certified version selected during Execute.
func NewClient(conn grpc.ClientConnInterface, binding *pb.Binding) (Client, error) {
	if conn == nil || binding == nil {
		return Client{}, errors.New("missing gRPC connection or cohort binding")
	}
	return Client{
		rpc:     pb.NewGraphServiceClient(conn),
		binding: proto.Clone(binding).(*pb.Binding),
		read:    &pb.ReadSelector{Kind: &pb.ReadSelector_Latest{Latest: &pb.Empty{}}},
	}, nil
}

// At selects an exact certified version, including version zero.
func (c Client) At(version uint64) Client {
	c.read = &pb.ReadSelector{Kind: &pb.ReadSelector_Exact{Exact: version}}
	return c
}

// AtLeast requests a certified version at or above the supplied minimum.
func (c Client) AtLeast(version uint64) Client {
	c.read = &pb.ReadSelector{Kind: &pb.ReadSelector_Minimum{Minimum: version}}
	return c
}

// Traversal is a reusable value. Appending a step creates an independent chain.
type Traversal struct {
	client    Client
	partition uint32
	ids       []uint64
	steps     []*pb.GraphStep
	err       error
}

// V starts with explicit IDs in one partition, preserving order and duplicates.
func (c Client) V(partition uint32, ids ...uint64) Traversal {
	q := Traversal{client: c, partition: partition}
	if len(ids) == 0 || len(ids) > 32 {
		q.err = errors.New("V requires 1 to 32 vertex IDs")
		return q
	}
	q.ids = slices.Clone(ids)
	return q
}

func (q Traversal) append(step *pb.GraphStep) Traversal {
	if q.err != nil {
		return q
	}
	// Up to three hops and a terminal. The server checks the full sequence.
	if len(q.steps) >= 4 {
		q.err = errors.New("graph traversal exceeds four steps")
		return q
	}
	q.steps = append(slices.Clip(q.steps), step)
	return q
}

func (q Traversal) hop(out bool, labels []string) Traversal {
	if q.err != nil {
		return q
	}
	h := &pb.GraphHop{}
	switch len(labels) {
	case 0:
	case 1:
		label := labels[0]
		h.Label = &label
	default:
		q.err = errors.New("a hop accepts at most one edge label")
		return q
	}
	if out {
		return q.append(&pb.GraphStep{Kind: &pb.GraphStep_Out{Out: h}})
	}
	return q.append(&pb.GraphStep{Kind: &pb.GraphStep_In{In: h}})
}

// Out follows outgoing edges, optionally filtered by a label.
func (q Traversal) Out(label ...string) Traversal { return q.hop(true, label) }

// In follows incoming edges, optionally filtered by a label.
func (q Traversal) In(label ...string) Traversal { return q.hop(false, label) }

// Path returns the executor's ordered paths and must be terminal.
func (q Traversal) Path() Traversal {
	return q.append(&pb.GraphStep{Kind: &pb.GraphStep_Path{Path: &pb.Empty{}}})
}

// Count returns one count row and must be terminal.
func (q Traversal) Count() Traversal {
	return q.append(&pb.GraphStep{Kind: &pb.GraphStep_Count{Count: &pb.Empty{}}})
}

// Execute submits one structured query with a timeout of at most five seconds.
// An earlier context deadline shortens this budget.
func (q Traversal) Execute(ctx context.Context) (*pb.GraphExecuteResponse, error) {
	if q.err != nil {
		return nil, q.err
	}
	if q.client.rpc == nil || q.client.binding == nil {
		return nil, errors.New("missing graph client or cohort binding")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	budget := 5 * time.Second
	if end, ok := ctx.Deadline(); ok {
		budget = min(time.Until(end), budget)
		if budget <= 0 {
			return nil, context.DeadlineExceeded
		}
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("graph request ID: %w", err)
	}
	identity := &pb.CallIdentity{RequestId: nonce, Binding: q.client.binding}
	req := &pb.GraphExecuteRequest{
		Context:   &pb.Context{Version: 2, TimeoutMillis: uint64((budget + time.Millisecond - 1) / time.Millisecond), Identity: identity},
		Read:      q.client.read,
		Traversal: &pb.GraphTraversal{Version: 1, Partition: q.partition, VertexIds: q.ids, Steps: q.steps},
	}
	result, err := q.client.rpc.Execute(ctx, req, grpc.MaxCallSendMsgSize(64<<10), grpc.MaxCallRecvMsgSize(1<<20))
	if err != nil {
		return nil, err
	}
	if !proto.Equal(result.GetIdentity(), identity) || result.GetData() == nil || result.Data.Completeness != pb.Completeness_UNVERIFIED {
		return nil, errors.New("invalid graph response identity or query data")
	}
	switch read := req.Read.Kind.(type) {
	case *pb.ReadSelector_Exact:
		if result.Version != read.Exact {
			return nil, errors.New("graph response did not use the exact version")
		}
	case *pb.ReadSelector_Minimum:
		if result.Version < read.Minimum {
			return nil, errors.New("graph response is below the minimum version")
		}
	}
	return result, nil
}
