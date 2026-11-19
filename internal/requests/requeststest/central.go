// Package requeststest provides an in-memory fake of central's gRPC ingestion
// service, in the spirit of net/http/httptest. It enforces the contract's
// message limit like the real server and records what it received.
package requeststest

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	ingestv1 "github.com/ads-iscte-rg-ja/monitor-api/gen/go/monitor/ingest/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"

	"monitor-remote/internal/requests"
)

// Central answers with Stats/Errors when set, or acknowledges everything.
type Central struct {
	ingestv1.UnimplementedIngestServiceServer

	Stats  func(*ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error)
	Errors func(*ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error)

	mu    sync.Mutex
	keys  []string
	calls int
}

// Start serves c in memory and returns an agent client that sends apiKey.
func Start(t testing.TB, c *Central, apiKey string) *requests.Client {
	t.Helper()
	listener := bufconn.Listen(8 << 20)
	server := grpc.NewServer(grpc.MaxRecvMsgSize(ingestv1.MaxMessageBytes))
	ingestv1.RegisterIngestServiceServer(server, c)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///central",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return requests.NewClientConn(conn, apiKey)
}

// Calls is the number of requests received.
func (c *Central) Calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// Keys are the API keys received, one per request.
func (c *Central) Keys() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.keys...)
}

func (c *Central) record(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	md, _ := metadata.FromIncomingContext(ctx)
	c.keys = append(c.keys, md.Get(ingestv1.APIKeyMetadata)...)
}

// IngestStats implements the service.
func (c *Central) IngestStats(ctx context.Context, req *ingestv1.IngestStatsRequest) (*ingestv1.IngestStatsResponse, error) {
	c.record(ctx)
	if c.Stats != nil {
		return c.Stats(req)
	}
	return AckStats(req, 7), nil
}

// IngestErrors implements the service.
func (c *Central) IngestErrors(ctx context.Context, req *ingestv1.IngestErrorsRequest) (*ingestv1.IngestErrorsResponse, error) {
	c.record(ctx)
	if c.Errors != nil {
		return c.Errors(req)
	}
	return AckErrors(req, len(req.GetErrors())), nil
}

// AckStats acknowledges every statistic for hostID.
func AckStats(req *ingestv1.IngestStatsRequest, hostID int64) *ingestv1.IngestStatsResponse {
	resp := &ingestv1.IngestStatsResponse{}
	for i, stat := range req.GetStats() {
		resp.Acknowledgements = append(resp.Acknowledgements, &ingestv1.StatAcknowledgement{Id: int64(1000 + i), HostId: hostID, SourceId: stat.GetSourceId()})
	}
	return resp
}

// AckErrors acknowledges every error, inserted of them as new.
func AckErrors(req *ingestv1.IngestErrorsRequest, inserted int) *ingestv1.IngestErrorsResponse {
	ids := ErrorIDs(req)
	return &ingestv1.IngestErrorsResponse{
		Accepted: int32(len(ids)), Inserted: int32(inserted), Duplicates: int32(len(ids) - inserted), //nolint:gosec // G115: test batches are tiny
		SourceIds: ids,
	}
}

// StatIDs and ErrorIDs list the source IDs of a request, in order.
func StatIDs(req *ingestv1.IngestStatsRequest) []int64 {
	ids := make([]int64, 0, len(req.GetStats()))
	for _, stat := range req.GetStats() {
		ids = append(ids, stat.GetSourceId())
	}
	return ids
}

func ErrorIDs(req *ingestv1.IngestErrorsRequest) []int64 {
	ids := make([]int64, 0, len(req.GetErrors()))
	for _, entry := range req.GetErrors() {
		ids = append(ids, entry.GetSourceId())
	}
	return ids
}

// Reject is central's answer to one invalid record. The message is deliberately
// misleading free text: clients must decide only by the detail.
func Reject(sourceID int64, reason ingestv1.RejectionReason) error {
	st, err := status.New(codes.InvalidArgument, "echoed-private-credential").
		WithDetails(&ingestv1.RecordRejection{SourceId: sourceID, Reason: reason})
	if err != nil {
		panic(err)
	}
	return st.Err()
}

// RetryLater is a transient failure carrying RetryInfo.
func RetryLater(code codes.Code, delay time.Duration) error {
	st, err := status.New(code, "busy").WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(delay)})
	if err != nil {
		panic(err)
	}
	return st.Err()
}
