// Package requests is the agent's client for the central ingestion service. The
// wire contract (messages, limits and error codes) is defined in
// github.com/ads-iscte-rg-ja/monitor-api (ADR-0003); this package translates the
// local records into it and classifies the outcome for the sender.
package requests

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	ingestv1 "github.com/ads-iscte-rg-ja/monitor-api/gen/go/monitor/ingest/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// MaxBatchRecords is the contract's maximum number of records per request.
const MaxBatchRecords = ingestv1.MaxBatchRecords

// callTimeout bounds one delivery attempt, as the HTTP client timeout did.
const callTimeout = 15 * time.Second

// ErrBatchBodyTooLarge means the encoded batch exceeds the contract's message
// limit; the sender splits the batch instead of quarantining its members.
var ErrBatchBodyTooLarge = errors.New("telemetry batch exceeds the request message limit")

// Client sends telemetry batches to central over gRPC.
type Client struct {
	api  ingestv1.IngestServiceClient
	conn *grpc.ClientConn

	mu     sync.RWMutex
	apiKey string
}

// SetAPIKey replaces the host key for the next requests (SIGHUP, ADR-0011).
func (c *Client) SetAPIKey(apiKey string) error {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return fmt.Errorf("host API key is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.apiKey = apiKey
	return nil
}

func (c *Client) key() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.apiKey
}

// NewClient connects to central at rawURL: https://host[:port] (TLS; the default
// port is 443) or, on loopback only, http://localhost[:port] without TLS.
func NewClient(rawURL, apiKey string) (*Client, error) {
	target, creds, err := parseTarget(rawURL)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(apiKey) == "" {
		return nil, fmt.Errorf("host API key is required")
	}
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(ingestv1.MaxMessageBytes)),
	)
	if err != nil {
		return nil, fmt.Errorf("create central connection: %w", err)
	}
	return &Client{api: ingestv1.NewIngestServiceClient(conn), apiKey: apiKey, conn: conn}, nil
}

// NewClientConn uses an existing connection (tests use an in-memory one).
func NewClientConn(conn grpc.ClientConnInterface, apiKey string) *Client {
	return &Client{api: ingestv1.NewIngestServiceClient(conn), apiKey: apiKey}
}

// Close releases the connection created by NewClient.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func parseTarget(rawURL string) (string, credentials.TransportCredentials, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Trim(u.Path, "/") != "" {
		return "", nil, fmt.Errorf("CENTRAL_GRPC_URL must be https://host[:port] (or http:// on loopback) without credentials, path, query, or fragment")
	}
	port := u.Port()
	switch u.Scheme {
	case "https":
		if port == "" {
			port = "443"
		}
		return net.JoinHostPort(u.Hostname(), port), credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}), nil
	case "http":
		ip := net.ParseIP(u.Hostname())
		if !strings.EqualFold(u.Hostname(), "localhost") && (ip == nil || !ip.IsLoopback()) {
			return "", nil, fmt.Errorf("CENTRAL_GRPC_URL requires HTTPS outside loopback")
		}
		if port == "" {
			port = "80"
		}
		return net.JoinHostPort(u.Hostname(), port), insecure.NewCredentials(), nil
	default:
		return "", nil, fmt.Errorf("CENTRAL_GRPC_URL must use https:// (or http:// on loopback)")
	}
}

// call runs one RPC with the host credential and the contract's size limit.
func call[Req proto.Message, Resp any](ctx context.Context, c *Client, req Req, rpc func(context.Context, Req, ...grpc.CallOption) (Resp, error)) (Resp, error) {
	var zero Resp
	if proto.Size(req) > ingestv1.MaxMessageBytes {
		return zero, ErrBatchBodyTooLarge
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	resp, err := rpc(metadata.AppendToOutgoingContext(ctx, ingestv1.APIKeyMetadata, c.key()), req)
	if err != nil {
		return zero, classify(err)
	}
	return resp, nil
}
