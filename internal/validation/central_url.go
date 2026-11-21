package validation

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// CentralEndpoint is a validated CENTRAL_GRPC_URL: the gRPC dial address and
// whether the connection uses TLS.
type CentralEndpoint struct {
	Address string
	TLS     bool
}

// CentralURL is the one rule for CENTRAL_GRPC_URL (ADR-0015), used by the
// configuration and by the gRPC client: https://host[:port] (default port 443),
// or http:// only on loopback, without credentials, path, query, or fragment.
func CentralURL(raw string) (CentralEndpoint, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Trim(u.Path, "/") != "" {
		return CentralEndpoint{}, fmt.Errorf("CENTRAL_GRPC_URL must be https://host[:port] (or http:// on loopback) without credentials, path, query, or fragment")
	}
	port := u.Port()
	switch u.Scheme {
	case "https":
		if port == "" {
			port = "443"
		}
		return CentralEndpoint{Address: net.JoinHostPort(u.Hostname(), port), TLS: true}, nil
	case "http":
		ip := net.ParseIP(u.Hostname())
		if !strings.EqualFold(u.Hostname(), "localhost") && (ip == nil || !ip.IsLoopback()) {
			return CentralEndpoint{}, fmt.Errorf("CENTRAL_GRPC_URL requires HTTPS outside loopback")
		}
		if port == "" {
			port = "80"
		}
		return CentralEndpoint{Address: net.JoinHostPort(u.Hostname(), port)}, nil
	default:
		return CentralEndpoint{}, fmt.Errorf("CENTRAL_GRPC_URL must use https:// (or http:// on loopback)")
	}
}
