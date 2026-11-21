package validation

import "testing"

func TestCentralURL(t *testing.T) {
	for raw, want := range map[string]CentralEndpoint{
		"https://central.example":       {Address: "central.example:443", TLS: true},
		"https://central.example:3334/": {Address: "central.example:3334", TLS: true},
		"http://localhost:3334":         {Address: "localhost:3334"},
		"http://127.0.0.2:3334":         {Address: "127.0.0.2:3334"}, // any loopback (config used to reject it)
		"http://[::1]":                  {Address: "[::1]:80"},
	} {
		if got, err := CentralURL(raw); err != nil || got != want {
			t.Errorf("%s: %+v %v", raw, got, err)
		}
	}
	for _, raw := range []string{
		"", "central.example:3334", "http://central.example:3334", "ftp://central.example",
		"https://central.example/ingest", // a path (config used to accept it; gRPC ignores it)
		"https://user:secret@central.example", "https://central.example?x=1", "https://central.example#f",
	} {
		if _, err := CentralURL(raw); err == nil {
			t.Errorf("%q accepted", raw)
		}
	}
}
