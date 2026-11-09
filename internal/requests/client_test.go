package requests

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"monitor-remote/internal/models"
	"monitor-remote/internal/validation"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestStatAcknowledgementAndSourceID(t *testing.T) {
	for _, tc := range []struct {
		name, body, contentType string
		status                  int
		valid                   bool
	}{
		{"valid", `{"data":[{"id":9,"host_id":7,"source_id":42}]}`, "application/json", 201, true},
		{"HTML", "<html>login</html>", "text/html", 201, false},
		{"empty", "", "application/json", 201, false},
		{"wrong status", `{"data":[{"id":9,"host_id":7,"source_id":42}]}`, "application/json", 200, false},
		{"wrong source", `{"data":[{"id":9,"host_id":7,"source_id":43}]}`, "application/json", 201, false},
		{"missing IDs", `{}`, "application/json", 201, false},
		{"trailing JSON", `{"data":[{"id":9,"host_id":7,"source_id":42}]} {}`, "application/json", 201, false},
		{"oversized", strings.Repeat(" ", 16385), "application/json", 201, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewClient("https://central.example", "test-key")
			if err != nil {
				t.Fatal(err)
			}
			client.httpClient.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
				var payload struct {
					Stats []struct {
						ID int64 `json:"id"`
					} `json:"stats"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || len(payload.Stats) != 1 || payload.Stats[0].ID != 42 {
					t.Fatal("persistent numeric source ID missing", err)
				}
				if r.Header.Get("X-API-Key") != "test-key" {
					t.Fatal("missing authentication")
				}
				return &http.Response{StatusCode: tc.status, Header: http.Header{"Content-Type": {tc.contentType}}, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})
			err = client.SendMachineStats(context.Background(), []models.MachineStat{validStat(42)})
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%t err=%v", tc.valid, err)
			}
		})
	}
}

func TestRedirectsNeverForwardCredentials(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		client, err := NewClient("https://central.example", "test-secret")
		if err != nil {
			t.Fatal(err)
		}
		calls := 0
		client.httpClient.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			if r.URL.Host != "central.example" {
				t.Fatal("credential-bearing request followed redirect")
			}
			return &http.Response{StatusCode: code, Header: http.Header{"Location": {"http://other.example/collect"}}, Body: io.NopCloser(strings.NewReader(""))}, nil
		})
		if err := client.SendMachineStats(context.Background(), []models.MachineStat{validStat(1)}); err == nil || calls != 1 {
			t.Fatal("redirect must fail with a single request")
		}
	}
}

func TestErrorIDsAndCompleteAcknowledgement(t *testing.T) {
	for _, response := range []struct {
		body  string
		valid bool
	}{
		{`{"accepted":1,"inserted":1,"duplicates":0,"source_ids":[12]}`, true},
		{`{"accepted":1,"inserted":0,"duplicates":1,"source_ids":[12]}`, true},
		{`{"accepted":0,"inserted":0,"duplicates":0}`, false},
		{`{"accepted":1}`, false},
		{`{"accepted":1,"inserted":1,"duplicates":0,"source_ids":[13]}`, false},
	} {
		client, err := NewClient("https://central.example", "test-key")
		if err != nil {
			t.Fatal(err)
		}
		client.httpClient.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
			var payload struct {
				Errors []struct {
					ID int64 `json:"id"`
				} `json:"errors"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || len(payload.Errors) != 1 || payload.Errors[0].ID != 12 {
				t.Fatal("numeric error contract mismatch", err)
			}
			return &http.Response{StatusCode: 201, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(response.body))}, nil
		})
		err = client.SendErrorLogs(context.Background(), []models.ErrorLog{validError(12)})
		if (err == nil) != response.valid {
			t.Fatal("unexpected acknowledgement result", err)
		}
	}
}

func validStat(id int64) models.MachineStat {
	return models.MachineStat{ID: id, CPUPercent: 25, RAMUsedMB: 1, RAMTotalMB: 2, DiskUsedGB: 1, DiskTotalGB: 2, CollectedAt: time.Now().UTC().Add(-time.Second)}
}

func validError(id int64) models.ErrorLog {
	return models.ErrorLog{ID: id, AppName: "app", Environment: "test", Level: "ERROR", Message: "failure", CreatedAt: time.Now().UTC().Add(-time.Second)}
}

func TestFullBatchesUseOneRequestAndRetainNumericIDs(t *testing.T) {
	for _, kind := range []string{"stats", "errors"} {
		t.Run(kind, func(t *testing.T) {
			client, err := NewClient("https://central.example", "test-key")
			if err != nil {
				t.Fatal(err)
			}
			var stats []models.MachineStat
			var logs []models.ErrorLog
			for i := int64(0); i < 100; i++ {
				id := int64(1<<63-1) - i
				stats = append(stats, validStat(id))
				logs = append(logs, validError(id))
			}
			calls := 0
			client.httpClient.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				var payload struct {
					Stats  []machineStatusPayload `json:"stats"`
					Errors []errorLogPayload      `json:"errors"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Fatal(err)
				}
				var ack any
				if kind == "stats" {
					if len(payload.Stats) != 100 {
						t.Fatal("statistics were not batched")
					}
					data := make([]map[string]int64, 0, 100)
					for i, stat := range payload.Stats {
						if stat.ID != stats[i].ID {
							t.Fatal("source ID or order changed")
						}
						data = append(data, map[string]int64{"id": stat.ID, "host_id": 1<<63 - 1, "source_id": stat.ID})
					}
					ack = map[string]any{"data": data}
				} else {
					if len(payload.Errors) != 100 {
						t.Fatal("errors were not batched")
					}
					ids := make([]int64, 0, 100)
					for i, entry := range payload.Errors {
						if entry.ID != logs[i].ID {
							t.Fatal("source ID or order changed")
						}
						ids = append(ids, entry.ID)
					}
					ack = map[string]any{"accepted": 100, "inserted": 50, "duplicates": 50, "source_ids": ids}
				}
				body, err := json.Marshal(ack)
				if err != nil {
					t.Fatal(err)
				}
				return &http.Response{StatusCode: 201, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
			})
			if kind == "stats" {
				err = client.SendMachineStats(context.Background(), stats)
			} else {
				err = client.SendErrorLogs(context.Background(), logs)
			}
			if err != nil || calls != 1 {
				t.Fatal("expected one HTTP request for 100 records", calls, err)
			}
		})
	}
}

func TestBatchAcknowledgementCannotRepeatOrOmitMembers(t *testing.T) {
	for _, tc := range []struct{ kind, body string }{
		{"stats", `{"data":[{"id":9,"host_id":7,"source_id":2}]}`},
		{"stats", `{"data":[{"id":9,"host_id":7,"source_id":2},{"id":10,"host_id":7,"source_id":2}]}`},
		{"stats", `{"data":[{"id":9,"host_id":7,"source_id":2},{"id":9,"host_id":7,"source_id":1}]}`},
		{"stats", `{"data":[{"id":9,"host_id":7,"source_id":2},{"id":10,"host_id":8,"source_id":1}]}`},
		{"errors", `{"accepted":2,"inserted":2,"duplicates":0,"source_ids":[2]}`},
		{"errors", `{"accepted":2,"inserted":2,"duplicates":0,"source_ids":[2,2]}`},
		{"errors", `{"accepted":2,"inserted":2,"duplicates":0,"source_ids":[2,3]}`},
	} {
		client, err := NewClient("https://central.example", "test-key")
		if err != nil {
			t.Fatal(err)
		}
		client.httpClient.Transport = transportFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 201, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
		})
		if tc.kind == "stats" {
			err = client.SendMachineStats(context.Background(), []models.MachineStat{validStat(2), validStat(1)})
		} else {
			err = client.SendErrorLogs(context.Background(), []models.ErrorLog{validError(2), validError(1)})
		}
		if err == nil {
			t.Fatal("incomplete or inconsistent batch acknowledgement accepted", tc.body)
		}
	}
}

func TestFailureClassificationDoesNotQuarantineDeliveryProblems(t *testing.T) {
	for _, tc := range []struct {
		name, body    string
		status        int
		record, pause bool
	}{
		{"record rejected", `{"error":"invalid_data","message":"timestamp cannot be in the future","source_id":42}`, 400, true, false},
		{"unscoped rejection", `{"error":"invalid_data","message":"timestamp cannot be in the future"}`, 400, false, true},
		{"contract mismatch", `{"error":"invalid_request"}`, 400, false, true},
		{"plain proxy rejection", `upstream error`, 400, false, true},
		{"credential rejected", "", 401, false, true},
		{"access denied", "", 403, false, true},
		{"wrong endpoint", "", 404, false, true},
		{"temporary failure", "", 503, false, false},
		{"rate limited", "", 429, false, false},
		{"unexpected success", "", 200, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := responseFailure(&http.Response{StatusCode: tc.status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(tc.body))})
			var record *validation.RecordError
			if errors.As(err, &record) != tc.record {
				t.Fatalf("unexpected record classification: %v", err)
			}
			if !tc.record {
				var delivery *DeliveryError
				if !errors.As(err, &delivery) || delivery.Pause != tc.pause {
					t.Fatalf("unexpected delivery classification: %v", err)
				}
			}
		})
	}
}

func TestRetryAfterAndSafeReason(t *testing.T) {
	if err := responseFailure(&http.Response{StatusCode: 413, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"error":"body_too_large"}`))}); !errors.Is(err, ErrBatchBodyTooLarge) {
		t.Fatal("oversized batch must split instead of quarantining every member", err)
	}
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{
		{"60", time.Minute}, {"-1", 0}, {"invalid", 0},
		{now.Add(2 * time.Minute).Format(http.TimeFormat), 2 * time.Minute},
		{now.Add(-time.Minute).Format(http.TimeFormat), 0},
	} {
		if got := retryAfter(tc.value, now); got != tc.want {
			t.Fatalf("Retry-After %q: %s, want %s", tc.value, got, tc.want)
		}
	}
	if reason := safeRecordReason("echoed-private-credential"); strings.Contains(reason, "private") {
		t.Fatal("untrusted response text persisted")
	}
}

func TestClientRejectsUnsafeURLs(t *testing.T) {
	for _, base := range []string{"http://remote.example", "https://user:pass@central.example", "https://central.example?x=y", "https://central.example#fragment"} {
		if _, err := NewClient(base, "key"); err == nil {
			t.Fatalf("unsafe URL accepted: %s", base)
		}
	}
}
