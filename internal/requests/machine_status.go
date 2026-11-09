package requests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"monitor-remote/internal/models"
	"monitor-remote/internal/validation"
)

type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

const MaxBatchRecords = 100

var ErrBatchBodyTooLarge = errors.New("telemetry batch exceeds the request body limit")

type machineStatusPayload struct {
	ID          int64     `json:"id"`
	CPUPercent  float64   `json:"cpu_percent"`
	RAMUsedMB   float64   `json:"ram_used_mb"`
	RAMTotalMB  float64   `json:"ram_total_mb"`
	DiskUsedGB  float64   `json:"disk_used_gb"`
	DiskTotalGB float64   `json:"disk_total_gb"`
	CollectedAt time.Time `json:"collected_at"`
}

func NewClient(baseURL, apiKey string) (*Client, error) {
	u, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, fmt.Errorf("CENTRAL_URL must be an HTTP(S) origin or base path without credentials, query, or fragment")
	}
	if u.Scheme != "https" {
		ip := net.ParseIP(u.Hostname())
		if !strings.EqualFold(u.Hostname(), "localhost") && (ip == nil || !ip.IsLoopback()) {
			return nil, fmt.Errorf("CENTRAL_URL requires HTTPS outside loopback")
		}
	}
	if strings.TrimSpace(apiKey) == "" {
		return nil, fmt.Errorf("host API key is required")
	}
	return &Client{
		baseURL: strings.TrimRight(u.String(), "/"), apiKey: apiKey,
		httpClient: &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}, nil
}

func (c *Client) SendMachineStats(ctx context.Context, stats []models.MachineStat) error {
	if len(stats) == 0 || len(stats) > MaxBatchRecords {
		return fmt.Errorf("statistics batch must contain 1 to %d records", MaxBatchRecords)
	}
	payload := struct {
		Stats []machineStatusPayload `json:"stats"`
	}{Stats: make([]machineStatusPayload, 0, len(stats))}
	want := make(map[int64]bool, len(stats))
	for _, stat := range stats {
		if err := validation.MachineStat(stat, time.Now().UTC()); err != nil {
			return &validation.RecordError{SourceID: stat.ID, Reason: err.Error()}
		}
		if want[stat.ID] {
			return fmt.Errorf("duplicate statistics source ID in batch")
		}
		want[stat.ID] = true
		payload.Stats = append(payload.Stats, machineStatusPayload{ID: stat.ID, CPUPercent: stat.CPUPercent, RAMUsedMB: stat.RAMUsedMB, RAMTotalMB: stat.RAMTotalMB, DiskUsedGB: stat.DiskUsedGB, DiskTotalGB: stat.DiskTotalGB, CollectedAt: stat.CollectedAt.UTC()})
	}
	var ack struct {
		Data []struct {
			ID       int64 `json:"id"`
			HostID   int64 `json:"host_id"`
			SourceID int64 `json:"source_id"`
		} `json:"data"`
	}
	if err := c.postJSON(ctx, "/api/v1/ingest/stats", payload, &ack); err != nil {
		return err
	}
	if len(ack.Data) != len(stats) {
		return fmt.Errorf("central did not acknowledge the complete statistics batch")
	}
	var hostID int64
	centralIDs := make(map[int64]bool, len(stats))
	for _, entry := range ack.Data {
		if entry.ID <= 0 || entry.HostID <= 0 || !want[entry.SourceID] || centralIDs[entry.ID] || (hostID != 0 && hostID != entry.HostID) {
			return fmt.Errorf("central acknowledgement does not match the submitted statistics source IDs")
		}
		hostID = entry.HostID
		centralIDs[entry.ID] = true
		delete(want, entry.SourceID)
	}
	return nil
}

func (c *Client) postJSON(ctx context.Context, path string, payload, ack any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode telemetry batch: %w", err)
	}
	if len(body) > 1048576 {
		return ErrBatchBodyTooLarge
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create POST request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.apiKey)
	response, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send POST request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		return responseFailure(response)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return fmt.Errorf("central acknowledgement must be application/json")
	}
	// 100 compact acknowledgements can exceed 8 KiB with large numeric IDs.
	const maxAcknowledgement = 16 * 1024
	encoded, err := io.ReadAll(io.LimitReader(response.Body, maxAcknowledgement+1))
	if err != nil {
		return fmt.Errorf("read central acknowledgement: %w", err)
	}
	if len(encoded) > maxAcknowledgement {
		return fmt.Errorf("central acknowledgement exceeds 16 KiB")
	}
	if err := json.Unmarshal(encoded, ack); err != nil {
		return fmt.Errorf("invalid central acknowledgement: %w", err)
	}
	return nil
}
