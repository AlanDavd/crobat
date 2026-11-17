package cassandra

// Integration tests use their own keyspace and skip without a cluster:
//
//	CASSANDRA_TEST_HOSTS=localhost go test ./internal/adapter/cassandra/

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"crobat/internal/domain"
)

func TestStripQueryRemovesSecretsFromURLs(t *testing.T) {
	for in, want := range map[string]string{
		"https://api.e.com/hook?token=s3cret#frag": "https://api.e.com/hook",
		"https://user:pass@api.e.com/hook":         "https://api.e.com/hook",
		"http://e.com":                             "http://e.com",
		"://bad":                                   "",
	} {
		if got := stripQuery(in); got != want {
			t.Errorf("stripQuery(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMonthPartitionIsUTC(t *testing.T) {
	// 23:30 on Jan 31 in New York is already February in UTC.
	ny, _ := time.LoadLocation("America/New_York")
	if got := month(time.Date(2026, 1, 31, 23, 30, 0, 0, ny)); got != "2026-02" {
		t.Fatalf("month = %s", got)
	}
}

func TestArchiveAndRead(t *testing.T) {
	hosts := os.Getenv("CASSANDRA_TEST_HOSTS")
	if hosts == "" {
		t.Skip("CASSANDRA_TEST_HOSTS not set")
	}
	h, err := Open(strings.Split(hosts, ","), "crobat_test", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	ctx := context.Background()
	tenant, task := "t-"+domain.NewID(), domain.NewID()
	now := time.Now().UTC().Truncate(time.Millisecond)
	var jobs []domain.Job
	for i := range 3 {
		jobs = append(jobs, domain.Job{
			ID: domain.NewID(), TenantID: tenant, TaskID: task, ScheduledAt: now.Add(time.Duration(i) * time.Minute),
			Priority: domain.High, Status: domain.JobSucceeded, Attempt: 1, HTTPStatus: 200, CreatedAt: now,
			StartedAt: &now, FinishedAt: &now, ResponseBody: "RESPONSESECRET",
			Request: domain.Request{Method: "POST", URL: "https://e.com/hook?token=s3cret",
				Headers: map[string]string{"Authorization": "TOPSECRET"}, Body: "BODYSECRET"},
		})
	}
	if err := h.Archive(ctx, jobs); err != nil {
		t.Fatal(err)
	}
	if err := h.Archive(ctx, jobs[:1]); err != nil { // idempotent: a retry after a crash
		t.Fatal(err)
	}

	got, err := h.GetJob(ctx, tenant, jobs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Archived || got.Status != domain.JobSucceeded || got.Priority != domain.High || got.FinishedAt == nil {
		t.Fatalf("got %+v", got)
	}
	if got.Request.URL != "https://e.com/hook" || got.Request.Headers != nil || got.Request.Body != "" || got.ResponseBody != "" {
		t.Fatalf("secrets must never reach history: %+v", got.Request)
	}

	list, err := h.ListForTask(ctx, tenant, task, 10)
	if err != nil || len(list) != 3 {
		t.Fatalf("history has %d rows (err %v), want 3 with no duplicates", len(list), err)
	}
	if !list[0].ScheduledAt.After(list[2].ScheduledAt) {
		t.Fatal("history must be newest first")
	}
	if _, err := h.GetJob(ctx, "other-tenant", jobs[0].ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("tenant isolation: %v", err)
	}
}
