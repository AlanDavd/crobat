package domain

import (
	"testing"
	"time"
)

func TestNextAfterCatchesUpFromPrevious(t *testing.T) {
	task := Task{Cron: "0 9 * * *", Timezone: "UTC"}
	prev := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	got := task.NextAfter(prev)
	if want := prev.Add(24 * time.Hour); !got.Equal(want) {
		t.Fatalf("next = %v, want %v", got, want)
	}
	task.Timezone = "America/New_York" // 09:00 NY = 14:00 UTC in January
	if got := task.NextAfter(prev); got.Hour() != 14 {
		t.Fatalf("tz next = %v, want 14:00 UTC", got)
	}
	if (&Task{}).NextAfter(prev) != nil {
		t.Fatal("one-shot task must have no next run")
	}
}

func TestApplyRetriesThenDies(t *testing.T) {
	now := time.Now()
	j := Job{Status: JobRunning, Attempt: 1, Retry: RetryPolicy{MaxAttempts: 2, BackoffMS: 1000}}
	j.Apply(Result{HTTPStatus: 500, FinishedAt: now})
	if j.Status != JobPending || !j.NextAttemptAt.Equal(now.Add(time.Second)) {
		t.Fatalf("attempt 1: %s at %v", j.Status, j.NextAttemptAt)
	}
	j.Attempt = 2
	j.Apply(Result{Err: "timeout", FinishedAt: now})
	if j.Status != JobDead || j.FinishedAt == nil {
		t.Fatalf("attempt 2: %s", j.Status)
	}
	j.Attempt = 3
	j.Apply(Result{HTTPStatus: 204, FinishedAt: now})
	if j.Status != JobSucceeded || j.Error != "" {
		t.Fatalf("success: %s %q", j.Status, j.Error)
	}
}

func TestBackoffCaps(t *testing.T) {
	if Backoff(1000, 3) != 4*time.Second {
		t.Fatal(Backoff(1000, 3))
	}
	if Backoff(1000, 40) != time.Hour {
		t.Fatal(Backoff(1000, 40))
	}
}

func TestFairPick(t *testing.T) {
	var c []Candidate
	for i := range 10 {
		c = append(c, Candidate{ID: string(rune('a' + i)), TenantID: "big"})
	}
	c = append(c, Candidate{ID: "x", TenantID: "small"}, Candidate{ID: "a", TenantID: "big"}) // dup id
	got := FairPick(c, 4)
	if len(got) != 4 || got[2] != "x" {
		t.Fatalf("small tenant should get a slot in the first pass: %v", got)
	}
	if got := FairPick(c[:3], 5); len(got) != 3 {
		t.Fatalf("must not invent jobs: %v", got)
	}
}
