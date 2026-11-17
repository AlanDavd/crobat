// Package domain holds the core types and rules: tasks (configuration) and
// jobs (one execution of a task). No I/O lives here.
package domain

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

var (
	ErrNotFound = errors.New("not found")
	ErrInvalid  = errors.New("invalid")
)

func invalid(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, a...))
}

// Priority: lower value is served first.
type Priority int8

const (
	Critical Priority = iota
	High
	Normal
	Low
)

var priorityNames = [...]string{"critical", "high", "normal", "low"}

func (p Priority) String() string {
	if p < Critical || p > Low {
		return "unknown"
	}
	return priorityNames[p]
}

func (p Priority) MarshalText() ([]byte, error) { return []byte(p.String()), nil }

func (p *Priority) UnmarshalText(b []byte) error {
	for i, n := range priorityNames {
		if n == string(b) {
			*p = Priority(i)
			return nil
		}
	}
	return invalid("priority must be one of %v", priorityNames)
}

// Request is the HTTP call a job makes. Headers and Body may hold secrets:
// they are never serialized to JSON, encrypted at rest and never archived.
type Request struct {
	Method  string            `json:"method"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"-"`
	Body    string            `json:"-"`
}

type RetryPolicy struct {
	MaxAttempts int `json:"max_attempts"`
	BackoffMS   int `json:"backoff_ms"`
}

type TaskStatus string

const (
	TaskActive  TaskStatus = "active"
	TaskPaused  TaskStatus = "paused"
	TaskDeleted TaskStatus = "deleted"
)

// Task is the configuration. Schedule: Cron set => recurring; At set => once
// at that time; neither => once, now.
type Task struct {
	ID             string      `json:"id"`
	TenantID       string      `json:"tenant_id"`
	Name           string      `json:"name"`
	Cron           string      `json:"cron,omitempty"`
	Timezone       string      `json:"timezone,omitempty"`
	At             *time.Time  `json:"at,omitempty"`
	Priority       Priority    `json:"priority"`
	Request        Request     `json:"request"`
	TimeoutMS      int         `json:"timeout_ms"`
	Retry          RetryPolicy `json:"retry"`
	Status         TaskStatus  `json:"status"`
	NextRunAt      *time.Time  `json:"next_run_at,omitempty"`
	IdempotencyKey string      `json:"-"`
	Version        int         `json:"version"`
	CreatedAt      time.Time   `json:"created_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
}

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

var allowedMethods = map[string]bool{"GET": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true}

// Normalize fills defaults and validates. Call before saving.
func (t *Task) Normalize() error {
	t.Name = strings.TrimSpace(t.Name)
	if t.Name == "" || len(t.Name) > 255 {
		return invalid("name is required (max 255 chars)")
	}
	if t.Cron != "" && t.At != nil {
		return invalid("set cron or at, not both")
	}
	if t.Timezone == "" {
		t.Timezone = "UTC"
	}
	if _, err := time.LoadLocation(t.Timezone); err != nil {
		return invalid("unknown timezone %q", t.Timezone)
	}
	if t.Cron != "" {
		if _, err := cronParser.Parse(t.Cron); err != nil {
			return invalid("bad cron expression: %v", err)
		}
	}
	if t.Priority < Critical || t.Priority > Low {
		return invalid("bad priority")
	}
	t.Request.Method = strings.ToUpper(t.Request.Method)
	if t.Request.Method == "" {
		t.Request.Method = "POST"
	}
	if !allowedMethods[t.Request.Method] {
		return invalid("method %q not allowed", t.Request.Method)
	}
	u, err := url.Parse(t.Request.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || len(t.Request.URL) > 2048 {
		return invalid("url must be an absolute http(s) URL")
	}
	if t.TimeoutMS == 0 {
		t.TimeoutMS = 10_000
	}
	if t.TimeoutMS < 100 || t.TimeoutMS > 300_000 {
		return invalid("timeout_ms must be between 100 and 300000")
	}
	if t.Retry.MaxAttempts == 0 {
		t.Retry.MaxAttempts = 3
	}
	if t.Retry.MaxAttempts < 1 || t.Retry.MaxAttempts > 25 {
		return invalid("retry.max_attempts must be between 1 and 25")
	}
	if t.Retry.BackoffMS == 0 {
		t.Retry.BackoffMS = 1_000
	}
	if t.Retry.BackoffMS < 100 || t.Retry.BackoffMS > 3_600_000 {
		return invalid("retry.backoff_ms must be between 100 and 3600000")
	}
	return nil
}

// FirstRun is when the task is first due, given the current time.
func (t *Task) FirstRun(now time.Time) *time.Time {
	switch {
	case t.Cron != "":
		return t.NextAfter(now)
	case t.At != nil:
		at := t.At.UTC()
		return &at
	default:
		return &now
	}
}

// NextAfter is the run following prev, or nil for one-shot tasks. Computed
// from the previous run, not from now, so runs missed in an outage catch up.
func (t *Task) NextAfter(prev time.Time) *time.Time {
	if t.Cron == "" {
		return nil
	}
	sched, err := cronParser.Parse(t.Cron)
	if err != nil {
		return nil
	}
	loc, err := time.LoadLocation(t.Timezone)
	if err != nil {
		loc = time.UTC
	}
	next := sched.Next(prev.In(loc)).UTC()
	if next.IsZero() {
		return nil
	}
	return &next
}

// NewJob plans one execution. It copies the request, timeout, retry policy
// and priority so later task edits never change an existing job.
func (t *Task) NewJob(at time.Time) Job {
	return Job{
		ID:            NewID(),
		TenantID:      t.TenantID,
		TaskID:        t.ID,
		ScheduledAt:   at,
		Priority:      t.Priority,
		Request:       t.Request,
		TimeoutMS:     t.TimeoutMS,
		Retry:         t.Retry,
		Status:        JobPending,
		NextAttemptAt: at,
	}
}

type JobStatus string

const (
	JobPending   JobStatus = "pending"
	JobRunning   JobStatus = "running"
	JobSucceeded JobStatus = "succeeded"
	JobDead      JobStatus = "dead"
)

// Job is one execution of a task. Its ID is the Idempotency-Key sent to the
// endpoint on every attempt.
type Job struct {
	ID             string      `json:"id"`
	TenantID       string      `json:"tenant_id"`
	TaskID         string      `json:"task_id"`
	ScheduledAt    time.Time   `json:"scheduled_at"`
	Priority       Priority    `json:"priority"`
	Request        Request     `json:"request"`
	TimeoutMS      int         `json:"timeout_ms"`
	Retry          RetryPolicy `json:"retry"`
	Status         JobStatus   `json:"status"`
	Attempt        int         `json:"attempt"`
	NextAttemptAt  time.Time   `json:"next_attempt_at"`
	WorkerID       string      `json:"worker_id,omitempty"`
	LeaseExpiresAt *time.Time  `json:"lease_expires_at,omitempty"`
	HTTPStatus     int         `json:"http_status,omitempty"`
	Error          string      `json:"error,omitempty"`
	ResponseBody   string      `json:"response_body,omitempty"`
	CreatedAt      time.Time   `json:"created_at"`
	StartedAt      *time.Time  `json:"started_at,omitempty"`
	FinishedAt     *time.Time  `json:"finished_at,omitempty"`
	Archived       bool        `json:"archived,omitempty"`
}

// Result of one HTTP attempt.
type Result struct {
	HTTPStatus int
	Body       string
	Err        string
	StartedAt  time.Time
	FinishedAt time.Time
}

func (r Result) OK() bool { return r.Err == "" && r.HTTPStatus >= 200 && r.HTTPStatus < 300 }

// Apply moves a running job to its next state: succeeded, pending with
// backoff, or dead once its attempts are used up.
func (j *Job) Apply(r Result) {
	j.HTTPStatus, j.ResponseBody, j.Error = r.HTTPStatus, r.Body, r.Err
	if j.Error == "" && !r.OK() {
		j.Error = fmt.Sprintf("http status %d", r.HTTPStatus)
	}
	started, finished := r.StartedAt, r.FinishedAt
	j.StartedAt = &started
	j.WorkerID, j.LeaseExpiresAt = "", nil
	switch {
	case r.OK():
		j.Status, j.FinishedAt, j.Error = JobSucceeded, &finished, ""
	case j.Attempt >= j.Retry.MaxAttempts:
		j.Status, j.FinishedAt = JobDead, &finished
	default:
		j.Status = JobPending
		j.NextAttemptAt = finished.Add(Backoff(j.Retry.BackoffMS, j.Attempt))
	}
}

// Backoff is exponential: base * 2^(attempt-1), capped at one hour.
func Backoff(baseMS, attempt int) time.Duration {
	d := float64(baseMS) * math.Pow(2, float64(attempt-1))
	return time.Duration(min(d, float64(time.Hour/time.Millisecond))) * time.Millisecond
}

// Candidate is a claimable job as seen by the claim query.
type Candidate struct{ ID, TenantID string }

// FairPick takes up to n candidates in the given preference order, giving
// each tenant at most an equal share of the batch first, then filling any
// slots left over (work-conserving).
func FairPick(cands []Candidate, n int) []string {
	seen := map[string]bool{}
	tenants := map[string]int{}
	var uniq []Candidate
	for _, c := range cands {
		if !seen[c.ID] {
			seen[c.ID] = true
			uniq = append(uniq, c)
			tenants[c.TenantID] = 0
		}
	}
	if len(uniq) == 0 || n <= 0 {
		return nil
	}
	share := (n + len(tenants) - 1) / len(tenants)
	picked := map[string]bool{}
	var out []string
	for pass := 0; pass < 2 && len(out) < n; pass++ {
		for _, c := range uniq {
			if len(out) == n {
				break
			}
			if picked[c.ID] || (pass == 0 && tenants[c.TenantID] >= share) {
				continue
			}
			picked[c.ID] = true
			tenants[c.TenantID]++
			out = append(out, c.ID)
		}
	}
	return out
}

// NewID returns a random UUIDv4.
func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
