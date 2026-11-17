// Package cassandra implements app.HistoryStore: finished jobs, append-only,
// 30-day TTL. Request headers/body and response bodies are never archived,
// and the URL is stored without its query string.
package cassandra

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/gocql/gocql"

	"crobat/internal/domain"
)

const ttl = 30 * 24 * 3600

type History struct{ s *gocql.Session }

// Open connects and creates the keyspace and tables if missing.
func Open(hosts []string, keyspace string, replication int) (*History, error) {
	c := gocql.NewCluster(hosts...)
	c.Consistency = gocql.Quorum
	c.Timeout = 5 * time.Second
	boot, err := c.CreateSession()
	if err != nil {
		return nil, err
	}
	err = boot.Query(fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication =
		{'class': 'SimpleStrategy', 'replication_factor': %d}`, keyspace, replication)).Exec()
	boot.Close()
	if err != nil {
		return nil, err
	}
	c.Keyspace = keyspace
	s, err := c.CreateSession()
	if err != nil {
		return nil, err
	}
	for _, table := range []string{
		`CREATE TABLE IF NOT EXISTS job_history (tenant_id text, task_id text, month text, scheduled_at timestamp,
			job_id text, ` + cols + `, PRIMARY KEY ((tenant_id, task_id, month), scheduled_at, job_id))
			WITH CLUSTERING ORDER BY (scheduled_at DESC, job_id ASC)`,
		`CREATE TABLE IF NOT EXISTS job_by_id (tenant_id text, job_id text, task_id text, scheduled_at timestamp,
			` + cols + `, PRIMARY KEY ((tenant_id, job_id)))`,
	} {
		if err := s.Query(table).Exec(); err != nil {
			s.Close()
			return nil, err
		}
	}
	return &History{s}, nil
}

func (h *History) Close() { h.s.Close() }

const cols = `priority int, method text, url text, timeout_ms int, max_attempts int, backoff_ms int, status text,
	attempt int, http_status int, error text, created_at timestamp, started_at timestamp, finished_at timestamp`

const fields = `tenant_id, task_id, scheduled_at, job_id, priority, method, url, timeout_ms, max_attempts, backoff_ms,
	status, attempt, http_status, error, created_at, started_at, finished_at`

func month(t time.Time) string { return t.UTC().Format("2006-01") }

func stripQuery(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	u.RawQuery, u.Fragment, u.User = "", "", nil
	return u.String()
}

// Archive writes both tables. Writes are idempotent (same primary key), so a
// retry after a crash only overwrites the same row.
func (h *History) Archive(ctx context.Context, jobs []domain.Job) error {
	for _, j := range jobs {
		v := []any{j.TenantID, j.TaskID, j.ScheduledAt, j.ID, int(j.Priority), j.Request.Method, stripQuery(j.Request.URL),
			j.TimeoutMS, j.Retry.MaxAttempts, j.Retry.BackoffMS, string(j.Status), j.Attempt, j.HTTPStatus, j.Error,
			j.CreatedAt, j.StartedAt, j.FinishedAt}
		if err := h.s.Query(`INSERT INTO job_history (month, `+fields+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) USING TTL `+fmt.Sprint(ttl),
			append([]any{month(j.ScheduledAt)}, v...)...).WithContext(ctx).Exec(); err != nil {
			return err
		}
		if err := h.s.Query(`INSERT INTO job_by_id (`+fields+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) USING TTL `+fmt.Sprint(ttl),
			v...).WithContext(ctx).Exec(); err != nil {
			return err
		}
	}
	return nil
}

func scan(it *gocql.Iter) []domain.Job {
	var out []domain.Job
	for {
		var j domain.Job
		var prio int
		var status string
		var started, finished time.Time
		if !it.Scan(&j.TenantID, &j.TaskID, &j.ScheduledAt, &j.ID, &prio, &j.Request.Method, &j.Request.URL,
			&j.TimeoutMS, &j.Retry.MaxAttempts, &j.Retry.BackoffMS, &status, &j.Attempt, &j.HTTPStatus, &j.Error,
			&j.CreatedAt, &started, &finished) {
			return out
		}
		j.Priority, j.Status, j.Archived = domain.Priority(prio), domain.JobStatus(status), true
		if !started.IsZero() {
			j.StartedAt = &started
		}
		if !finished.IsZero() {
			j.FinishedAt = &finished
		}
		out = append(out, j)
	}
}

func (h *History) GetJob(ctx context.Context, tenantID, id string) (*domain.Job, error) {
	it := h.s.Query(`SELECT `+fields+` FROM job_by_id WHERE tenant_id = ? AND job_id = ?`, tenantID, id).WithContext(ctx).Iter()
	jobs := scan(it)
	if err := it.Close(); err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return nil, domain.ErrNotFound
	}
	return &jobs[0], nil
}

// ListForTask reads this month and the previous one (the TTL is 30 days).
func (h *History) ListForTask(ctx context.Context, tenantID, taskID string, limit int) ([]domain.Job, error) {
	now := time.Now().UTC()
	var out []domain.Job
	var errs []error
	for _, m := range []string{month(now), month(now.AddDate(0, -1, 0))} {
		it := h.s.Query(`SELECT `+fields+` FROM job_history WHERE tenant_id = ? AND task_id = ? AND month = ? LIMIT ?`,
			tenantID, taskID, m, limit).WithContext(ctx).Iter()
		out = append(out, scan(it)...)
		errs = append(errs, it.Close())
	}
	return out, errors.Join(errs...)
}
