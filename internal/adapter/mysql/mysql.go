// Package mysql implements app.TaskStore and app.JobStore. Request headers
// and body are encrypted (AES-256-GCM) before they reach a row.
package mysql

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"crobat/internal/domain"
)

type Store struct {
	db   *sql.DB
	aead cipher.AEAD
}

// Open connects to MySQL. key is the 32-byte secrets key.
func Open(dsn string, key []byte) (*Store, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ParseTime, cfg.Loc = true, time.UTC
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(50)
	db.SetConnMaxLifetime(5 * time.Minute)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("secrets key: %w", err)
	}
	aead, _ := cipher.NewGCM(block)
	return &Store{db: db, aead: aead}, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// ------------------------------------------------------------------- secrets

type secret struct {
	Headers map[string]string `json:"h,omitempty"`
	Body    string            `json:"b,omitempty"`
}

func (s *Store) seal(r domain.Request) []byte {
	if len(r.Headers) == 0 && r.Body == "" {
		return nil
	}
	plain, _ := json.Marshal(secret{r.Headers, r.Body})
	nonce := make([]byte, s.aead.NonceSize())
	_, _ = rand.Read(nonce)
	return s.aead.Seal(nonce, nonce, plain, nil)
}

func (s *Store) open(blob []byte, r *domain.Request) error {
	if len(blob) == 0 {
		return nil
	}
	n := s.aead.NonceSize()
	if len(blob) < n {
		return errors.New("secret: ciphertext too short")
	}
	plain, err := s.aead.Open(nil, blob[:n], blob[n:], nil)
	if err != nil {
		return errors.New("secret: cannot decrypt (wrong SECRETS_KEY?)")
	}
	var sec secret
	if err := json.Unmarshal(plain, &sec); err != nil {
		return err
	}
	r.Headers, r.Body = sec.Headers, sec.Body
	return nil
}

// --------------------------------------------------------------------- tasks

const taskCols = `id, tenant_id, name, cron_expr, timezone, run_at, priority, method, url, secret,
	timeout_ms, max_attempts, backoff_ms, status, next_run_at, idempotency_key, version, created_at, updated_at`

type scanner interface{ Scan(...any) error }

func (s *Store) scanTask(row scanner) (*domain.Task, error) {
	var t domain.Task
	var cronExpr, idem sql.NullString
	var sec []byte
	err := row.Scan(&t.ID, &t.TenantID, &t.Name, &cronExpr, &t.Timezone, &t.At, &t.Priority,
		&t.Request.Method, &t.Request.URL, &sec, &t.TimeoutMS, &t.Retry.MaxAttempts, &t.Retry.BackoffMS,
		&t.Status, &t.NextRunAt, &idem, &t.Version, &t.CreatedAt, &t.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	t.Cron, t.IdempotencyKey = cronExpr.String, idem.String
	return &t, s.open(sec, &t.Request)
}

func null(s string) sql.NullString { return sql.NullString{String: s, Valid: s != ""} }

func (s *Store) CreateTask(ctx context.Context, t *domain.Task, first *domain.Job) (*domain.Task, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO tasks (`+taskCols+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		t.ID, t.TenantID, t.Name, null(t.Cron), t.Timezone, t.At, t.Priority, t.Request.Method, t.Request.URL,
		s.seal(t.Request), t.TimeoutMS, t.Retry.MaxAttempts, t.Retry.BackoffMS, t.Status, t.NextRunAt,
		null(t.IdempotencyKey), t.Version, t.CreatedAt, t.UpdatedAt)
	var me *mysql.MySQLError
	if errors.As(err, &me) && me.Number == 1062 && t.IdempotencyKey != "" {
		// Same POST again: return what the first one created.
		row := s.db.QueryRowContext(ctx, `SELECT `+taskCols+` FROM tasks WHERE tenant_id = ? AND idempotency_key = ?`,
			t.TenantID, t.IdempotencyKey)
		return s.scanTask(row)
	}
	if err != nil {
		return nil, err
	}
	if first != nil {
		if err := s.insertJobs(ctx, tx, []domain.Job{*first}, t.CreatedAt); err != nil {
			return nil, err
		}
	}
	return t, tx.Commit()
}

func (s *Store) GetTask(ctx context.Context, tenantID, id string) (*domain.Task, error) {
	return s.scanTask(s.db.QueryRowContext(ctx,
		`SELECT `+taskCols+` FROM tasks WHERE id = ? AND tenant_id = ?`, id, tenantID))
}

func (s *Store) ListTasks(ctx context.Context, tenantID string, limit int) ([]domain.Task, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+taskCols+` FROM tasks
		WHERE tenant_id = ? AND status <> 'deleted' ORDER BY created_at DESC LIMIT ?`, tenantID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Task
	for rows.Next() {
		t, err := s.scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *t)
	}
	return out, rows.Err()
}

func (s *Store) UpdateTask(ctx context.Context, t *domain.Task) error {
	res, err := s.db.ExecContext(ctx, `UPDATE tasks SET name=?, cron_expr=?, timezone=?, run_at=?, priority=?,
		method=?, url=?, secret=?, timeout_ms=?, max_attempts=?, backoff_ms=?, status=?, next_run_at=?,
		version=version+1, updated_at=? WHERE id=? AND tenant_id=? AND version=?`,
		t.Name, null(t.Cron), t.Timezone, t.At, t.Priority, t.Request.Method, t.Request.URL, s.seal(t.Request),
		t.TimeoutMS, t.Retry.MaxAttempts, t.Retry.BackoffMS, t.Status, t.NextRunAt, t.UpdatedAt,
		t.ID, t.TenantID, t.Version)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// The scheduler advanced next_run_at, or another edit won the race.
		return fmt.Errorf("%w: task changed concurrently, retry", domain.ErrInvalid)
	}
	t.Version++
	return nil
}

func (s *Store) ScheduleDue(ctx context.Context, horizon time.Time, limit int, plan func(*domain.Task) []domain.Job) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT `+taskCols+` FROM tasks
		WHERE status = 'active' AND next_run_at <= ? ORDER BY next_run_at LIMIT ? FOR UPDATE SKIP LOCKED`, horizon, limit)
	if err != nil {
		return 0, err
	}
	var tasks []*domain.Task
	for rows.Next() {
		t, err := s.scanTask(rows)
		if err != nil {
			rows.Close()
			return 0, err
		}
		tasks = append(tasks, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	var jobs []domain.Job
	for _, t := range tasks {
		jobs = append(jobs, plan(t)...)
	}
	if err := s.insertJobs(ctx, tx, jobs, now); err != nil {
		return 0, err
	}
	// ponytail: one UPDATE per task; batch with CASE if this shows up in profiles.
	// version is bumped so a concurrent edit (read before this tick) is rejected.
	for _, t := range tasks {
		if _, err := tx.ExecContext(ctx, `UPDATE tasks SET next_run_at = ?, version = version + 1 WHERE id = ?`,
			t.NextRunAt, t.ID); err != nil {
			return 0, err
		}
	}
	return len(tasks), tx.Commit()
}

// ---------------------------------------------------------------------- jobs

const jobCols = `id, tenant_id, task_id, scheduled_at, priority, method, url, secret, timeout_ms, max_attempts,
	backoff_ms, status, attempt, next_attempt_at, worker_id, lease_expires_at, http_status, error, response_body,
	created_at, started_at, finished_at`

func (s *Store) insertJobs(ctx context.Context, tx *sql.Tx, jobs []domain.Job, now time.Time) error {
	const chunk = 500
	for len(jobs) > 0 {
		n := min(chunk, len(jobs))
		var args []any
		for _, j := range jobs[:n] {
			args = append(args, j.ID, j.TenantID, j.TaskID, j.ScheduledAt, j.Priority, j.Request.Method, j.Request.URL,
				s.seal(j.Request), j.TimeoutMS, j.Retry.MaxAttempts, j.Retry.BackoffMS, j.Status, j.NextAttemptAt, now)
		}
		// UNIQUE(task_id, scheduled_at): planning the same run twice is a no-op.
		q := `INSERT INTO jobs (id, tenant_id, task_id, scheduled_at, priority, method, url, secret, timeout_ms,
			max_attempts, backoff_ms, status, next_attempt_at, created_at) VALUES ` +
			strings.TrimSuffix(strings.Repeat("(?,?,?,?,?,?,?,?,?,?,?,?,?,?),", n), ",") +
			` ON DUPLICATE KEY UPDATE id = id`
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return err
		}
		jobs = jobs[n:]
	}
	return nil
}

func (s *Store) scanJob(row scanner) (*domain.Job, error) {
	var j domain.Job
	var worker, errMsg, body sql.NullString
	var status sql.NullInt64
	var sec []byte
	err := row.Scan(&j.ID, &j.TenantID, &j.TaskID, &j.ScheduledAt, &j.Priority, &j.Request.Method, &j.Request.URL,
		&sec, &j.TimeoutMS, &j.Retry.MaxAttempts, &j.Retry.BackoffMS, &j.Status, &j.Attempt, &j.NextAttemptAt,
		&worker, &j.LeaseExpiresAt, &status, &errMsg, &body, &j.CreatedAt, &j.StartedAt, &j.FinishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	j.WorkerID, j.HTTPStatus, j.Error, j.ResponseBody = worker.String, int(status.Int64), errMsg.String, body.String
	return &j, s.open(sec, &j.Request)
}

func (s *Store) queryJobs(ctx context.Context, q queryer, query string, args ...any) ([]domain.Job, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Job
	for rows.Next() {
		j, err := s.scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func candidates(ctx context.Context, tx *sql.Tx, order string, horizon time.Time, limit int) ([]domain.Candidate, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id, tenant_id FROM jobs
		WHERE status = 'pending' AND next_attempt_at <= ? ORDER BY `+order+` LIMIT ? FOR UPDATE SKIP LOCKED`, horizon, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Candidate
	for rows.Next() {
		var c domain.Candidate
		if err := rows.Scan(&c.ID, &c.TenantID); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// leaseGrace is added to the job's timeout; the lease counts from the job's
// due time, not the claim, so jobs waiting on their timer are not rescued.
const leaseGrace = 30 * time.Second

// retryDeadlock reruns fn when InnoDB picks it as a deadlock victim (or a
// lock wait times out). Claims and result writes touch the same index
// records, so under load this is expected; a lost result write would
// otherwise cost a full lease and a duplicate call.
func retryDeadlock[T any](fn func() (T, error)) (T, error) {
	for i := 1; ; i++ {
		v, err := fn()
		var me *mysql.MySQLError
		if i < 5 && errors.As(err, &me) && (me.Number == 1213 || me.Number == 1205) {
			time.Sleep(time.Duration(i) * 5 * time.Millisecond)
			continue
		}
		return v, err
	}
}

func (s *Store) Claim(ctx context.Context, workerID string, n int, horizon, now time.Time) ([]domain.Job, error) {
	return retryDeadlock(func() ([]domain.Job, error) { return s.claim(ctx, workerID, n, horizon, now) })
}

func (s *Store) claim(ctx context.Context, workerID string, n int, horizon, now time.Time) ([]domain.Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Anti-starvation lane: ~10% of the batch goes to the oldest due jobs,
	// whatever their priority. The rest goes in priority order.
	oldest, err := candidates(ctx, tx, "next_attempt_at", horizon, max(1, n/10))
	if err != nil {
		return nil, err
	}
	byPrio, err := candidates(ctx, tx, "priority, next_attempt_at", horizon, 4*n)
	if err != nil {
		return nil, err
	}
	ids := domain.FairPick(append(oldest, byPrio...), n)
	if len(ids) == 0 {
		return nil, nil
	}
	in := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := []any{workerID, now, leaseGrace.Milliseconds()}
	for _, id := range ids {
		args = append(args, id)
	}
	_, err = tx.ExecContext(ctx, `UPDATE jobs SET status = 'running', worker_id = ?, attempt = attempt + 1,
		lease_expires_at = DATE_ADD(GREATEST(next_attempt_at, ?), INTERVAL ((timeout_ms + ?) * 1000) MICROSECOND)
		WHERE id IN (`+in+`)`, args...)
	if err != nil {
		return nil, err
	}
	jobs, err := s.queryJobs(ctx, tx, `SELECT `+jobCols+` FROM jobs WHERE id IN (`+in+`)`, args[3:]...)
	if err != nil {
		return nil, err
	}
	return jobs, tx.Commit() // commit before any HTTP call
}

func (s *Store) SaveResult(ctx context.Context, j *domain.Job, workerID string) (bool, error) {
	return retryDeadlock(func() (bool, error) { return s.saveResult(ctx, j, workerID) })
}

func (s *Store) saveResult(ctx context.Context, j *domain.Job, workerID string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET status = ?, next_attempt_at = ?, worker_id = NULL,
		lease_expires_at = NULL, http_status = ?, error = ?, response_body = ?, started_at = ?, finished_at = ?
		WHERE id = ? AND status = 'running' AND worker_id = ? AND attempt = ?`,
		j.Status, j.NextAttemptAt, sql.NullInt64{Int64: int64(j.HTTPStatus), Valid: j.HTTPStatus != 0},
		null(trunc(j.Error)), null(trunc(j.ResponseBody)), j.StartedAt, j.FinishedAt,
		j.ID, workerID, j.Attempt)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func trunc(s string) string {
	if len(s) > 1000 {
		return s[:1000]
	}
	return s
}

func (s *Store) ReleaseExpired(ctx context.Context, now time.Time, limit int) (int64, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET
		status = IF(attempt >= max_attempts, 'dead', 'pending'),
		finished_at = IF(attempt >= max_attempts, ?, NULL),
		next_attempt_at = ?, worker_id = NULL, lease_expires_at = NULL, error = 'lease expired: executor lost'
		WHERE status = 'running' AND lease_expires_at < ? LIMIT ?`, now, now, now, limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) ListArchivable(ctx context.Context, succeededBefore, deadBefore time.Time, limit int) ([]domain.Job, error) {
	return s.queryJobs(ctx, s.db, `(SELECT `+jobCols+` FROM jobs WHERE status = 'succeeded' AND finished_at < ? LIMIT ?)
		UNION ALL (SELECT `+jobCols+` FROM jobs WHERE status = 'dead' AND finished_at < ? LIMIT ?)`,
		succeededBefore, limit-limit/2, deadBefore, limit/2)
}

func (s *Store) DeleteArchived(ctx context.Context, jobs []domain.Job) error {
	// Guarded by status and attempt: a job retried from the DLQ meanwhile stays.
	for _, j := range jobs {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM jobs WHERE id = ? AND status = ? AND attempt = ?`,
			j.ID, j.Status, j.Attempt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetJob(ctx context.Context, tenantID, id string) (*domain.Job, error) {
	return s.scanJob(s.db.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = ? AND tenant_id = ?`, id, tenantID))
}

func (s *Store) ListJobsForTask(ctx context.Context, tenantID, taskID string, limit int) ([]domain.Job, error) {
	return s.queryJobs(ctx, s.db, `SELECT `+jobCols+` FROM jobs WHERE tenant_id = ? AND task_id = ?
		ORDER BY scheduled_at DESC LIMIT ?`, tenantID, taskID, limit)
}

func (s *Store) ListDead(ctx context.Context, tenantID string, limit int) ([]domain.Job, error) {
	return s.queryJobs(ctx, s.db, `SELECT `+jobCols+` FROM jobs WHERE tenant_id = ? AND status = 'dead'
		ORDER BY finished_at DESC LIMIT ?`, tenantID, limit)
}

func (s *Store) RetryDead(ctx context.Context, tenantID, id string, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE jobs SET status = 'pending', next_attempt_at = ?,
		max_attempts = attempt + 1, finished_at = NULL WHERE id = ? AND tenant_id = ? AND status = 'dead'`, now, id, tenantID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("%w: no dead job with that id in the DLQ", domain.ErrNotFound)
	}
	return nil
}
