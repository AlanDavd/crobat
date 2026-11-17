package mysql

// Integration tests against a real MySQL. They create and use their own
// database, so they never touch a running stack's data:
//
//	MYSQL_TEST_DSN="root:$MYSQL_ROOT_PASSWORD@tcp(localhost:3306)/crobat_test" go test ./internal/adapter/mysql/

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"crobat/internal/domain"
)

var ctx = context.Background()

func live(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_TEST_DSN not set")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := cfg.DBName
	cfg.DBName, cfg.MultiStatements = "", true
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema, err := os.ReadFile("../../../deploy/mysql/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec("CREATE DATABASE IF NOT EXISTS `" + name + "`; USE `" + name + "`; " + string(schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec("TRUNCATE `" + name + "`.jobs; TRUNCATE `" + name + "`.tasks"); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dsn, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.db.Close() })
	return s
}

func newJob(tenant string, p domain.Priority, due time.Time, maxAttempts int) domain.Job {
	return domain.Job{ID: domain.NewID(), TenantID: tenant, TaskID: domain.NewID(), ScheduledAt: due, Priority: p,
		Request:   domain.Request{Method: "POST", URL: "https://e.com", Headers: map[string]string{"X-Secret": "s"}},
		TimeoutMS: 1000, Retry: domain.RetryPolicy{MaxAttempts: maxAttempts, BackoffMS: 100},
		Status: domain.JobPending, NextAttemptAt: due}
}

func insert(t *testing.T, s *Store, jobs ...domain.Job) {
	t.Helper()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.insertJobs(ctx, tx, jobs, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func now() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }

func newTask(cron string, next *time.Time) *domain.Task {
	n := now()
	return &domain.Task{ID: domain.NewID(), TenantID: "acme", Name: "t", Cron: cron, Timezone: "UTC",
		Request:   domain.Request{Method: "POST", URL: "https://e.com", Headers: map[string]string{"Authorization": "TOPSECRET"}},
		TimeoutMS: 1000, Retry: domain.RetryPolicy{MaxAttempts: 3, BackoffMS: 100}, Status: domain.TaskActive,
		NextRunAt: next, Version: 1, CreatedAt: n, UpdatedAt: n}
}

func TestCreateTaskIsIdempotentAndEncryptsSecrets(t *testing.T) {
	s := live(t)
	first := newTask("", nil)
	first.IdempotencyKey = "k1"
	j := newJob("acme", domain.Normal, now(), 3)
	j.TaskID = first.ID
	if _, err := s.CreateTask(ctx, first, &j); err != nil {
		t.Fatal(err)
	}
	again := newTask("", nil)
	again.IdempotencyKey = "k1"
	got, err := s.CreateTask(ctx, again, nil)
	if err != nil || got.ID != first.ID {
		t.Fatalf("same key must return the original task: %v %v", got, err)
	}
	var raw []byte
	_ = s.db.QueryRow("SELECT secret FROM tasks WHERE id = ?", first.ID).Scan(&raw)
	if len(raw) == 0 || string(raw) == "TOPSECRET" || got.Request.Headers["Authorization"] != "TOPSECRET" {
		t.Fatalf("secret must be encrypted at rest and decrypted on read: raw=%q", raw)
	}
	if _, err := s.GetJob(ctx, "acme", j.ID); err != nil {
		t.Fatalf("the first job must be written with the task: %v", err)
	}
	if _, err := s.GetTask(ctx, "evil", first.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("tenant isolation: %v", err)
	}
}

func TestUpdateTaskRejectsStaleVersions(t *testing.T) {
	s := live(t)
	tk := newTask("* * * * *", nil)
	if _, err := s.CreateTask(ctx, tk, nil); err != nil {
		t.Fatal(err)
	}
	a, _ := s.GetTask(ctx, "acme", tk.ID)
	b, _ := s.GetTask(ctx, "acme", tk.ID)
	if err := s.UpdateTask(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateTask(ctx, b); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("stale write must be rejected, got %v", err)
	}
}

func TestScheduleDuePlansOnceAndAdvances(t *testing.T) {
	s := live(t)
	due := now().Truncate(time.Minute).Add(-2 * time.Minute)
	tk := newTask("* * * * *", &due)
	if _, err := s.CreateTask(ctx, tk, nil); err != nil {
		t.Fatal(err)
	}
	horizon := now().Add(30 * time.Second)
	plan := func(t *domain.Task) []domain.Job {
		var out []domain.Job
		for t.NextRunAt != nil && !t.NextRunAt.After(horizon) {
			out = append(out, t.NewJob(*t.NextRunAt))
			t.NextRunAt = t.NextAfter(*t.NextRunAt)
		}
		return out
	}
	if n, err := s.ScheduleDue(ctx, horizon, 10, plan); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	got, _ := s.GetTask(ctx, "acme", tk.ID)
	if !got.NextRunAt.After(horizon) {
		t.Fatalf("next_run_at not advanced: %v", got.NextRunAt)
	}
	count := func() (n int) {
		_ = s.db.QueryRow("SELECT COUNT(*) FROM jobs WHERE task_id = ?", tk.ID).Scan(&n)
		return
	}
	planned := count()
	if planned < 3 {
		t.Fatalf("planned %d jobs, want the missed runs too", planned)
	}
	// Replaying the same runs (e.g. a crash before the task update) is a no-op.
	_, _ = s.db.Exec("UPDATE tasks SET next_run_at = ? WHERE id = ?", due, tk.ID)
	if _, err := s.ScheduleDue(ctx, horizon, 10, plan); err != nil {
		t.Fatal(err)
	}
	if count() != planned {
		t.Fatalf("UNIQUE(task_id, scheduled_at) must prevent duplicates: %d vs %d", count(), planned)
	}
}

func TestClaimLeaseAndFencing(t *testing.T) {
	s := live(t)
	due := now()
	insert(t, s, newJob("acme", domain.Normal, due, 3))

	claimed, err := s.Claim(ctx, "w1", 5, now().Add(time.Second), now())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claimed %d, err %v", len(claimed), err)
	}
	j := claimed[0]
	if j.Status != domain.JobRunning || j.Attempt != 1 || j.WorkerID != "w1" || j.Request.Headers["X-Secret"] != "s" {
		t.Fatalf("claimed job = %+v", j)
	}
	if j.LeaseExpiresAt == nil || !j.LeaseExpiresAt.After(due.Add(time.Second)) {
		t.Fatalf("lease must cover due + timeout: %v", j.LeaseExpiresAt)
	}
	if again, _ := s.Claim(ctx, "w2", 5, now().Add(time.Second), now()); len(again) != 0 {
		t.Fatal("a running job must not be claimed twice")
	}

	stale := j
	stale.Attempt = 0
	stale.Apply(domain.Result{HTTPStatus: 200, StartedAt: now(), FinishedAt: now()})
	if ok, err := s.SaveResult(ctx, &stale, "w1"); ok || err != nil {
		t.Fatalf("stale attempt must be fenced off: ok=%v err=%v", ok, err)
	}
	if ok, _ := s.SaveResult(ctx, &j, "other-worker"); ok {
		t.Fatal("another worker's write must be fenced off")
	}
	j.Apply(domain.Result{HTTPStatus: 200, StartedAt: now(), FinishedAt: now()})
	if ok, err := s.SaveResult(ctx, &j, "w1"); !ok || err != nil {
		t.Fatalf("owner's write: ok=%v err=%v", ok, err)
	}
	got, _ := s.GetJob(ctx, "acme", j.ID)
	if got.Status != domain.JobSucceeded || got.HTTPStatus != 200 || got.FinishedAt == nil || got.WorkerID != "" {
		t.Fatalf("saved job = %+v", got)
	}
}

func TestClaimServesPriorityWithAnAntiStarvationLane(t *testing.T) {
	s := live(t)
	var all []domain.Job
	for range 5 {
		all = append(all, newJob("acme", domain.Low, now().Add(-10*time.Second), 3))
		all = append(all, newJob("acme", domain.Critical, now(), 3))
	}
	insert(t, s, all...)
	claimed, err := s.Claim(ctx, "w1", 5, now().Add(time.Second), now())
	if err != nil {
		t.Fatal(err)
	}
	by := map[domain.Priority]int{}
	for _, j := range claimed {
		by[j.Priority]++
	}
	if by[domain.Critical] != 4 || by[domain.Low] != 1 {
		t.Fatalf("want 4 critical + 1 oldest low, got %v", by)
	}
}

// Fair share applies within the claim's candidate window (4 × batch, in
// priority and due order). A tenant whose older same-priority backlog is larger
// than that window still delays others FIFO; see ARCHITECTURE.md known limits.
func TestClaimSharesBatchesBetweenTenants(t *testing.T) {
	s := live(t)
	var all []domain.Job
	for range 12 { // < 4 × batch, so the small tenant's job is in the window
		all = append(all, newJob("noisy", domain.Normal, now().Add(-time.Second), 3))
	}
	all = append(all, newJob("small", domain.Normal, now(), 3)) // newest: last in priority order
	insert(t, s, all...)
	claimed, _ := s.Claim(ctx, "w1", 4, now().Add(time.Second), now())
	small := 0
	for _, j := range claimed {
		if j.TenantID == "small" {
			small++
		}
	}
	if len(claimed) != 4 || small != 1 {
		t.Fatalf("the small tenant must get a slot: %d claimed, %d small", len(claimed), small)
	}
}

func TestConcurrentClaimsNeverOverlap(t *testing.T) {
	s := live(t)
	var all []domain.Job
	for range 200 {
		all = append(all, newJob("acme", domain.Priority(len(all)%4), now(), 3))
	}
	insert(t, s, all...)
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for {
				js, err := s.Claim(ctx, string(rune('a'+w)), 10, now().Add(time.Second), now())
				if err != nil {
					t.Error(err)
					return
				}
				if len(js) == 0 {
					return
				}
				mu.Lock()
				for _, j := range js {
					seen[j.ID]++
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if len(seen) != 200 {
		t.Fatalf("claimed %d distinct jobs, want 200", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %s claimed %d times", id, n)
		}
	}
}

func TestReleaseExpired(t *testing.T) {
	s := live(t)
	last, retry := newJob("acme", domain.Normal, now(), 1), newJob("acme", domain.Normal, now(), 3)
	insert(t, s, last, retry)
	if c, _ := s.Claim(ctx, "w1", 10, now().Add(time.Second), now()); len(c) != 2 {
		t.Fatalf("claimed %d", len(c))
	}
	if n, _ := s.ReleaseExpired(ctx, now(), 100); n != 0 {
		t.Fatal("live leases must not be released")
	}
	if n, err := s.ReleaseExpired(ctx, now().Add(time.Hour), 100); n != 2 || err != nil {
		t.Fatalf("n=%d err=%v", n, err)
	}
	a, _ := s.GetJob(ctx, "acme", last.ID)
	b, _ := s.GetJob(ctx, "acme", retry.ID)
	if a.Status != domain.JobDead || a.FinishedAt == nil || b.Status != domain.JobPending || b.WorkerID != "" {
		t.Fatalf("out of attempts → dead, otherwise → pending: %s, %s", a.Status, b.Status)
	}
}

func TestDLQRetryAndArchiveGuard(t *testing.T) {
	s := live(t)
	insert(t, s, newJob("acme", domain.Normal, now(), 1))
	c, _ := s.Claim(ctx, "w1", 1, now().Add(time.Second), now())
	j := c[0]
	j.Apply(domain.Result{HTTPStatus: 500, StartedAt: now(), FinishedAt: now()})
	if ok, _ := s.SaveResult(ctx, &j, "w1"); !ok || j.Status != domain.JobDead {
		t.Fatalf("want dead, got %s", j.Status)
	}
	if dead, _ := s.ListDead(ctx, "acme", 10); len(dead) != 1 {
		t.Fatalf("DLQ has %d", len(dead))
	}
	snapshot, _ := s.ListArchivable(ctx, now().Add(time.Hour), now().Add(time.Hour), 10)
	if len(snapshot) != 1 {
		t.Fatalf("archivable %d", len(snapshot))
	}

	if err := s.RetryDead(ctx, "acme", j.ID, now()); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteArchived(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetJob(ctx, "acme", j.ID)
	if err != nil || got.Status != domain.JobPending || got.Retry.MaxAttempts != 2 {
		t.Fatalf("a job retried from the DLQ must survive the archiver: %+v %v", got, err)
	}
	if err := s.RetryDead(ctx, "acme", j.ID, now()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("only dead jobs can be retried: %v", err)
	}
	if err := s.RetryDead(ctx, "evil", j.ID, now()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("tenant isolation: %v", err)
	}
}
