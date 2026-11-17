# Scheduler

**Objective:** turn due tasks into pending jobs **ahead of time**, so every execution exists in the queue before its second arrives.

| | |
|---|---|
| Run | `crobat serve scheduler` |
| Compose service | `scheduler` (no port) |
| Code | [scheduler.go](scheduler.go) |
| Uses ports | `app.TaskStore` ([ports.go](../../app/ports.go)), implemented by [internal/adapter/mysql](../../adapter/mysql/mysql.go) |

## How it works

Every `SCHEDULER_INTERVAL` (5 s) it runs one tick:

1. Lock up to `SCHEDULER_BATCH` active tasks with `next_run_at <= now + 30s`, using `FOR UPDATE SKIP LOCKED`.
2. For each task, plan every run up to the horizon with `domain.Task.NewJob`. Each job copies the task's request, timeout, retry policy, and priority.
3. In the **same transaction**, insert the jobs as `pending` and advance each task's `next_run_at`.
4. If the batch was full, run another tick immediately. Otherwise wait for the next interval.

Because the jobs are created up to 30 s early, the [executors](../executor/README.md) can claim them shortly before their time and fire them from in-memory timers.

## Guarantees

- **No lower bound on the query** (`next_run_at <= horizon`). A task the scheduler reaches late is delayed, never skipped.
- **Catch-up after outages.** The next run is computed from the *previous* `next_run_at`, not from now, so runs missed while the scheduler was down are created afterwards. A task can create at most 1,000 jobs per tick; the rest follow on later ticks.
- **Idempotent planning.** `UNIQUE(task_id, scheduled_at)` makes inserting the same run twice a no-op.
- **One-shot tasks** (`at` / now) get `next_run_at = NULL` once planned.

## Configuration

| Env | Default | |
|---|---|---|
| `SCHEDULER_INTERVAL` | `5s` | Time between ticks |
| `SCHEDULER_LOOKAHEAD` | `30s` | How far ahead jobs are created |
| `SCHEDULER_BATCH` | `500` | Tasks per transaction |
| `MYSQL_DSN`, `SECRETS_KEY` | | See [ARCHITECTURE.md](../../../ARCHITECTURE.md#configuration-env) |

## Scaling

Multiple replicas are safe: `SKIP LOCKED` hands each tick a disjoint set of tasks. In the load test, one replica planned 30,000 jobs in 8.3 s, finishing 19 s before they were due.
