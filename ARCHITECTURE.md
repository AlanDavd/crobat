# Crobat architecture

Crobat is cron as a service: a tenant asks it to call an HTTP endpoint **now**, **at** a time, or on a **cron** expression, with a priority, retries, a dead letter queue (DLQ), and a history of every run. Product requirements are in [PRODUCT.md](PRODUCT.md). This document covers how the code meets them.

The one decision that shapes the rest: **there is no message broker. The `jobs` table in MySQL is the queue.** Saving a job is a single write, so the database and a queue can never disagree.

---

## 1. Components

```
                 tenants (JWT, sub = tenant id)
                    │                      │
          ┌─────────▼─────────┐  ┌─────────▼─────────┐
          │ task-api   :8080  │  │ job-api    :8081  │  read side: status, history,
          │ create/edit/pause │  │                   │  DLQ, manual retry
          └─────────┬─────────┘  └────┬─────────┬────┘
                    │ tasks (+ first job for "now")  │
          ┌─────────▼─────────────────▼──┐      │
          │ MySQL                         │      │
          │  tasks  (configuration)       │      │
          │  jobs   (hot table = queue)   │      │
          └──▲──────────▲──────────▲─────┘      │
             │          │          │            │
   ┌─────────┴──┐ ┌─────┴──────┐ ┌─┴──────────┐ │
   │ scheduler  │ │ executor×N │ │ reconciler │ │
   │ tasks→jobs │ │ claim, call│ │ expired    │ │
   │ 30s ahead  │ │ save result│ │ leases     │ │
   └────────────┘ └─────┬──────┘ └────────────┘ │
                        │ HTTPS via SSRF-checked dialer
                        ▼                       │
                 tenant endpoints               │
                                                │
   ┌────────────┐   write first,       ┌────────▼───┐
   │ archiver   │──────────────────────▶ Cassandra  │ history, 30-day TTL
   │ MySQL→Cass │   then delete MySQL  └────────────┘
   └────────────┘
```

| Service | What it does | Reads/writes |
|---|---|---|
| **task-api** | Create, get, list, replace, pause, resume, and delete tasks. `POST` accepts `Idempotency-Key`. | `tasks` (and the first `jobs` row for "now" tasks) |
| **scheduler** | Every 5 s, takes the active tasks due in the next 30 s. In one transaction it inserts their jobs as `pending` and advances `next_run_at`. | `tasks` → `jobs` |
| **executor** | Claims as many jobs as it has free workers, fires each on its own timer, calls the endpoint, saves the result under a fencing guard. | `jobs` |
| **reconciler** | Returns `running` jobs with an expired lease to `pending`, or to `dead` if they have no attempts left. | `jobs` |
| **archiver** | Copies terminal jobs to Cassandra, then deletes them from MySQL: succeeded jobs after 1 h, dead jobs after 14 d (the DLQ retry window). | `jobs` → Cassandra |
| **job-api** | Job status, task history (MySQL and Cassandra merged, deduplicated by job id), DLQ inbox, manual retry. | `jobs`, Cassandra |

All six run from **one binary** (`crobat serve <name>`), built into one image, and scale independently. The same binary is the tenant CLI. There is also a seventh mode, `crobat serve echo`, which is a demo target endpoint for local runs.

---

## 2. Code layout (hexagonal)

```
cmd/crobat/main.go              wiring (env → adapters → services) + CLI
internal/domain/                pure rules, no I/O
    domain.go                   Task, Job, Priority, validation, cron next-run,
                                Job.Apply (state machine), Backoff, FairPick
internal/app/
    ports.go                    TaskStore, JobStore, HistoryStore, Caller
internal/services/              the six services; depend only on ports (README each)
    taskservice/                write API for tasks
    scheduler/                  tasks → pending jobs, 30 s ahead
    executor/                   claim, fire on timer, call, save under fencing
    reconciler/                 expired leases → pending / dead
    archiver/                   MySQL → Cassandra, then delete
    jobservice/                 status, history, DLQ, manual retry
internal/adapter/               implementations of the ports
    mysql/                      TaskStore + JobStore; AES-GCM secrets at rest
    cassandra/                  HistoryStore
    caller/                     Caller: net/http with SSRF guard in the dialer
    httpapi/                    driving adapter: REST + HS256 JWT auth
deploy/mysql/schema.sql         tables + indexes (loaded by the mysql container)
docker-compose.yml, Dockerfile
```

Dependencies point inward: `adapter → services → app (ports) → domain`. The domain, app, and services packages never import a driver. Swapping MySQL for Vitess, or Cassandra for another store, means rewriting one adapter.

---

## 3. Data model

Two words carry the design:

- A **task** is the configuration: schedule, priority, request, timeout, retry policy, `next_run_at`. It changes rarely.
- A **job** is one execution of a task. It is written, claimed, retried, and finished. It **copies** the task's request, timeout, retry policy, and priority when it is created, so editing a task never changes a job that already exists.

### `tasks` (MySQL)

| Column | Notes |
|---|---|
| `id`, `tenant_id`, `name` | |
| `cron_expr`, `timezone`, `run_at` | cron → recurring; `run_at` → once; neither → once, now |
| `priority` | 0 critical, 1 high, 2 normal, 3 low |
| `method`, `url` | plaintext |
| `secret` | AES-256-GCM of `{headers, body}` |
| `timeout_ms`, `max_attempts`, `backoff_ms` | |
| `status` | `active` · `paused` · `deleted` (soft delete) |
| `next_run_at` | NULL once a one-shot task has been planned |
| `idempotency_key` | `UNIQUE(tenant_id, idempotency_key)` |
| `version` | optimistic concurrency for edits; the scheduler bumps it too |

Indexes: `(status, next_run_at)` for the scheduler and `(tenant_id, created_at)` for listing.

### `jobs` (MySQL, the queue)

| Column | Notes |
|---|---|
| `id` | UUID. It is also the `Idempotency-Key` sent on every attempt. |
| `task_id`, `scheduled_at` | `UNIQUE(task_id, scheduled_at)`, so planning a run twice is a no-op |
| copied request fields | `priority`, `method`, `url`, `secret`, `timeout_ms`, `max_attempts`, `backoff_ms` |
| `status` | `pending` · `running` · `succeeded` · `dead` |
| `attempt` | incremented by every claim; it is the **fencing token** |
| `next_attempt_at` | when the job may next fire (moves forward on backoff) |
| `worker_id`, `lease_expires_at` | the executor's lease |
| `http_status`, `error`, `response_body` | the last attempt's result; error and body truncated to 1000 chars |
| `started_at`, `finished_at` | scheduling lag = `started_at − scheduled_at` |

| Index | Used by |
|---|---|
| `(status, priority, next_attempt_at)` | executor claim, priority lane |
| `(status, next_attempt_at)` | executor claim, anti-starvation lane |
| `(status, lease_expires_at)` | reconciler |
| `(status, finished_at)` | archiver |
| `(tenant_id, status)` | DLQ inbox |
| `(tenant_id, task_id, scheduled_at)` | history |

### Cassandra (history)

Only terminal jobs are stored here. Once a job is finished it never changes again, so last-write-wins is harmless. Rows have a 30-day TTL.

- `job_history`: `PRIMARY KEY ((tenant_id, task_id, month), scheduled_at DESC, job_id)`
- `job_by_id`: `PRIMARY KEY ((tenant_id, job_id))`

Headers, request bodies, and response bodies are never archived, and URLs are stored with the query string and userinfo stripped. The keyspace and tables are created by the archiver or job-api when it starts.

---

## 4. Life of a job

```
            scheduler / task-api ("now")
                      │ INSERT
                      ▼
   ┌──────────────► pending ◄───────────────────────────┐
   │                  │ claim (SKIP LOCKED), attempt+1,  │
   │                  │ lease, COMMIT                    │
   │                  ▼                                  │
   │               running ── lease expires ─► reconciler┤
   │                  │ fire on timer, call endpoint     │ (attempts left)
   │     ┌────────────┼────────────┐                     │
   │    2xx        failure      failure, no attempts     │
   │     ▼        (attempts     left / lease expired     │
   │ succeeded      left)            ▼                   │
   │     │            └─backoff──►  dead (DLQ) ──────────┘
   │     │                           │      manual retry: +1 attempt
   │     ▼ after 1 h                 ▼ after 14 d
   │  archiver: Cassandra first, then DELETE from MySQL
   └── (retry from DLQ)
```

Take a task with `0 9 * * *`:

1. **Planned ahead.** At about 08:59:30 the scheduler's tick sees `next_run_at = 09:00:00 ≤ now + 30s`. In one transaction it locks the task row (`FOR UPDATE SKIP LOCKED`, so several schedulers can run), inserts the job as `pending`, and sets `next_run_at` to tomorrow's 09:00. The next run is computed from the previous `next_run_at`, not from now, and the query has no lower bound. A late scheduler therefore delays runs but never skips them, and runs missed during an outage are caught up (at most 1000 per task per tick).
2. **Claimed.** Each executor claims pending jobs due before its next poll (500 ms), up to its number of free workers. It claims on every poll tick, and also as soon as a quarter of its worker slots free up, so throughput is not capped at `workers ÷ poll`. It sets them `running` with its `worker_id`, `attempt + 1`, and `lease_expires_at = max(due, now) + timeout + 30s`, and commits **before** any HTTP call. The lease is counted from the due time so jobs waiting on their timer are not rescued by mistake.
3. **Fired on time.** Each job sleeps on its own in-memory timer until `next_attempt_at`, then the executor calls the endpoint with `Idempotency-Key: <job id>`, `X-Scheduled-At`, and `X-Attempt`. Precision comes from the timer, not from polling the database often. In the local stack, a `* * * * *` job fired 13 ms after `:00`.
4. **Result saved only if still ours.**
   ```sql
   UPDATE jobs SET ... WHERE id = ? AND status = 'running' AND worker_id = ? AND attempt = ?
   ```
   Claims and result writes retry automatically when InnoDB picks them as a deadlock victim, which happens under load (see [LOADTESTING.md](LOADTESTING.md)). A 2xx response moves the job to `succeeded`. Anything else (non-2xx, timeout, network error, blocked destination) moves it back to `pending` at `now + backoff · 2^(attempt−1)` (capped at 1 h), or to `dead` once `attempt ≥ max_attempts`. The state transition is `domain.Job.Apply`.
5. **Rescued if the executor dies.** The reconciler flips `running` jobs whose lease has expired to `pending`, or to `dead` if they have no attempts left. The next claim increments `attempt`, so a zombie executor's late write matches 0 rows.
6. **Archived.** The archiver writes to Cassandra first and deletes from MySQL second. A crash in between leaves a duplicate (job-api deduplicates by id), never a lost job. The delete is guarded by `status` and `attempt`, so a job retried from the DLQ in the meantime stays in MySQL.

"Now" tasks skip the scheduler: task-api inserts the task and its job **in the same transaction**, so a critical "now" job is claimable within one executor poll.

---

## 5. Guarantees and how each is met

| Requirement | Mechanism |
|---|---|
| **An accepted task is never lost** | A `201` is returned only after the MySQL commit. The job is persisted before it can run ("never run a job that is not persisted"). History is written before the hot row is deleted. |
| **At-least-once, never dropped silently** | Leases and the reconciler recover jobs from dead executors. Every failure ends in `pending` (retry) or `dead` (DLQ, visible via `crobat job dlq`). Nothing is discarded. |
| **Receiver can deduplicate** | `Idempotency-Key = job id` is stable across all attempts and manual retries. |
| **Late results cannot corrupt a job** | `attempt` is a fencing token in every result write (verified: a stale-attempt write affects 0 rows). |
| **Critical never waits behind low** | The claim orders by `priority, next_attempt_at`. |
| **Low never starves** | About 10 % of every claim batch (minimum 1) is an "oldest due first" lane that ignores priority, so an old low job is eventually claimed even during a critical flood. |
| **Tenant fairness** | `domain.FairPick` caps each tenant at an equal share of a claim batch, then fills leftover slots (work-conserving). |
| **Secrets** | Headers and body are encrypted with AES-256-GCM in MySQL, never serialized to JSON (`json:"-"`), never archived, and never logged. Stored and logged errors are rewritten to generic causes so URLs (which may carry tokens) do not leak. |
| **SSRF** | See §6. |
| **Idempotent task creation** | `UNIQUE(tenant_id, idempotency_key)`. A repeated `POST` returns the original task. |
| **Edits never change planned jobs** | Jobs copy the task's request at plan time. Edits use optimistic `version` checks; a lost race returns 400 "retry". |

### Scheduling-lag targets (p99)

| Priority | Target | How |
|---|---|---|
| critical | < 1 s | Jobs exist ≤ 30 s ahead. Executors poll every 500 ms with a one-poll lookahead and fire on in-memory timers. |
| high | < 20 s | Same path; only delayed if executors are saturated. |
| normal | a few min | Served after critical/high within each batch. |
| low | < 20 min | Priority lane plus the anti-starvation lane. |

These targets depend on enough executor capacity for the peak (for example 30k jobs due at 09:00:00). Pre-planning and the per-job timer spread the database work across the 30 s before the peak, so the database is not hit with all of it at the due second.

---

## 6. SSRF defence

Executors call tenant-chosen URLs from inside the network, next to MySQL and the cloud metadata service. Validating the URL when the task is saved is not enough because of DNS rebinding. The check therefore runs in `net.Dialer.Control`, **after DNS resolution and right before `connect()`, on the exact IP being dialed** (`internal/adapter/caller`):

- Only ports **80 and 443** are allowed.
- Only **public unicast** addresses are allowed. Rejected: loopback, RFC 1918, link-local (including `169.254.169.254`), CGNAT `100.64/10`, `0/8`, `198.18/15`, ULA `fc00::/7`, NAT64 `64:ff9b::/96`, and v4-mapped forms of all of these.
- No proxy (`Proxy: nil`) and no redirects (a 3xx counts as a failed attempt).
- The error is a generic "destination not allowed".

`EXECUTOR_ALLOW_CIDRS` adds exceptions **for local development only**. Compose uses it to allow just the echo container (`172.28.0.100/32`), and the port rule still applies. The executor logs a warning at startup when it is set.

---

## 7. Why these technologies

- **MySQL for the hot table, not Postgres.** Every state change is a conditional `UPDATE ... WHERE status = ...` on an indexed column. InnoDB handles this cheaply. Postgres would need non-HOT updates, which rewrite every index, plus vacuum and bloat on a high-churn table.
- **No broker.** A database write plus a broker publish is a dual write. With Kafka, per-partition offsets also mean a slow job holds back every job behind it. An outbox would remove the dual write but add a relay and latency. `FOR UPDATE SKIP LOCKED` lets many executors claim from one table without waiting on each other (the same pattern as Shopify's inventory reservations).
- **Leases in the row, not Redis locks.** The claim already writes the row. A Redis lock would be a second source of truth that can disagree with the database or vanish in a failover.
- **Cassandra for history only.** It takes huge write volumes, and history is append-only, so last-write-wins and the cost of lightweight transactions don't matter.
- **Go stdlib first.** HTTP routing (Go 1.22+ mux patterns), JWT HS256 verification, AES-GCM, UUIDs, and logging (`slog`) are all stdlib. The third-party dependencies are `go-sql-driver/mysql`, `gocql`, and `robfig/cron/v3`.

---

## 8. Running locally

```bash
cp .env.example .env                  # secrets and knobs; generate your own for anything shared
docker compose up -d --build          # MySQL, Cassandra, 6 services (2 executors), echo
go build -o bin/crobat ./cmd/crobat   # the CLI

set -a; source .env; set +a           # JWT_SECRET and API URLs for the CLI
export CROBAT_TOKEN=$(bin/crobat token acme)

bin/crobat task create -name hello -url http://echo/hello -priority critical \
    -header "Authorization: Bearer xyz" -idempotency-key first
bin/crobat task create -name daily -url http://echo/report -cron "0 9 * * *" -tz Europe/Madrid
bin/crobat task create -name flaky -url http://echo/flaky -backoff-ms 500   # succeeds on attempt 2
bin/crobat task create -name doomed -url http://echo/fail -max-attempts 2   # ends up in the DLQ
bin/crobat task list
bin/crobat job history <task-id>
bin/crobat job dlq
bin/crobat job retry <job-id>
bin/crobat task pause|resume|delete <task-id>

docker compose logs -f executor echo   # watch calls, attempts, lag_ms
```

Cassandra takes about a minute to become healthy. job-api and archiver wait for it.

Demo endpoints on `http://echo/...`: `/fail` (always 500), `/flaky` (503 on the first attempt), `/slow?ms=N`, and anything else (200).

### REST API

All routes need `Authorization: Bearer <HS256 JWT>`. The tenant is the token's `sub` claim.

| Method & path | Service |
|---|---|
| `POST /tasks` (`Idempotency-Key` header optional) | task-api |
| `GET /tasks`, `GET /tasks/{id}` | task-api |
| `PUT /tasks/{id}` (full replace) | task-api |
| `POST /tasks/{id}/pause`, `POST /tasks/{id}/resume`, `DELETE /tasks/{id}` | task-api |
| `GET /jobs/{id}`, `POST /jobs/{id}/retry` | job-api |
| `GET /tasks/{id}/jobs?limit=n`, `GET /dlq` | job-api |

Task body:

```json
{
  "name": "daily-report",
  "cron": "0 9 * * *",
  "timezone": "Europe/Madrid",
  "priority": "high",
  "request": {"method": "POST", "url": "https://api.example.com/run",
              "headers": {"Authorization": "Bearer ..."}, "body": "{}"},
  "timeout_ms": 10000,
  "retry": {"max_attempts": 3, "backoff_ms": 1000}
}
```

Use `"at": "2026-12-01T09:00:00Z"` instead of `cron` for a one-shot task, or omit both to run it now.

### Configuration (env)

docker compose reads secrets and tunables from `.env` (template: [.env.example](.env.example)). It fails fast if `SECRETS_KEY`, `JWT_SECRET` or the MySQL passwords are missing. Each service reads:

| Var | Default | Used by |
|---|---|---|
| `MYSQL_DSN` | `crobat:crobat@tcp(localhost:3306)/crobat` | all except echo |
| `SECRETS_KEY` | *(required)* base64 of 32 bytes | all except echo |
| `JWT_SECRET` | *(required)* | task-api, job-api, `crobat token` |
| `CASSANDRA_HOSTS`, `CASSANDRA_KEYSPACE`, `CASSANDRA_RF` | `localhost`, `crobat`, `1` | job-api, archiver |
| `HTTP_ADDR` | `:8080` / `:8081` / `:80` | APIs, echo |
| `SCHEDULER_INTERVAL`, `SCHEDULER_LOOKAHEAD`, `SCHEDULER_BATCH` | `5s`, `30s`, `500` | scheduler |
| `EXECUTOR_WORKERS`, `EXECUTOR_POLL`, `WORKER_ID` | `64`, `500ms`, hostname | executor (compose: also `EXECUTOR_REPLICAS`, default `2`) |
| `EXECUTOR_ALLOW_CIDRS` | empty | executor (**dev only**) |
| `RECONCILER_INTERVAL` | `5s` | reconciler |
| `ARCHIVER_INTERVAL`, `ARCHIVE_SUCCEEDED_AFTER`, `ARCHIVE_DEAD_AFTER` | `10s`, `1h`, `336h` | archiver |
| `CROBAT_TOKEN`, `CROBAT_TASK_API`, `CROBAT_JOB_API` | –, `:8080`, `:8081` on localhost | CLI |

### Tests

`go test ./...` runs unit tests for the domain rules (cron catch-up and timezones, the retry/dead state machine, backoff cap, fair pick) and the SSRF address filter. The MySQL and Cassandra paths were verified end to end against the compose stack:

- now, cron, retry, and DLQ flows
- idempotent create and tenant isolation
- SSRF block of `169.254.169.254`
- executors killed mid-call → reconciler → attempt 2 with the same key
- DLQ retry
- archive to Cassandra, history served from Cassandra
- a stale fencing write affecting 0 rows

`loadtest/run.sh` reproduces the 30,000-jobs-in-one-second peak with k6 and reports lag by priority and tenant. Method, results and the bugs it found are in [LOADTESTING.md](LOADTESTING.md).

---

## 9. Behaviour worth knowing

- **Pause** stops new jobs from being planned. Jobs already planned (≤ 30 s ahead) still run, because a job is the plan. **Resume** of a cron task restarts from now: runs missed while paused are skipped on purpose, unlike outage catch-up.
- **Manual retry** from the DLQ grants exactly one more attempt (`max_attempts = attempt + 1`). The `Idempotency-Key` is unchanged.
- **Delete** is a soft delete. History stays queryable until its TTL expires.
- **`3xx` is a failure.** Redirects are never followed (SSRF).

---

## 10. Known limits and next steps

Shortcuts taken on purpose for now, each with the point at which to revisit it:

| Today | Ceiling | Upgrade path |
|---|---|---|
| One commit per result write | Measured bottleneck: MySQL CPU, 1.2–2.4k jobs/s on a laptop, critical p99 at the 1 s target ([LOADTESTING.md](LOADTESTING.md)) | Batch result writes per ~5 ms. Claim further ahead for critical only. |
| A single `jobs` table | All claiming load lands on one MySQL primary | Shard `jobs` by hash (Vitess). Executors claim per shard. |
| Fairness is per claim batch | A tenant with a huge backlog still gets an equal share of each batch, not a global quota | Per-tenant in-flight quotas or weighted fair queuing in the claim |
| Scheduler updates `next_run_at` one row at a time | Shows up near tens of thousands of tasks per tick | Batched `UPDATE ... CASE` (marked `ponytail:` in code) |
| Archiver deletes row by row | Archiving throughput | `DELETE ... WHERE (id, status, attempt) IN (...)` |
| HS256 JWT with a shared secret, no API gateway | Fine for one issuer | RS256/JWKS behind the gateway. Per-tenant rate limits. |
| `SECRETS_KEY` from env | No rotation | KMS envelope encryption with a key id stored per row |
| Logs only (JSON `slog`, with `lag_ms` per job) | No dashboards or alerts | Prometheus metrics for claim rate, lag histograms per priority, DLQ size, and lease expiries |
| Load-tested on one laptop only (30k due in one second, drained in 11–24 s) | Does not show 500k/s or 1B/day | The same `loadtest/run.sh` against a sharded deployment on real hardware |
| 99.99 % API availability | A deployment property: multiple replicas of every service, Multi-AZ MySQL, Cassandra RF 3 | The services are stateless and scale horizontally as-is |
