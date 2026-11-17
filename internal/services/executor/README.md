# Executor

**Objective:** claim pending jobs, fire each one at its exact second, call the tenant's endpoint, and save the result, but only if the job is still ours.

| | |
|---|---|
| Run | `crobat serve executor` |
| Compose service | `executor`, 2 replicas by default (`EXECUTOR_REPLICAS`), no port |
| Code | [executor.go](executor.go) |
| Uses ports | `app.JobStore`, implemented by [internal/adapter/mysql](../../adapter/mysql/mysql.go); `app.Caller`, implemented by [internal/adapter/caller](../../adapter/caller/caller.go) |

## How it works

1. **Claim.** The executor takes up to `free workers` pending jobs due before its next poll, in one transaction with `FOR UPDATE SKIP LOCKED`. Each claimed job is marked `running` with this executor's `worker_id`, `attempt + 1`, and a lease, and the transaction **commits before any HTTP call**.
   - Jobs are taken in `priority, next_attempt_at` order, so critical goes first.
   - About 10 % of each batch is reserved for the oldest due jobs regardless of priority, so low priority never starves.
   - `domain.FairPick` caps each tenant at an equal share of the batch.
2. **Wait.** Each job sleeps on its own in-memory timer until `next_attempt_at`.
3. **Call.** The endpoint is called with the job's timeout and these headers: `Idempotency-Key: <job id>`, `X-Scheduled-At`, `X-Attempt`.
4. **Save.** The result is written with the fencing guard:
   ```sql
   WHERE id = ? AND status = 'running' AND worker_id = ? AND attempt = ?
   ```
   A 2xx makes the job `succeeded`. Anything else makes it `pending` with exponential backoff, or `dead` once its attempts are used up. If another executor has taken the job over, the write affects 0 rows and is logged as "fenced off".

The executor claims on every poll tick, and also as soon as a quarter of its worker slots are free. Without that wake-up, throughput was capped at `workers ÷ poll` ([LOADTESTING.md](../../../LOADTESTING.md#bug-1-executor-throughput-capped-at-workers--poll-interval)). Claims and result writes retry automatically on InnoDB deadlocks.

## Security: SSRF guard

Tenant URLs are dialed through a `net.Dialer` whose `Control` hook checks the **resolved IP right before `connect()`**:

- only public unicast addresses on ports 80 and 443;
- no proxy and no redirects;
- a generic error when blocked.

Errors stored on the job never include the URL. `EXECUTOR_ALLOW_CIDRS` adds exceptions **for local development only** and logs a warning at startup.

## Configuration

| Env | Default | |
|---|---|---|
| `EXECUTOR_WORKERS` | `64` | Max in-flight jobs per replica |
| `EXECUTOR_POLL` | `500ms` | Poll interval, and how far ahead jobs are claimed |
| `WORKER_ID` | hostname | Owner id written into claimed rows |
| `EXECUTOR_ALLOW_CIDRS` | empty | **Dev only.** Extra allowed destinations. |
| `MYSQL_DSN`, `SECRETS_KEY` | | See [ARCHITECTURE.md](../../../ARCHITECTURE.md#configuration-env) |

## Failure behaviour

- **Executor crashes:** its jobs stay `running` until the lease expires (due time + timeout + 30 s). The [reconciler](../reconciler/README.md) then returns them to `pending`, and the next claim increments `attempt`, so any late write from the dead executor is rejected.
- **Shutdown (SIGTERM):** the executor stops claiming and finishes its in-flight jobs before exiting.

## Scaling

Add replicas freely. `SKIP LOCKED` means executors never wait on each other. In the load test, 2 replicas drained 30,000 simultaneous jobs in 11–24 s; at that point MySQL, not the executors, was the limit.
