# Reconciler

**Objective:** rescue jobs whose executor died, so no execution is ever dropped silently.

| | |
|---|---|
| Run | `crobat serve reconciler` |
| Compose service | `reconciler` (no port) |
| Code | [reconciler.go](reconciler.go) |
| Uses ports | `app.JobStore` ([ports.go](../../app/ports.go)), implemented by [internal/adapter/mysql](../../adapter/mysql/mysql.go) |

## How it works

Every `RECONCILER_INTERVAL` (5 s) it runs one statement in batches of 1,000:

```sql
UPDATE jobs SET status = IF(attempt >= max_attempts, 'dead', 'pending'), ...
WHERE status = 'running' AND lease_expires_at < now
```

- A job with attempts left goes back to `pending` and is due immediately.
- A job with no attempts left goes to `dead`, where it appears in the tenant's DLQ.
- Its error is set to `lease expired: executor lost`.

## Why this is safe

- **Leases count from the job's due time**, not from the claim: lease = due time + timeout + 30 s. A job waiting on its timer is never mistaken for an abandoned one.
- **Fencing.** The next claim increments `attempt`. If the "dead" executor was only slow and comes back with a result, its write matches 0 rows.
- **The receiver can deduplicate.** A rescued job is called again with the **same `Idempotency-Key`**, which is the at-least-once contract.

Verified locally by killing both executors mid-call. The reconciler released the job once its lease expired, and the job succeeded on attempt 2 with the same key.

## Configuration

| Env | Default | |
|---|---|---|
| `RECONCILER_INTERVAL` | `5s` | Time between sweeps |
| `MYSQL_DSN`, `SECRETS_KEY` | | See [ARCHITECTURE.md](../../../ARCHITECTURE.md#configuration-env) |

## Scaling

One replica is enough. The sweep is a single indexed `UPDATE` on `(status, lease_expires_at)`. Extra replicas are harmless, because the update is idempotent.
