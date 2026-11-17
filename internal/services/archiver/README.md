# Archiver

**Objective:** keep the MySQL hot table small by moving finished jobs to the Cassandra history, without ever losing one.

| | |
|---|---|
| Run | `crobat serve archiver` |
| Compose service | `archiver` (no port) |
| Code | [archiver.go](archiver.go) |
| Uses ports | `app.JobStore`, implemented by [internal/adapter/mysql](../../adapter/mysql/mysql.go); `app.HistoryStore`, implemented by [internal/adapter/cassandra](../../adapter/cassandra/cassandra.go) |

## How it works

Every `ARCHIVER_INTERVAL` (10 s), in batches of 500:

1. Select terminal jobs that are past their retention window:
   - `succeeded` for longer than `ARCHIVE_SUCCEEDED_AFTER` (1 h);
   - `dead` for longer than `ARCHIVE_DEAD_AFTER` (14 days). This is the window in which a tenant can retry them from the DLQ.
2. **Write them to Cassandra first.** Each job goes to `job_history` (partitioned by tenant, task, and month) and to `job_by_id`, both with a 30-day TTL.
3. **Then delete them from MySQL**, guarded by `id AND status AND attempt`.

## Guarantees

- **Never lost.** A crash between steps 2 and 3 leaves a duplicate, never a gap. Cassandra writes are idempotent (same primary key), and the [job service](../jobservice/README.md) deduplicates by job id.
- **No race with manual retries.** If a tenant retries a dead job from the DLQ in the meantime, its status and attempt no longer match the guard and the row stays in MySQL.
- **No secrets in history.** Request headers, request bodies, and response bodies are never archived. URLs are stored without the query string and userinfo.

## Configuration

| Env | Default | |
|---|---|---|
| `ARCHIVER_INTERVAL` | `10s` | Time between sweeps |
| `ARCHIVE_SUCCEEDED_AFTER` | `1h` | Retention of succeeded jobs in MySQL |
| `ARCHIVE_DEAD_AFTER` | `336h` (14 d) | Retention of dead jobs in MySQL (DLQ window) |
| `CASSANDRA_HOSTS`, `CASSANDRA_KEYSPACE`, `CASSANDRA_RF` | `localhost`, `crobat`, `1` | Keyspace and tables are created at startup if missing |
| `MYSQL_DSN`, `SECRETS_KEY` | | See [ARCHITECTURE.md](../../../ARCHITECTURE.md#configuration-env) |

## Scaling

One replica is enough for the local stack. Bulk deletes cause InnoDB purge work that can slow down other writers ([LOADTESTING.md](../../../LOADTESTING.md#bug-4-in-the-harness-api-latency-after-bulk-deletes)), so pace this service rather than adding replicas.
