# Job Service

**Objective:** the read side. It lets tenants see what happened to their executions: job status, a task's history, the DLQ, and manual retries.

| | |
|---|---|
| Run | `crobat serve job-api` |
| Compose service | `job-api`, port **8081** |
| Code | [jobservice.go](jobservice.go) |
| Driven by | REST adapter: [internal/adapter/httpapi](../../adapter/httpapi/httpapi.go) (`JobAPI`) |
| Uses ports | `app.JobStore`, implemented by [internal/adapter/mysql](../../adapter/mysql/mysql.go); `app.HistoryStore`, implemented by [internal/adapter/cassandra](../../adapter/cassandra/cassandra.go) |

## API

All routes require `Authorization: Bearer <JWT>`. Every read is scoped to the token's tenant.

| Method & path | Result |
|---|---|
| `GET /jobs/{id}` | The job's status, attempts, and last result. Looked up in MySQL first, then in Cassandra (`"archived": true`). |
| `GET /tasks/{id}/jobs?limit=n` | The task's executions, newest first (default 50, max 500) |
| `GET /dlq` | The tenant's dead jobs |
| `POST /jobs/{id}/retry` | Sends a dead job back to `pending` with **one more attempt** |
| `GET /healthz` | Liveness, no auth |

## Behaviour

- **History merges both stores.** Recent and in-flight jobs come from MySQL; archived ones come from Cassandra (this month and the previous month, matching the 30-day TTL). Results are deduplicated by job id, because the [archiver](../archiver/README.md) may briefly leave a job in both.
- **Manual retry** keeps the job's id, so the endpoint receives the **same `Idempotency-Key`**. It sets `max_attempts = attempt + 1`, so the retry is a single extra attempt. It only works while the job is still in MySQL, which is 14 days after it died.
- **Secrets are never returned.** Request headers and body are not serialized.

## Configuration

| Env | Default | |
|---|---|---|
| `HTTP_ADDR` | `:8081` | Listen address |
| `JWT_SECRET` | *(required)* | HS256 secret for tenant tokens |
| `CASSANDRA_HOSTS`, `CASSANDRA_KEYSPACE`, `CASSANDRA_RF` | `localhost`, `crobat`, `1` | |
| `MYSQL_DSN`, `SECRETS_KEY` | | See [ARCHITECTURE.md](../../../ARCHITECTURE.md#configuration-env) |

## Scaling and availability

Stateless. Run as many replicas as needed. Reads go to indexed MySQL queries and single-partition Cassandra queries.
