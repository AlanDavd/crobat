# Task Service

**Objective:** let tenants create and manage their tasks (the configuration of what to call, when, and how) and store them durably in MySQL.

| | |
|---|---|
| Run | `crobat serve task-api` |
| Compose service | `task-api`, port **8080** |
| Code | [taskservice.go](taskservice.go) |
| Driven by | REST adapter: [internal/adapter/httpapi](../../adapter/httpapi/httpapi.go) (`TaskAPI`) |
| Uses ports | `app.TaskStore` ([ports.go](../../app/ports.go)), implemented by [internal/adapter/mysql](../../adapter/mysql/mysql.go) |

## Responsibilities

- **Create** a task scheduled now, at a date (`at`), or on a cron expression (`cron` + `timezone`).
- **Get**, **list**, and **replace** (`PUT`) tasks.
- **Pause**, **resume**, and **delete** (soft) tasks.
- Validate every input at the boundary. The rules live in `domain.Task.Normalize`: URL scheme, method, priority, timeout, retry bounds, cron syntax, and timezone.

## API

All routes require `Authorization: Bearer <JWT>`. The tenant is the token's `sub` claim and is never read from the body.

| Method & path | Result |
|---|---|
| `POST /tasks` | `201` with the task. An optional `Idempotency-Key` header makes retries safe: the same key returns the original task. |
| `GET /tasks` | The tenant's tasks (deleted ones excluded) |
| `GET /tasks/{id}` | One task |
| `PUT /tasks/{id}` | Full replace of the editable fields |
| `POST /tasks/{id}/pause` | Stops planning new runs |
| `POST /tasks/{id}/resume` | Resumes planning |
| `DELETE /tasks/{id}` | `204`. Soft delete. |
| `GET /healthz` | Liveness, no auth |

Request headers and body are accepted on write and **never returned**. They are encrypted at rest by the MySQL adapter.

## Behaviour

- **"Now" tasks skip the scheduler.** The task and its first job are inserted **in the same transaction**, so the job can be claimed within one executor poll instead of waiting for a scheduler tick.
- **`at` and `cron` tasks** get `next_run_at` and are picked up by the [scheduler](../scheduler/README.md).
- **Edits never touch existing jobs.** Jobs copy the request when they are planned. `next_run_at` is recomputed only when the schedule itself changes.
- **Concurrent edits** use optimistic concurrency on `version`. Losing the race returns `400 "task changed concurrently, retry"`. The scheduler also bumps `version` when it advances a task.
- **Pause** stops new runs from being planned. Jobs already planned (≤ 30 s ahead) still run.
- **Resume** of a cron task restarts from now. Runs missed while paused are skipped on purpose.

## Configuration

| Env | Default | |
|---|---|---|
| `HTTP_ADDR` | `:8080` | Listen address |
| `JWT_SECRET` | *(required)* | HS256 secret for tenant tokens |
| `SECRETS_KEY` | *(required)* | Base64 32-byte AES key for headers and body |
| `MYSQL_DSN` | `crobat:crobat@tcp(localhost:3306)/crobat` | |

## Scaling and availability

Stateless. Run as many replicas as needed behind a load balancer. Every create is one MySQL commit, so throughput is bound by the database. Locally it handles about 2,570 creates/s with 0 errors ([LOADTESTING.md](../../../LOADTESTING.md#api-capacity)).
