# Crobat: Product Requirements

**Distributed job scheduler: cron as a service**

| | |
|---|---|
| **Status** | v1 implemented locally, with a CLI and no frontend. See [ARCHITECTURE.md](ARCHITECTURE.md) and [LOADTESTING.md](LOADTESTING.md). |
| **Scale target** | ~1 billion executions/day, peaks of 500,000/s |

---

## 1. Summary

Crobat is cron as a service, written in Go. A tenant tells it to call an HTTP endpoint **now**, **at a date**, or **on a cron expression**, and Crobat makes the call with a priority, retries, a dead letter queue (DLQ), and a queryable history of every run.

Crobat has **no message broker**: no Kafka, no RabbitMQ, no Redis. The queue is a MySQL table. That one decision removes the dual write between a database and a broker, and most of the failure modes that come with it.

---

## 2. Problem

### 2.1 Schedulers are judged by their peaks

People schedule on the hour. They write `0 9 * * *`, not `17 9 * * *`. The average load of a cron platform is low, but nearly all of it lands on second `:00`, and at 09:00 the every-minute, hourly, and daily crons all meet.

In the reference load model:

| Metric | Value |
|---|---|
| Average load | ~38 executions/s |
| Load at 09:00:00 | **30,000 executions due in the same second** |

A scheduler that handles the average but not that second has failed.

### 2.2 The dual-write problem

A typical design saves a job in a database and then publishes it to a broker. Nothing makes those two writes atomic. A crash between them leaves the job in one and not the other, and the two disagree forever. Crobat is designed so that this cannot happen.

---

## 3. Goals and non-goals

### Goals

- Run an HTTP call now, at a date, or on a cron expression, reliably and on time.
- Never lose an accepted task, and never drop an execution silently.
- Serve critical work first without starving low-priority work or any one tenant.
- Keep one source of truth for every job's state.
- Keep tenant secrets out of logs and history.

### Non-goals

- **Exactly-once delivery.** It does not exist over a network. Crobat promises at-least-once and gives receivers a key to deduplicate with.
- **A message broker** of any kind, including a transactional outbox with a relay.
- **A web frontend** in v1. Tenants use the REST API and the CLI.
- **Arbitrary code execution.** Crobat only makes HTTP calls to tenant endpoints.

---

## 4. Core concepts

Two words carry the whole design.

| Concept | Definition | Change rate |
|---|---|---|
| **Task** | The configuration: schedule, priority, HTTP request, timeout, retry policy, and `next_run_at` (the next time it is due). | Rarely |
| **Job** | One execution of a task. It is written, claimed, retried, and finished. | Constantly: thousands of times per second at scale |

Because they behave differently, they live in **different tables**. When a job is created it **copies** the task's request, timeout, retry policy, and priority into its own row. Editing a task never changes a job that already exists: the job that fires at 09:00 is the one that was planned, even if someone edited the URL at 08:59:59.

Other terms used in this document:

- **Lease:** an executor's temporary right to run a job, stored in the job's row as a deadline.
- **Fencing token:** a number that increases with each new owner of a job, so writes carrying an old number are rejected. In Crobat this is the `attempt` counter.
- **Scheduling lag:** how late an execution starts compared with when it was due.
- **DLQ:** dead letter queue. Jobs whose attempts are used up wait there for manual review or retry.

---

## 5. Functional requirements

| ID | Requirement |
|---|---|
| **FR-1** | A tenant can schedule a task to run **now**, **at** a date and time, or on a **cron** expression. |
| **FR-2** | A tenant can **create, edit, pause, resume, and delete** tasks. |
| **FR-3** | Every task has a **priority**: `critical`, `high`, `normal`, or `low`. Critical never waits behind low. |
| **FR-4** | Failed jobs are **retried** with exponential backoff, up to a configurable maximum number of attempts. |
| **FR-5** | Jobs that use up their attempts go to a **DLQ**, where the tenant can review them and **retry them manually**. |
| **FR-6** | A tenant can query a **job's status** and a **task's execution history**. |
| **FR-7** | Every call carries the headers `Idempotency-Key` (the job id, stable across attempts), `X-Scheduled-At`, and `X-Attempt`. |
| **FR-8** | Task creation is idempotent: `POST` accepts an `Idempotency-Key` header. |
| **FR-9** | The tenant is identified from the request's **JWT**, never from the request body. |

---

## 6. Non-functional requirements

| ID | Area | Requirement |
|---|---|---|
| **NFR-1** | Durability | An accepted task is never lost. |
| **NFR-2** | Delivery | Every execution happens **at least once** and is never dropped silently. Each job carries a stable idempotency key so the receiver can deduplicate. |
| **NFR-3** | Scheduling lag (p99) | Critical **< 1 s** · High **< 20 s** · Normal **< a few minutes** · Low **< 20 minutes** |
| **NFR-4** | Fairness | Low priority never starves. Tenants get a fair share of capacity. |
| **NFR-5** | Availability | API availability of **99.99 %**. |
| **NFR-6** | Security | Secrets are encrypted and never appear in logs or history. Tenant URLs cannot be used to reach internal networks (SSRF). |
| **NFR-7** | Scale | About **1 billion executions/day**, with peaks of **500,000/s**. |

---

## 7. Solution overview

### 7.1 Services

Six services in Go, each built as a hexagon (ports and adapters), and two databases.

| Service | Responsibility |
|---|---|
| **Task Service** | API to create, edit, pause, resume, and delete tasks in MySQL. |
| **Scheduler** | Turns due tasks into jobs. |
| **Executors** | Claim jobs, call the endpoints, and save the results. |
| **Reconciler** | Rescues jobs whose executor died. |
| **Archiver** | Moves finished jobs from MySQL to Cassandra. |
| **Job Service** | The read side: job status, task history, the DLQ, and manual retries. |

| Store | Holds |
|---|---|
| **MySQL** | `tasks` and `jobs`. The `jobs` table is the hot table *and* the queue. |
| **Cassandra** | Execution history: append-only, partitioned by tenant, task, and month, with a 30-day TTL. |

What is missing matters as much as what is there: **there is no message broker.**

### 7.2 The life of a job

Take a task with the cron expression `0 9 * * *` and follow tomorrow's run.

1. **The scheduler creates it ahead of time.** Every 5 seconds the scheduler takes the active tasks due in the next 30 seconds. In one transaction it inserts their jobs as `pending` and moves each task's `next_run_at` to the following run. By 08:59:35, tomorrow's job exists. `UNIQUE(task_id, scheduled_at)` makes planning the same run twice a no-op.
2. **An executor claims it.** Each executor takes as many pending jobs as it has free workers, among those due before its next poll, using `FOR UPDATE SKIP LOCKED`. That locks the rows it reads and skips the ones another transaction already holds, so executors never wait for each other. It marks them `running` with its own id, adds one to `attempt`, gives each a lease, and **commits before any HTTP call**.
3. **The job waits for its second.** Each claimed job sleeps on its own in-memory timer until 09:00:00, and then the executor sends the request with its three headers.
4. **The result is saved only if the job is still ours.** A 2xx makes it `succeeded`. Anything else sends it back to `pending` with exponential backoff, or to `dead` once its attempts are used up. If the executor dies instead, the lease expires and the reconciler returns the job to `pending`.
5. **Finished jobs move to history.** The archiver writes them to Cassandra and deletes them from MySQL: **1 hour** after they succeed, or **14 days** after they die. The 14 days are the window in which a tenant can retry them from the DLQ.

---

## 8. Key design decisions

### 8.1 The table is the queue

**Decision:** jobs live only in MySQL, and executors claim them with `SELECT ... FOR UPDATE SKIP LOCKED`.

**Alternatives considered:**

- **SQS, RabbitMQ, or Kafka.** Every job would have to be written twice, to the database and to the queue, and nothing makes those two writes atomic. Kafka has a second problem for this workload: it tracks progress with one offset per partition, so a slow job holds back every job behind it.
- **A transactional outbox** (write to the database, and let a relay publish to the queue). This removes the dual write but adds the relay and some latency.

**Rationale:** with one source of truth, everything else gets simpler. The idea comes from Shopify moving inventory reservations from Redis to MySQL for the same reason, claiming rows with `SKIP LOCKED`.

**Trade-off:** all the claiming load lands on MySQL, so the jobs table has to be sharded sooner, by hash, with something like Vitess.

### 8.2 MySQL for the hot table, not Postgres

**Decision:** the `jobs` table runs on MySQL (InnoDB). On AWS that means RDS for MySQL or Aurora MySQL.

**Rationale:** every state change of a job is conditional, `UPDATE ... WHERE status = 'running' AND ...`, and in MySQL that is a cheap `WHERE`. Postgres would pay more for this table. Its indexes point to the physical row, so an update can rewrite every index. The escape is a HOT update (heap-only tuple, an update that skips the indexes), which is impossible when an indexed column changes, and the claim needs `status` indexed. Postgres would also bring vacuum and bloat to a table with this much churn.

### 8.3 Cassandra for history only

**Decision:** finished jobs are archived to Cassandra. Active jobs never touch it.

**Rationale:** Cassandra takes huge write volumes, but it resolves conflicts with last-write-wins (the write with the newest timestamp wins; nothing is compared). A conditional update needs a lightweight transaction, which is a Paxos round on every write. That is far too expensive for every state change. Once a job is finished, though, it never changes again, so history is append-only and those problems go away.

**Data model:** partitioned by tenant, task, and month, with a 30-day TTL.

**Ordering guarantee:** the archiver writes to Cassandra first and deletes from MySQL second. A crash in between leaves a duplicate, never a lost job.

### 8.4 Leases in the database, not locks in Redis

**Decision:** the lease is a column in the job's row, `lease_expires_at`.

**Rationale:** the claim already writes the job's row. A lock in Redis would be a second source of truth that can disagree with the database, and that can vanish in a failover, without saving a single write.

**Detail:** the lease is counted from the job's **due time**, not from the claim. Otherwise the reconciler would take back jobs that were still waiting on their timer.

**Principle:** never run a job that is not persisted.

### 8.5 At-least-once, with a fencing token

If a call times out, there is no way to know whether the endpoint did the work. Exactly-once delivery over a network does not exist, so Crobat promises **at-least-once** and gives the receiver a way to deduplicate: the same `Idempotency-Key` on every attempt of a job.

What the database *can* guarantee is that a late result never corrupts the job. Every result write carries this guard:

```sql
WHERE id = ? AND status = 'running' AND worker_id = ? AND attempt = ?
```

`attempt` grows on every claim, so it works as a fencing token. Suppose executor A is slow, its lease expires, and executor B claims the job. A's late write then affects 0 rows. What the guard cannot prevent is the endpoint receiving the call twice, and that is what the idempotency key is for.

### 8.6 On time without polling every second

Precision does not come from polling the database often. It comes from three things:

- The scheduler creates jobs **up to 30 seconds ahead**.
- Executors claim them **shortly before** their time.
- Each job fires from **its own in-memory timer** at the exact second.

Two rules make the scheduler resilient to its own delays:

- Its query has **no lower bound** (`next_run_at <= horizon`), so a task the scheduler gets to late is delayed but never skipped.
- The next run is computed from the **previous `next_run_at`**, not from now, so the runs missed during an outage are caught up afterwards.

### 8.7 SSRF is checked when connecting, not when saving the URL

Executors call URLs chosen by tenants from inside the network, next to MySQL and the cloud metadata service. Server-side request forgery (SSRF) is a tenant using the executor to reach them.

Validating the URL when the task is created is not enough, because of **DNS rebinding**: a name can resolve to a public IP at validation time and to an internal one at call time. So the check runs in the `Control` hook of Go's `net.Dialer`, **after DNS resolution and right before `connect()`, on the exact IP being dialed**.

- Only public unicast addresses on ports 80 and 443 get through.
- No proxy and no redirects.
- Blocked calls get a generic error.

---

## 9. Success metrics

| Metric | Target | How it is measured |
|---|---|---|
| Lost accepted tasks | 0 | Every created task produces jobs; reconciliation of `tasks` against `jobs` and history |
| Silently dropped executions | 0 | Every job ends `succeeded` or `dead` (visible in the DLQ) |
| p99 scheduling lag | Per priority (NFR-3) | `started_at − scheduled_at` per job |
| Low-priority and tenant fairness | No starvation; even lag across tenants | Lag percentiles by priority and by tenant under peak load |
| API availability | 99.99 % | Successful responses / total, monthly |

The peak test in [LOADTESTING.md](LOADTESTING.md) measures these against the 30,000-jobs-in-one-second model.

---

## 10. Related documents

- [README.md](README.md): overview and quick start
- [ARCHITECTURE.md](ARCHITECTURE.md): how the system is built (code layout, data model, configuration, known limits)
- [LOADTESTING.md](LOADTESTING.md): measured behaviour under the defining peak
