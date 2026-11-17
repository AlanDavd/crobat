# Load testing

PRODUCT.md says a scheduler is judged by its peaks, not its average, so these tests reproduce the defining peak of a cron platform: **30,000 executions due in the same second** (09:00:00 in the product's load model). They measure **scheduling lag** (how late each job started compared with when it was due), broken down by priority and by tenant, and check that no job is lost.

Load testing found two real bugs and one design bottleneck, and the fixes are now in the code. On a laptop, the final build drains the 30k peak in **11–24 s** with **zero lost, stuck or duplicated jobs**. It meets the lag targets for high, normal and low priority with a wide margin. Critical sits right at its 1 s target.

## Summary

Final build, default configuration, five runs of the 30k peak:

| Priority | p99 lag target (PRODUCT.md) | Measured p99 (range over 5 runs) | Result |
|---|---|---|---|
| critical | < 1 s | **0.85 – 1.56 s** (median 1.17 s) | ⚠️ at the target; met in 1 of 5 runs |
| high | < 20 s | **2.4 – 5.4 s** | ✅ |
| normal | a few minutes | **8.3 – 16.1 s** | ✅ |
| low | < 20 min | **11.2 – 23.9 s** | ✅ |

| Check | Result |
|---|---|
| Jobs lost / left unfinished / dead | **0 / 0 / 0** in every run of the final build |
| Duplicate executions (retries) | **0** in the final build (1 in an earlier build; see bug 3) |
| Tenant fairness | Each tenant's p50 within 5 % of the others, while one tenant had 70 % of the load |
| Low-priority starvation | None. Low jobs finish while higher-priority jobs are still being drained. |
| Executor throughput during the peak | **1,250 – 2,400 jobs/s**, bound by MySQL CPU |
| Task API (`POST /tasks`) | 1,000 creates/s at p99 48–373 ms with 0 errors. Saturates at about **2,570 creates/s** (p99 700 ms, still 0 errors). |

## Tool choice

I compared the open-source HTTP load generators that come up most often for local testing:

| Tool | Language for scripts | Fit here |
|---|---|---|
| **k6** (Grafana) | JavaScript | ✅ Chosen. Exact arrival-rate control (`constant-arrival-rate`), built-in thresholds that can fail CI, and an official Docker image, so nothing to install. |
| Locust | Python | Good for modelling user journeys. Rate control is less precise. Would add a Python toolchain to a Go repo. |
| Vegeta | Go CLI | Great for constant-rate shell benchmarks. Building a different JSON body per request (priority, tenant, `Idempotency-Key`) takes more work than in k6. |
| JMeter | XML/GUI | Heavier per virtual user and not code-first. |

k6 drives the API side. The lag numbers can't come from any HTTP tool, because the interesting moment is when the scheduler and executors fire jobs, long after the API calls return. They are computed from the database (`started_at − scheduled_at` per job) by [loadtest/lag.sql](loadtest/lag.sql).

Sources: [k6 vs Locust vs JMeter in 2026 (DEV)](https://dev.to/qainsights/jmeter-vs-k6-vs-locust-in-2026-which-load-testing-tool-should-you-pick-366f), [k6 vs Artillery vs Locust vs wrk2 (DEV)](https://dev.to/_6638a39c349d7e9c85ee20/load-testing-guide-2026-k6-vs-artillery-vs-locust-vs-wrk2-for-performance-testing-11h3), [Open source load testing tools (BlazeMeter)](https://www.blazemeter.com/blog/open-source-load-testing-tools), [grafana/k6 on Docker Hub](https://hub.docker.com/r/grafana/k6).

## The workload

[loadtest/run.sh](loadtest/run.sh) runs one complete test:

1. Mints JWTs for 4 fresh tenants.
2. Runs [loadtest/create-tasks.js](loadtest/create-tasks.js) in the `grafana/k6:1.8.0` container on the compose network. k6 creates **30,000 one-shot tasks at 1,000/s** through `task-api`, **all due at the same second** `T` (45 s after the last create).
   - **Priority mix:** 5 % critical, 15 % high, 50 % normal, 30 % low.
   - **Tenant mix:** one noisy tenant with 70 % of the tasks; three small tenants with 10 % each. Every tenant gets the same priority mix.
   - Every task carries a secret header, so the encryption path is exercised.
   - Every request has an `Idempotency-Key`.
3. Stops the archiver so finished jobs stay in MySQL for the report.
4. Waits until every task has been planned and every job has finished.
5. Prints lag percentiles by priority and by tenant, plus totals (lost, retried, drain time, throughput).
6. Deletes the run's rows. Use `KEEP=1` to keep them for inspection.

Along the way the scheduler plans the jobs in the 30 s before `T`, the executors claim them up to one poll ahead, and every job calls the echo endpoint at `T`.

```bash
docker compose up -d --build
loadtest/run.sh                          # 30k tasks, 1000/s, 4 tenants
TASKS=5000 RATE=500 loadtest/run.sh      # smaller
RATE=3000 loadtest/run.sh                # API stress
EXECUTOR_WORKERS=256 docker compose up -d executor   # change capacity, then rerun
```

**Environment:** Apple M1 laptop, Docker Desktop VM with 8 vCPU and 3.8 GiB RAM. Every container ran on that one VM: MySQL 8.4 with full durability (`innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`), Cassandra, all six services, 2 executor replicas, the echo target and k6.

## What the tests found, and what changed

Every row is the same 30k-peak test. The first four rows are consecutive builds.

| # | Build | Jobs/s | Drain | Critical p99 | High p99 | Normal p99 | Low p99 | Notes |
|---|---|---|---|---|---|---|---|---|
| 1 | As first written | **256** | 116 s | 6.2 s | 25.6 s | 87.8 s | 116.2 s | Throughput exactly = 2 × 64 workers / 0.5 s poll |
| 2 | + claim again after a full batch | 340 | 88 s | 6.2 s | 25.4 s | 62.4 s | 87.7 s | Test fixed so each tenant gets the full priority mix |
| 3 | + wake claim loop when slots free | 815 | 37 s | 1.5 s | 5.3 s | 17.8 s | 25.5 s | 1 result lost to a deadlock and re-run after its lease expired |
| 4 | + retry deadlocked transactions | 1,419 | 20 s | 1.8 s | 4.8 s | 14.4 s | 19.5 s | 0 lost results. API slow in this run (see bug 4). |
| 5 | Final build, 5 runs | 1,246 – 2,391 | 11 – 24 s | 0.85 – 1.56 s | 2.4 – 5.4 s | 8.3 – 16.1 s | 11.2 – 23.9 s | 0 lost, 0 retried |

### Bug 1: executor throughput capped at `workers ÷ poll interval`

An HTTP call took **4.9 ms** on average, yet each worker finished only one job per 500 ms. A claimed job holds its worker slot while it waits for its timer and then makes its call. When the slot freed up, the executor didn't claim again until the next poll tick, so throughput could never exceed `replicas × workers / poll` = 256 jobs/s, however fast the endpoints were.

**Fix** ([services.go](internal/app/services.go), `Executor.Run`): a finishing job signals the claim loop as soon as a quarter of the slots are free. The executor then refills them immediately, in batches, instead of waiting for the tick. That took throughput from 256 to 815 jobs/s, and from 815 to 1,400–2,400 once bug 3 was fixed.

### Bug 2 (in the test itself): tenants looked unfair, but weren't

The first run reported small tenants about 50 % later than the noisy one. The cause was the script: it picked the tenant with `i % 10` and the priority with `i % 20`, so the small tenants never got critical or high jobs. Once tenants were picked per block of 20 tasks (`floor(i / 20)`), every tenant's p50 landed within 5 % of the others in every run. Per-batch fair share (`domain.FairPick`) works as intended.

### Bug 3: deadlocks could lose a result

Claims lock candidate rows through the priority index and then update them. Result writes update rows by primary key, and that changes the same secondary indexes. Under load InnoDB detected **39 deadlocks** in one run. When the victim was a result write, the job stayed `running` until its lease expired (timeout + 30 s), and then the reconciler sent it again. That is still at-least-once, but it cost **36 s of lag and a duplicate call**.

**Fix** ([mysql.go](internal/adapter/mysql/mysql.go), `retryDeadlock`): claims and result writes rerun up to 4 times on error 1213 (deadlock) or 1205 (lock wait timeout), which is standard practice for InnoDB. Deadlocks and retries both dropped to **0** in every later run. The fencing guard keeps a retried write safe.

### Bug 4 (in the harness): API latency after bulk deletes

When the script deleted the previous run's 60k rows right before starting, API p99 jumped from about 270 ms to **1.8–2.1 s**, and k6 dropped 2–3k iterations. InnoDB was purging the deleted rows in the background while k6 was writing. Cleanup now runs at the end of each run, so the database is quiet before the next one. Production has the same pattern: the archiver's deletes should be paced. See next steps.

## API capacity

`POST /tasks` with encryption, an idempotency key, and one durable commit per create:

| Target rate | Achieved | p50 | p95 | p99 | Errors | Dropped by k6 |
|---|---|---|---|---|---|---|
| 1,000/s (7 clean runs) | 996–1,000/s | 1.2–14 ms | 17–178 ms | **48–373 ms** | 0 | 0–30 |
| 3,000/s | **2,573/s** | 63 ms | 444 ms | 700 ms | 0 | 3,920 |

The API saturates at about 2,570 creates/s on this machine, and it never returned an error, even at saturation. Each create is one InnoDB commit, so the ceiling is MySQL's commit rate. task-api is stateless and scales horizontally, so in production this limit belongs to the database.

## Where the remaining limit is

- **More executors don't help.** Running 256 workers per executor (4× the default) drained at 1,897 jobs/s, inside the 64-worker range.
- **MySQL is the busiest component.** Sampled during the drain, MySQL used **280–325 % CPU** against about 60 % per executor and 17 % for the echo target. The InnoDB redo log was fsyncing about 480 times per second. Group commit merges the concurrent result writes, but every job still costs a claim lock, a `running` update, a result update, and maintenance of six secondary indexes.
- **Fewer locked candidates didn't help either.** Locking 2× instead of 4× the batch size per claim gave 1,454–1,701 jobs/s, which is within the noise, so I kept 4× (it is the setting already shown to keep tenants even).
- **Run-to-run variance is large** (1,246 to 2,391 jobs/s with identical settings). Everything shares one laptop VM, Cassandra included. Treat the ranges above as the result, not any single run.

PRODUCT.md (§8.1) anticipates this trade-off: *"all the claiming load lands on MySQL, so the jobs table has to be sharded sooner."*

### Critical lag

All 30k jobs become due in the same second. The claim serves critical first, so the 1,500 critical jobs are effectively the head of the queue, and their p99 lag ≈ 1,500 ÷ claim throughput ≈ 0.6–1.2 s, plus up to one poll. Ways to get below 1 s with margin, from cheapest to most work:

1. **Batch result writes.** Collect results for about 5 ms and write them in one transaction, which cuts commits per job by about 2×.
2. **Claim further ahead for critical only.** Jobs already sit on in-memory timers, so a longer lookahead for priority 0 makes the claim work for critical happen before `T` instead of at `T`.
3. **Shard `jobs`** (Vitess, by hash) so claim throughput scales with shards.

## Caveats

- This is a **single-node laptop test**. It shows the design behaves correctly under a real peak, and it finds the bottleneck. It does not show the PRODUCT.md target of 500k jobs/s, which needs sharded MySQL on real hardware.
- The echo target answers in about 5 ms. Slower tenant endpoints hold worker slots longer; capacity planning there is about `EXECUTOR_WORKERS`, not MySQL.
- The archiver was paused during the tests. Its deletes add purge load (see bug 4).
- The scheduler was not the bottleneck. In a run that kept its rows, it planned all 30,000 jobs in 8.3 s, finishing **19 s before** they were due.

## Next steps

- [ ] Batch result writes (above). Expected to give the biggest single gain for critical lag.
- [ ] Per-priority claim lookahead.
- [ ] Pace the archiver's deletes (smaller batches, and the planned `DELETE ... IN (...)`).
- [ ] Export Prometheus histograms for lag per priority, so these numbers come from production rather than SQL.
- [ ] Repeat on separate machines: MySQL on its own host, k6 off the cluster.
- [ ] A sharded run (Vitess) to measure how claim throughput scales with shards.
