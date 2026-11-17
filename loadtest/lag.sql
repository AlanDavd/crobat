-- Scheduling-lag report for one load-test run. Expects @run (tenant prefix).
-- lag = started_at - scheduled_at of the attempt that succeeded.

WITH l AS (
  SELECT priority, tenant_id, attempt,
         TIMESTAMPDIFF(MICROSECOND, scheduled_at, started_at) / 1000 AS lag_ms
  FROM jobs WHERE tenant_id LIKE CONCAT(@run, '%') AND status = 'succeeded'
), r AS (
  SELECT priority, lag_ms,
         ROW_NUMBER() OVER (PARTITION BY priority ORDER BY lag_ms) AS rn,
         COUNT(*)     OVER (PARTITION BY priority)                 AS n
  FROM l
)
SELECT ELT(priority + 1, 'critical', 'high', 'normal', 'low') AS priority,
       MAX(n) AS jobs,
       ROUND(MIN(CASE WHEN rn >= 0.50 * n THEN lag_ms END)) AS p50_ms,
       ROUND(MIN(CASE WHEN rn >= 0.95 * n THEN lag_ms END)) AS p95_ms,
       ROUND(MIN(CASE WHEN rn >= 0.99 * n THEN lag_ms END)) AS p99_ms,
       ROUND(MAX(lag_ms)) AS max_ms
FROM r GROUP BY priority ORDER BY priority;

-- Fairness: lag per tenant (tenant 0 is the noisy one).
WITH l AS (
  SELECT tenant_id, TIMESTAMPDIFF(MICROSECOND, scheduled_at, started_at) / 1000 AS lag_ms
  FROM jobs WHERE tenant_id LIKE CONCAT(@run, '%') AND status = 'succeeded'
), r AS (
  SELECT tenant_id, lag_ms,
         ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY lag_ms) AS rn,
         COUNT(*)     OVER (PARTITION BY tenant_id)                 AS n
  FROM l
)
SELECT tenant_id, MAX(n) AS jobs,
       ROUND(MIN(CASE WHEN rn >= 0.50 * n THEN lag_ms END)) AS p50_ms,
       ROUND(MIN(CASE WHEN rn >= 0.99 * n THEN lag_ms END)) AS p99_ms
FROM r GROUP BY tenant_id ORDER BY tenant_id;

-- Totals: nothing lost, nothing stuck, how fast the peak drained.
SELECT COUNT(*) AS jobs,
       SUM(status = 'succeeded') AS succeeded,
       SUM(status = 'dead') AS dead,
       SUM(status IN ('pending', 'running')) AS unfinished,
       SUM(attempt > 1) AS retried,
       ROUND(TIMESTAMPDIFF(MICROSECOND, MIN(scheduled_at), MAX(finished_at)) / 1e6, 2) AS drain_s,
       ROUND(COUNT(*) / (TIMESTAMPDIFF(MICROSECOND, MIN(started_at), MAX(finished_at)) / 1e6)) AS jobs_per_s
FROM jobs WHERE tenant_id LIKE CONCAT(@run, '%');
