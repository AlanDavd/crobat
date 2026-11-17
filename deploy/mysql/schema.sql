-- Loaded by the mysql container on first start (docker-entrypoint-initdb.d).
-- All times are UTC.

CREATE TABLE IF NOT EXISTS tasks (
  id              CHAR(36)     NOT NULL PRIMARY KEY,
  tenant_id       VARCHAR(64)  NOT NULL,
  name            VARCHAR(255) NOT NULL,
  cron_expr       VARCHAR(255) NULL,
  timezone        VARCHAR(64)  NOT NULL DEFAULT 'UTC',
  run_at          DATETIME(3)  NULL,
  priority        TINYINT      NOT NULL,             -- 0 critical .. 3 low
  method          VARCHAR(10)  NOT NULL,
  url             VARCHAR(2048) NOT NULL,
  secret          BLOB NULL,                        -- AES-GCM(headers + body)
  timeout_ms      INT          NOT NULL,
  max_attempts    INT          NOT NULL,
  backoff_ms      INT          NOT NULL,
  status          ENUM('active','paused','deleted') NOT NULL,
  next_run_at     DATETIME(3)  NULL,
  idempotency_key VARCHAR(255) NULL,
  version         INT          NOT NULL,
  created_at      DATETIME(3)  NOT NULL,
  updated_at      DATETIME(3)  NOT NULL,
  KEY idx_tenant (tenant_id, created_at),
  KEY idx_due (status, next_run_at),
  UNIQUE KEY uq_idem (tenant_id, idempotency_key)
) ENGINE=InnoDB;

-- The hot table and the queue.
CREATE TABLE IF NOT EXISTS jobs (
  id               CHAR(36)     NOT NULL PRIMARY KEY, -- also the Idempotency-Key
  tenant_id        VARCHAR(64)  NOT NULL,
  task_id          CHAR(36)     NOT NULL,
  scheduled_at     DATETIME(3)  NOT NULL,
  priority         TINYINT      NOT NULL,
  method           VARCHAR(10)  NOT NULL,
  url              VARCHAR(2048) NOT NULL,
  secret           BLOB NULL,
  timeout_ms       INT          NOT NULL,
  max_attempts     INT          NOT NULL,
  backoff_ms       INT          NOT NULL,
  status           ENUM('pending','running','succeeded','dead') NOT NULL,
  attempt          INT          NOT NULL DEFAULT 0,  -- fencing token
  next_attempt_at  DATETIME(3)  NOT NULL,
  worker_id        VARCHAR(64)  NULL,
  lease_expires_at DATETIME(3)  NULL,
  http_status      INT          NULL,
  error            VARCHAR(1024) NULL,
  response_body    VARCHAR(1024) NULL,
  created_at       DATETIME(3)  NOT NULL,
  started_at       DATETIME(3)  NULL,
  finished_at      DATETIME(3)  NULL,
  KEY idx_claim   (status, priority, next_attempt_at),  -- executor claim
  KEY idx_oldest  (status, next_attempt_at),            -- anti-starvation lane
  KEY idx_lease   (status, lease_expires_at),           -- reconciler
  KEY idx_archive (status, finished_at),                -- archiver
  KEY idx_dlq     (tenant_id, status),                  -- DLQ inbox
  KEY idx_task    (tenant_id, task_id, scheduled_at),   -- history
  UNIQUE KEY uq_task_run (task_id, scheduled_at)        -- scheduler idempotency
) ENGINE=InnoDB;
