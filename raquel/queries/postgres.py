from .base import QueriesBase


class PostgresQueries(QueriesBase):
    create_jobs_table =  """
CREATE TABLE IF NOT EXISTS jobs (
    id BIGSERIAL PRIMARY KEY,
    queue TEXT NOT NULL DEFAULT 'default',
    payload TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    locked_by TEXT,
    max_age INTEGER,
    max_retry_count INTEGER,
    max_retry_exponent INTEGER DEFAULT 32,
    min_retry_delay INTEGER NOT NULL DEFAULT 1000,
    max_retry_delay INTEGER NOT NULL DEFAULT 43200000,
    enqueued_at BIGINT NOT NULL DEFAULT CAST(
        EXTRACT(epoch from (NOW() AT TIME ZONE 'UTC')) * 1000 AS BIGINT
    ),
    scheduled_at BIGINT NOT NULL DEFAULT CAST(
        EXTRACT(epoch from (NOW() AT TIME ZONE 'UTC')) * 1000 AS BIGINT
    ),
    attempts INTEGER NOT NULL DEFAULT 0,
    failed_error TEXT,
    failed_traceback TEXT,
    cancelled_reason TEXT,
    locked_at BIGINT,
    finished_at BIGINT
)
"""

    create_jobs_index_queue = """
CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs (queue)
"""

    create_jobs_index_status = """
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status)
"""

    create_jobs_index_scheduled_at = """
CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at ON jobs (scheduled_at)
"""

    drop_jobs_table = """
DROP TABLE IF EXISTS jobs
"""

    insert_job = """
INSERT INTO jobs (
  queue,
  payload,
  status,
  max_age,
  max_retry_count,
  max_retry_exponent,
  min_retry_delay,
  max_retry_delay,
  enqueued_at,
  scheduled_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
RETURNING id
"""

    select_job = """
SELECT
    id,
    queue,
    payload,
    status,
    max_age,
    max_retry_count,
    max_retry_exponent,
    min_retry_delay,
    max_retry_delay,
    enqueued_at,
    scheduled_at,
    attempts,
    failed_error,
    failed_traceback,
    cancelled_reason,
    locked_at,
    finished_at
FROM jobs
WHERE id = %s
"""

    select_jobs = """
SELECT
    id,
    queue,
    payload,
    status,
    max_age,
    max_retry_count,
    max_retry_exponent,
    min_retry_delay,
    max_retry_delay,
    enqueued_at,
    scheduled_at,
    attempts,
    failed_error,
    failed_traceback,
    cancelled_reason,
    locked_at,
    finished_at
FROM jobs
WHERE queue = %s
ORDER BY scheduled_at DESC
"""

    select_jobs_count = """
SELECT
    COUNT(*)
FROM jobs
WHERE queue = %s
"""

    select_jobs_count_status = """
SELECT
    COUNT(*)
FROM jobs
WHERE queue = %s
  AND status = %s
"""

    select_oldest_job = """
SELECT
    id,
    queue,
    payload,
    status,
    max_age,
    max_retry_count,
    max_retry_exponent,
    min_retry_delay,
    max_retry_delay,
    enqueued_at,
    scheduled_at,
    attempts,
    failed_error,
    failed_traceback,
    cancelled_reason,
    locked_at,
    finished_at
FROM jobs
WHERE
    queue = %s
    AND (status = 'queued' OR status = 'failed')
    AND scheduled_at <= %s
    AND (
        max_age IS NULL
        OR enqueued_at + max_age >= %s
    )
ORDER BY scheduled_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED
"""

    select_queues = """
SELECT
    queue,
    SUM(1) AS total,
    SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued,
    SUM(CASE WHEN status = 'locked' THEN 1 ELSE 0 END) AS locked,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
FROM jobs
GROUP BY queue
"""

    update_job_lock = """
UPDATE jobs
SET
    status = 'locked',
    locked_at = %s,
    locked_by = %s
WHERE id = %s
"""

    update_job_done = """
UPDATE jobs
SET
    status = %s,
    attempts = %s,
    cancelled_reason = %s,
    finished_at = %s
WHERE id = %s
"""

    update_job_retry = """
UPDATE jobs
SET
    status = 'queued',
    scheduled_at = %s,
    attempts = %s,
    failed_error = %s,
    failed_traceback = %s,
    finished_at = %s
WHERE id = %s
"""
