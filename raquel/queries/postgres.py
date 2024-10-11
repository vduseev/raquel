create_jobs_table = """
CREATE TABLE IF NOT EXISTS jobs (
    id BIGSEREIAL PRIMARY KEY,
    queue TEXT NOT NULL,
    payload TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    locked_by TEXT,
    max_age INTEGER,
    max_retry_count INTEGER,
    max_retry_exponent INTEGER DEFAULT 32,
    min_retry_delay INTEGER NOT NULL DEFAULT 1000,
    max_retry_delay INTEGER NOT NULL DEFAULT 43200000,
    enqueued_at BIGINT NOT NULL,
    scheduled_at BIGINT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    failed_error TEXT,
    failed_traceback TEXT,
    cancelled_reason TEXT,
    locked_at BIGINT,
    finished_at BIGINT
)
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
    queue = ?
    AND (status = 'queued' OR status = 'failed')
    AND scheduled_at <= ?
    AND (
        max_age IS NULL
        OR enqueued_at + max_age >= ?
    )
ORDER BY scheduled_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED
"""
