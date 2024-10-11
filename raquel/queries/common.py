create_jobs_index_queue = """
CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs (queue)
"""

create_jobs_index_status = """
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status)
"""

create_jobs_index_scheduled_at = """
CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at ON jobs (scheduled_at)
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
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
WHERE id = ?
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
WHERE queue = ?
ORDER BY scheduled_at DESC
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
"""

select_queues = """
SELECT
    queue,
    SUM(id) AS total,
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
    locked_at = ?,
    locked_by = ?
WHERE id = ?
"""

update_job_done = """
UPDATE jobs
SET
    status = ?,
    attempts = ?,
    cancelled_reason = ?,
    finished_at = ?
WHERE id = ?
"""

update_job_retry = """
UPDATE jobs
SET
    status = 'queued',
    scheduled_at = ?,
    attempts = ?,
    failed_error = ?,
    failed_traceback = ?,
    finished_at = ?
WHERE id = ?
"""
