create_jobs_table = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
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
