CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    queue TEXT NOT NULL,
    payload TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    max_age BIGINT,
    max_retry_count INTEGER,
    min_retry_delay INTEGER DEFAULT 1000,
    max_retry_delay INTEGER DEFAULT 43200000,
    backoff_base INTEGER DEFAULT 1000,
    enqueued_at BIGINT NOT NULL DEFAULT extract(epoch from now()) * 1000,
    scheduled_at BIGINT NOT NULL DEFAULT extract(epoch from now()) * 1000,
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    error_trace TEXT,
    claimed_by TEXT,
    claimed_at BIGINT,
    finished_at BIGINT
);
CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs (queue);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at ON jobs (scheduled_at);
CREATE INDEX IF NOT EXISTS idx_jobs_claimed_by ON jobs (claimed_by);
