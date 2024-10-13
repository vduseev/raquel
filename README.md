# raquel

*Simple and elegant Job Queues for Python using SQL.*

Tired of complex job queues for distributed computing or event based systems?
Do you want full visibility and complete reliability of your job queue?
Raquel is a perfect solution for a distributed task queue and background workers.

* **Simple**: Use **any** existing or standalone SQL database. Just a **single** table!
* **Freedom**: Schedule whatever you want however you want. No frameworks, no restrictions.
* **Reliable**: Uses SQL transactions and handles exceptions, retries, and "at least once" execution. SQL guarantees persistent jobs.
* **Transparent**: Full visibility into which jobs are runnning, which failed and why, which are pending, etc.

## Installation

*Raquel has 0 dependencies. It's light as a feather, simple, pure Python.*

```bash
pip install raquel
```

## Usage

### Basic example with Postgres

On client side, schedule jobs:

```python
import psycopg2.pool
from raquel import Raquel

pool = psycopg2.pool.ThreadedConnectionPool(1, 10, host='localhost', user='postgres', password='postgres')
rq = Raquel(pool)

rq.enqueue('Hello, World!')
```

On worker side, run jobs:

```python
import time
import psycopg2.pool
from raquel import Raquel

pool = psycopg2.pool.ThreadedConnectionPool(1, 10, host='localhost', user='postgres', password='postgres')
rq = Raquel(pool)

while True:
    job = rq.dequeue()
    if job is None:
        time.sleep(1)
    print(job)
```

## Prepare database

Raquel only needs **a single table** to work. Can you believe it?

You can configure the table using ``Raquel.setup()`` method, which will
automatically use the supported syntax for the database you are using (it is 
safe to run it multiple times, it only creates the table once.).

```python
rq.setup()
```

Alternatively, you can create the table manually. For example, in Postgres:

```sql
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
);
CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs (queue);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at ON jobs (scheduled_at);
```

## Comparison to other distributed task queues

| Feature | Raquel | Celery | RQ | Dramatiq | arq | pgqueuer | pq |
|---------|--------|--------|----|----------|-----|----------|----|
| **Special tooling to run workers** | No ✅ | Yes ❌ | Yes ❌ | Yes ❌ | Yes ❌ | Yes ❌ | Yes ❌ |
| **Needs message queue** | No ✅ | Yes ❌ | Yes ❌ | Yes ❌ | Yes ❌ | Yes ❌ | Yes ❌ |
| **Supports SQL** | Yes ✅ | No ❌ | No ❌ | No ❌ | No ❌ | Yes ✅ | Yes ✅ |
| **Full visibility** | Yes ✅ | No ❌ | No ❌ | No ❌ | No ❌ | Yes ✅ | Yes ✅ |
| **Reliable** | Yes ✅ | Yes ✅ | Yes ✅ | Yes ✅ | Yes ✅ | Yes ✅ | Yes ✅ |
| **Supports async** | Yes ✅ | Yes ❌ | Yes ❌ | Yes ❌ | Yes ❌ | Yes ❌ | Yes ❌ |
| **Persistent jobs** | Yes ✅ | Yes ❌ | Yes ❌ | Yes ❌ | Yes ❌ | Yes ✅ | Yes ✅ |
| **Schedule from anywhere** | Yes ✅ | No ❌ | No ❌ | Yes ✅ | No ❌ | Yes ✅ | Yes ✅ |


## Other

* [How to get DBAPI connection in SQLAlchemy](https://docs.sqlalchemy.org/en/20/faq/connections.html)
