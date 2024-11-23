# raquel

*Simple and elegant Job Queues for Python using SQL.*

Tired of complex job queues for distributed computing or event based systems?
Do you want full visibility and complete reliability of your job queue?
Raquel is a perfect solution for a distributed task queue and background workers.

* **Simple**: Use **any** existing or standalone SQL database. Requires a **single** table!
* **Flexible**: Schedule whatever you want however you want. No frameworks, no restrictions.
* **Reliable**: Uses SQL transactions and handles exceptions, retries, and "at least once" execution. SQL guarantees persistent jobs.
* **Transparent**: Full visibility into which jobs are runnning, which failed and why, which are pending, etc.

## Installation

```bash
pip install raquel
```

To install with async support, specify the `asyncio` extra. This simply
adds the `greenlet` package as a dependency.

```bash
pip install raquel[asyncio]
```

## Usage

### Basic example

On the client side, schedule your jobs. Payload can be any JSON-serializable object
or simply a string.

```python
from raquel import Raquel

rq = Raquel("postgresql+psycopg2://postgres:postgres@localhost/postgres")
rq.enqueue(queue"default", payload="Hello, World!")
```

On the worker side, pick jobs from the queue and process them:

```python
import time
from raquel import Raquel

rq = Raquel("postgresql+psycopg2://postgres:postgres@localhost/postgres")

while True:
    with rq.dequeue() as job:
        if job is None:
            time.sleep(1)
            continue
        do_work(job.payload)
```

### Async example

Client side:

```python
import asyncio
from raquel import AsyncRaquel

rq = AsyncRaquel("postgresql+asyncpg://postgres:postgres@localhost/postgres")

async def main():
    await rq.enqueue("default", {'my': {'name_is': 'Slim Shady'}})

asyncio.run(main())
```

Worker side:

```python
import asyncio
from raquel import AsyncRaquel

rq = AsyncRaquel("postgresql+asyncpg://postgres:postgres@localhost/postgres")

async def main():
    while True:
        async with rq.dequeue() as job:
            if job is None:
                await asyncio.sleep(1)
                continue
            await do_work(job.payload)

asyncio.run(main())
```

## The `jobs` table

Raquel only needs **a single table** to work. Can you believe it?
Here is what this table consists of:

| Column | Type | Description | Default | Nullable |
|--------|------|-------------|-------------|--------|
| id | UUID | Unique identifier of the job. | | No |
| queue | TEXT | Name of the queue. | `"default"` | No |
| payload | TEXT | Payload of the job. It can by anything. Just needs to be serializable to text. | Null | Yes |
| status | TEXT | Status of the job. | `"queued"` | No |
| max_age | INTEGER | Maximum age of the job in milliseconds. | Null | Yes |
| max_retry_count | INTEGER | Maximum number of retries. | Null | Yes |
| min_retry_delay | INTEGER | Minimum delay between retries in milliseconds. | `1000` | Yes |
| max_retry_delay | INTEGER | Maximum delay between retries in milliseconds. | `12 * 3600 * 1000` | Yes |
| backoff_base | INTEGER | Base in milliseconds for exponential retry backoff. | `1000` | Yes |
| enqueued_at | BIGINT | Time when the job was enqueued in milliseconds since epoch (UTC). | `now` | No |
| scheduled_at | BIGINT | Time when the job is scheduled to run in milliseconds since epoch (UTC). | `now` | No |
| attempts | INTEGER | Number of attempts to run the job. | `0` | No |
| error | TEXT | Error message if the job failed. | Null | Yes |
| error_trace | TEXT | Error traceback if the job failed. | Null | Yes |
| claimed_by | TEXT | ID or name of the worked that claimed the job. | Null | Yes |
| claimed_at | BIGINT | Time when the job was claimed in milliseconds since epoch (UTC). | Null | Yes |
| finished_at | BIGINT | Time when the job was finished in milliseconds since epoch (UTC). | Null | Yes |

## Job status

Jobs can have the following statuses:

* `queued` - Job is waiting to be picked up by a worker.
* `claimed` - Job is currently locked and is being processed by a worker.
* `success` - Job was successfully executed.
* `failed` - Job failed to execute. This happens when an exception was
caught by the `dequeue()` context manager. The last error message and
traceback are stored in the `error` and `error_trace` columns. Job will be
retried, mening it will be rescheduled with an exponential backoff delay.
* `cancelled` - Job was manually cancelled.
* `expired` - Job was not picked up by a worker in time (when `max_age` is set).
* `exhausted` - Job has reached the maximum number of retries (when `max_retry_count` is set).

## How do we guarantee that same job is not picked up by multiple workers?

When a worker picks up a job, it first selects the job from the table and then
updates the job status to `claimed`. This is done in a single transaction, so
no other worker can pick up the same job at the same time.

In PostgreSQL, we use the `SELECT FOR UPDATE SKIP LOCKED` statement is used.
In other databases that support this (such as Oracle, MySQL) a similar approach
is used.

In extremely simple databases, such as SQLite, the fact that the whole
database is locked during a write operation guarantees that no other worker
will be able to set the job status to `claimed` at the same time.

## Job scheduling

All jobs are scheduled to run at a specific time. By default, this time is set to
the current time when the job is enqueued. You can set the `scheduled_at` field
to a future time to schedule the job to run at that time.

```python
rq.enqueue(10_000, queue="my-jobs", at=datetime.now() + timedelta(hours=10))
```

Fancy SQL? You can schedule jobs by directly inserting them into the table.
For example, in PostgreSQL:

```sql
INSERT INTO jobs 
    (id, queue, status, payload)
VALUES
    (uuid_generate_v4(), 'my-jobs', 'queued', '{"my": "payload"}'),
    (uuid_generate_v4(), 'my-jobs', 'queued', '101'),
    (uuid_generate_v4(), 'my-jobs', 'queued', 'Is this the real life?');
```

### Rescheduling a Job

The `reschedule()` method is used to reprocess the job at a later time.
The job will remain in the queue with a new scheduled execution time, and the
current attempt won't count towards the maximum number of retries.
This method should only be called inside the `dequeue()` context manager.

```python
with rq.dequeue("my-queue") as job:
    if not job:
        return

    # Check if we have everything ready to process the job, and if not,
    # reschedule the job to run 10 minutes from now
    if not is_everything_ready_to_process(job.payload):
        job.reschedule(delay=timedelta(minutes=10))

    # Otherwise, process the job
    do_work(job.payload)
```

The only things that change in the `jobs` table are the `scheduled_at` field,
the `claimed_by` field (if `claim_as` was set), and the `claimed_at` field.

Thus, you can find all rescheduled jobs by filtering for those that were
claimed, but not finished, and didn't have an error.

```sql
SELECT * FROM jobs WHERE status = 'claimed' AND finished_at IS NULL AND error IS NULL;
```

Other ways to reschedule a job using the same `reschedule()` method:

```python
# Run when the next day starts
job.reschedule(
    at=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))

# Same but using the `delay` parameter
job.reschedule(
    at=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
    delta=timedelta(days=1),
)

# Run in 500 milliseconds
job.reschedule(delay=500)

# Run in `min_retry_delay` milliseconds, as configured for this job
# (default is 1 second)
job.reschedule()
```

### Rejecting the Job

In case your worker can't process the job for some reason, you can reject it,
allowing it to be immediately claimed by another worker. This method should
only be called inside the dequeue() context manager.

The processing attempt won't count towards the maximum number of retries.
Overall it will look like the job wasn't even attempted and the corresponding
record in the `jobs` table will remain completely unchanged.

```python
with rq.dequeue("my-queue") as job:
    if job:
        return

    # Maybe this worker doesn't have the necessary permissions to process
    # this job.
    if job.payload.get("requires_admin"):
        job.reject()
        return

    # Otherwise, process the job
    do_work(job.payload)
```

## Queue names

By default, all jobs are placed into the `"default"` queue. You can specify
the queue name when enqueuing a job:

```python
# Enqueue a payload into the "default" queue
rq.enqueue(payload="Hello, World!")

# Same as above
rq.enqueue("default", "Hello, World!")

# Enqueue a payload into the "my-queue" queue
rq.enqueue("my-queue", "Hello, World!")
```

When jobs are dequeued by a worker, they are dequeued from whatever queue,
whichever job is scheduled to run first. If you want to dequeue jobs from
a specific queue, you can specify the queue name in the `dequeue()` context
manager:

```python
while True:
    with rq.dequeue("my-queue") as job:
        if job:
            do_work(job.payload)
        time.sleep(1)
```

## Job retries

Jobs are retried when they fail. When n exception is caught by the `dequeue()`
context manager, the job is rescheduled with an exponential backoff delay.

By default, the job will be retried indefinitely. You can set the `max_retry_count`
or `max_age` fields to limit the number of retries or the maximum age of the job.

```python
while True:
    with rq.dequeue() as job:
        if job:
            do_work(job.payload)
            raise Exception("Oh no")
        time.sleep(1)
```

The next retry is calculated as follows:

* Take the current `scheduled_at` time.
* Add the time it took to process the job (aka duration).
* Add the retry delay.

Now, the retry delay is calculated as:

```
backoff_base * 2 ^ attempt
```

That's the *planned* retry delay. The *actual* retry delay is capped by the
`min_retry_delay` and `max_retry_delay` fields. The `min_retry_delay` defaults
to 1 second and the `max_retry_delay` defaults to 12 hours. The `backoff_base`
defaults to 1 seconds as well.

In other words, here is how your job will be retried (assuming there is
always a worker available and the job takes almost no time to process):

* *First retry:* in 1 second
* *Second retry:* after 2 seconds following the first retry
* *Third retry:* after 4 seconds
* ...
* *7'th retry:* after ~2 minutes
* ...
* *11'th retry:* after ~30 minutes
* ...
* *15'th retry:* after about 9 hours
* ... and so on with the maximum delay of 12 hours, or whatever you set
  for this job using `max_retry_delay` setting.

In some tasks it makes sense to chill out a bit before retrying. For example,
Firebase API's might have a rate limit or it could be that some data isn't
ready yet. In such cases, you can set the `min_retry_delay` to a higher value,
such as 10 or 30 seconds.

ℹ️ Remeber, that all durations and timestampts are in milliseconds. So 10 seconds
is `10 * 1000 = 10000` milliseconds. 1 minute is `60 * 1000 = 60000` milliseconds.

## Prepare your database

You can configure the table using the ``create_all()`` method, which will
automatically use the supported syntax for the database you are using (it is
safe to run it multiple times, it only creates the table once.).

```python
# Works for all databases
rq.create_all()
```

Alternatively, you can create the table **manually**. For example, in **Postgres**:

```sql
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
```

Or, when using **Alembic**, add this to your `upgrade()` and `downgrade()` functions in
the appropriate migration file:

```python

def upgrade() -> None:
    op.create_table('jobs',
    sa.Column('id', sa.UUID(), autoincrement=False, nullable=False),
    sa.Column('queue', sa.VARCHAR(length=255), autoincrement=False, nullable=False),
    sa.Column('payload', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('status', sa.VARCHAR(length=30), autoincrement=False, nullable=False),
    sa.Column('max_age', sa.BIGINT(), autoincrement=False, nullable=True),
    sa.Column('max_retry_count', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('min_retry_delay', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('max_retry_delay', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('backoff_base', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('enqueued_at', sa.BIGINT(), autoincrement=False, nullable=False),
    sa.Column('scheduled_at', sa.BIGINT(), autoincrement=False, nullable=False),
    sa.Column('attempts', sa.INTEGER(), autoincrement=False, nullable=False),
    sa.Column('error', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('error_trace', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('claimed_by', sa.VARCHAR(length=255), autoincrement=False, nullable=True),
    sa.Column('claimed_at', sa.BIGINT(), autoincrement=False, nullable=True),
    sa.Column('finished_at', sa.BIGINT(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('id', name='jobs_pkey')
    )
    op.create_index('ix_jobs_status', 'jobs', ['status'], unique=False)
    op.create_index('ix_jobs_scheduled_at', 'jobs', ['scheduled_at'], unique=False)
    op.create_index('ix_jobs_queue', 'jobs', ['queue'], unique=False)
    op.create_index('ix_jobs_claimed_by', 'jobs', ['claimed_by'], unique=False)

def downgrade() -> None:
    op.drop_index('ix_jobs_claimed_by', table_name='jobs')
    op.drop_index('ix_jobs_queue', table_name='jobs')
    op.drop_index('ix_jobs_scheduled_at', table_name='jobs')
    op.drop_index('ix_jobs_status', table_name='jobs')
    op.drop_table('jobs')
```

## Fun facts

* Raquel is named after the famous actress Raquel Welch, who I have never seen in a movie.
  But her poster was one of the cutouts hanging in the shrine dedicated to
  Arnold Schwarzenegger in the local gym I used to attend. Don't ask me why and how.
* The name Raquel is also a combination of the words "queue" and "SQL".
* The `payload` can be empty or Null (`None`). You can use it to schedule jobs
  without any payload, for example, to send a notification or to run a periodic
  task.

## Comparison to alternatives

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
| **Job payload size limit** | No ✅ | Yes ❌ | Yes ❌ | Yes ❌ | Yes ❌ | Yes ❌ | Yes ❌ |

As you can clearly see, not only is Raquel a superior distributed task queue,
job queue, and background worker system, but very likely the best software
ever written in the history of mankind. And I claim this without any bias.

## Thinking behind Raquel

* Random UUID (`uuid4`) is used as job ID, to facilitate migration between databases and HA setups.
* Payload is stored as text, to allow any kind of data to be stored.
* Job lock is performed by first selecting the first unclaimed job through `SELECT... WHERE status = 'queued'`
  and then doing the `UPDATE... WHERE id = ?` query in the same transaction, to ensure atomicity.
  When possible (in PostgreSQL or MySQL), `SELECT FOR UPDATE SKIP LOCKED` is used for more efficiency.
* All timestamps are stored as milliseconds since epoch (UTC timezone). This removes any confusion about timezones.
