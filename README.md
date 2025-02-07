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

# Raquel uses SQLAlchemy to connect to most SQL databases
rq = Raquel("postgresql+psycopg2://postgres:postgres@localhost/postgres")
# Enqueing a job is as simple as this
rq.enqueue(queue="messages", payload="Hello, World!")
rq.enqueue(queue="tasks", payload={"data": [1, 2]})
```

On the worker side, pick jobs from the queue and process them:

```python
from raquel import Raquel, Job

rq = Raquel("postgresql+psycopg2://postgres:postgres@localhost/postgres")

@rq.subscribe("messages", "tasks")
def worker(job: Job):
    do_work(job.payload)

# This loop will run forever, picking up jobs from the "messages" and  "tasks"
# queues every second as their scheduled time approaches.
worker.run()
```

A couple of cool things are happening here:

1. The worker will pick up jobs as they arrive.
1. A successfully processed job will have its status set to `success` after
    the processing is done.
1. Any exception raised by the `do_work()` function will be caught, stored in
   the job record, and the job will be rescheduled for a retry. The job status
    will be set to `failed`.
1. You can launch the subscribed worker in a separate thread if you want to.

Or, if you prefer, you can use a more hands-on approach, by manually dequeuing
jobs. In fact, this is exactly what the `subscribe()` method does under the
hood.

```python
while True:
    # dequeue() is a context manager that yields a job object. It will
    # handle the job status and exceptions for you.
    with rq.dequeue("tasks") as job:
        if not job:
            time.sleep(1)
            continue
        do_work(job.payload)
```

### How about async?

Everything in Raquel is designed to work with both sync and async code.
You can use the `AsyncRaquel` class to enqueue and dequeue jobs in an async
manner.

Client:

```python
import asyncio
from raquel import AsyncRaquel

rq = AsyncRaquel("postgresql+asyncpg://postgres:postgres@localhost/postgres")

async def main():
    await rq.enqueue("default", {'my': {'name_is': 'Slim Shady'}})

asyncio.run(main())
```

Worker:

```python
import asyncio
from raquel import AsyncRaquel, Job

rq = AsyncRaquel("postgresql+asyncpg://postgres:postgres@localhost/postgres")

@rq.subscribe("default")
async def worker(job: Job):
    await do_work(job.payload)

asyncio.run(worker.run())
```

## The `jobs` table

Raquel uses **a single database table** called `jobs`.
This is all it needs. Can you believe it?

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

## Enqueue a job

All jobs are scheduled to run at a specific time. By default, this time is set to
the current time when the job is enqueued. You can set the `scheduled_at` field
to a future time to schedule the job to run at that time.

```python
rq.enqueue("my-jobs", 10_000, at=datetime.now() + timedelta(hours=10))
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

## Reschedule a job

The `reschedule()` method is used to reprocess the job at a later time.
The job will remain in the queue with a new scheduled execution time, and the
current attempt won't count towards the maximum number of retries.
This method should only be called inside the `dequeue()` or `subscribe()`
context managers.

```python
for job in rq.subscribe("my-queue"):
    # Check if we have everything ready to process the job, and if not,
    # reschedule the job to run 10 minutes from now
    if not is_everything_ready_to_process(job.payload):
        job.reschedule(delay=timedelta(minutes=10))
    else:
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
    at=datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0,
    ) + timedelta(days=1)
)

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

## Reject the job

In case your worker can't process the job for some reason, you can reject it,
allowing it to be immediately claimed by another worker. This method should
only be called inside the `dequeue()` context manager or `subscribe()`
generator.

The processing attempt won't count towards the maximum number of retries.
Overall it will look like the job wasn't even attempted and the corresponding
record in the `jobs` table will remain completely unchanged.

```python
for job in rq.subscribe("my-queue"):
    # Maybe this worker doesn't have the necessary permissions to process
    # this job.
    if job.payload.get("requires_admin"):
        job.reject()
        continue

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
specific queues, you can specify the queue name in the `subscribe()` method:

```python
for job in rq.subscribe("my-queue", "another-queue", "as-many-as-you-want"):
    do_work(job.payload)
```

## Job retries

Jobs are retried when they fail. When an exception is caught by the `dequeue()`
context manager, the job is rescheduled with an exponential backoff delay.

Same works with `subscribe()` method, which is just a wrapper around `dequeue()`.

By default, the job will be retried indefinitely. You can set the `max_retry_count`
or `max_age` fields to limit the number of retries or the maximum age of the job.

```python
for job in rq.subscribe():
    # Catch an exception manually
    try:
        do_work(job.payload)
    except Exception as e:
        # If you mark the job as failed, it will be retried.
        job.fail(str(e))

    # Or, you can just let the context manager handle the exception for you.
    # The exception will be caught and the job will be retried.
    # Under the hood, context manager will call `job.fail()` for you.
    raise Exception("Oh no")
```

Whenever job fails, the error and the traceback are stored in the `error` and
`error_trace` columns. The job status is set to `failed` and the job will
be retried.

The next retry time is calculated as follows:

* Take the current `scheduled_at` time.
* Add the time it took to process the job (aka duration).
* Add the retry delay.

Now, the retry delay is calculated as:

```
backoff_base * 2 ^ attempt
```

Whic is a *planned* retry delay. The *actual* retry delay is capped between the
`min_retry_delay` and `max_retry_delay`. The `min_retry_delay` defaults
to 1 second and the `max_retry_delay` defaults to 12 hours. The `backoff_base`
defaults to 1 second.

In other words, here is how your job will be retried (assuming there is
always a worker available and the job takes almost no time to process):

| Retry   | delay |
|---------|-------------|
| 1       | 1 second after 1'st attempt |
| 2       | 2 seconds after 2'nd attempt |
| 3       | after 4 seconds |
| ...     | ... |
| 6       | after ~2 minutes |
| ...     | ... |
| 10      | after ~30 minutes |
| ...     | ... |
| 14      | after ~9 hours |
| ...     | ... |

and so on with the maximum delay of 12 hours, or based on the maximum value
you set for this job using the `max_retry_delay` setting.

For certain types of jobs, it makes sense to chill out for a bit before
retrying. For example, an API might have a rate limit you've just hit or
some data might not be ready yet. In such cases, you can set the
`min_retry_delay` to a higher value, such as 10 or 30 seconds.

ℹ️ Remeber, that all durations and timestampts are in milliseconds. So 10 seconds
is `10 * 1000 = 10000` milliseconds. 1 minute is `60 * 1000 = 60000` milliseconds.

## Prepare your database

### Create the table by calling a function

You can configure the table using the ``create_all()`` method, which will
automatically use the supported syntax for the database you are using (it is
safe to run it multiple times, it only creates the table once.).

```python
# Works for all databases
rq.create_all()
```

### Create the table using SQL

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

### Create the table using Alembic

Add this to your `env.py` file in Alembic directory:

```python
from sqlmodel import SQLModel
from raquel.models.raw_job import RawJob
from raquel.models.base_sql import BaseSQL as RaquelBaseSQL

raquel_metadata = RaquelBaseSQL.metadata
target_metadata = SQLModel.metadata
for table in raquel_metadata.tables.values():
    table.to_metadata(target_metadata)
```

Once you do that, Alembic will automatically add the Jobs table and the appropriate
indexes to your migration file.

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
