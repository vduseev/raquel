# raquel

<p>
    <a href="https://pypi.org/pypi/raquel"><img alt="Package version" src="https://img.shields.io/pypi/v/raquel?logo=python&logoColor=white&color=blue"></a>
    <a href="https://pypi.org/pypi/raquel"><img alt="Supported python versions" src="https://img.shields.io/pypi/pyversions/raquel?logo=python&logoColor=white"></a>
</p>

*Simple and elegant Job Queues for Python using SQL.*

Tired of complex job queues for distributed computing or event-based systems?
Do you want full visibility and complete reliability of your job queue?
Raquel is a perfect solution for a distributed task queue and background workers.

* **Simple**: Use **any** existing or standalone SQL database. Requires
  a **single** table!
* **Flexible**: Schedule whatever you want however you want. No frameworks,
  no restrictions.
* **Reliable**: Uses SQL transactions and handles exceptions, retries, and
  "at least once" execution. SQL guarantees persistent jobs.
* **Transparent**: Full visibility into which jobs are running, which failed
  and why, which are pending, etc. Query anything using SQL.

## Installation

```bash
pip install raquel
```

To install with async support, specify the `asyncio` extra. This simply
adds the `greenlet` package as a dependency.

```bash
pip install raquel[asyncio]
```

## Scheduling jobs

In Raquel, jobs are scheduled using the `enqueue()` method. By default, the
job is scheduled for immediate execution. But you can also schedule the job
for a specific time in the future or past using the `at` parameter.

Use the `delay` parameter to schedule the job to run after a certain amount of
time in addition to the `at` time (yes, you use either or both). The workers
won't pick up the job until the scheduled time is reached.

Payload can be any JSON-serializable object or simply a string. It can even
be empty. In database, the payload is stored as text for maximum compatibility
with all SQL databases, so anything that can be serialized to text can be used
as a payload.

If you omit the queue name, the job will be placed into the `"default"` queue.
Use the `queue` name to place jobs into different queues.

```python
from raquel import Raquel

# Raquel uses SQLAlchemy to connect to most SQL databases
rq = Raquel("postgresql+psycopg2://postgres:postgres@localhost/postgres")
# Enqueing a job is as simple as this
rq.enqueue(queue="messages", payload="Hello, World!")
rq.enqueue(queue="tasks", payload={"data": [1, 2]})
```

ℹ️ Everything in Raquel is designed to work with both sync and async code.
You can use the `AsyncRaquel` class to enqueue and dequeue jobs in an async
manner.

*Just don't forget the `asyncio` extra when installing the package:*
`raquel[asyncio]`.

```python
import asyncio
from raquel import AsyncRaquel

rq = AsyncRaquel("postgresql+asyncpg://postgres:postgres@localhost/postgres")

async def main():
    await rq.enqueue("default", {'my': {'name_is': 'Slim Shady'}})

asyncio.run(main())
```

We can also schedule jobs using plain SQL by simply inserting a row into the
`jobs` table. For example, in PostgreSQL:

```sql
-- Schedule 3 jobs in the "my-jobs" queue for immediate processing
INSERT INTO jobs 
    (id, queue, status, payload)
VALUES
    (uuid_generate_v4(), 'my-jobs', 'queued', '{"my": "payload"}'),
    (uuid_generate_v4(), 'my-jobs', 'queued', '101'),
    (uuid_generate_v4(), 'my-jobs', 'queued', 'Is this the real life?');
```

## Picking up jobs

Jobs can be picked up from the queues in two main ways.

* [The `subscribe()` decorator](#the-subscribe-decorator)
* [The `dequeue()` context manager](#the-dequeue-context-manager)

### The `subscribe()` decorator

Decorates a function to subscribe to certain queues. This is the simplest
approach, because it handles everything for you.

This approach is recommended for most use cases. It works exactly like the
`dequeue()` context manager described next, because it's essentially
a while-loop wrapper around it.

The `subscribe()` approach will run an infinite loop for you, picking up jobs
from the queues as they arrive.

#### Subscribe to queues

```python
from raquel import Raquel, Job

rq = Raquel("postgresql+psycopg2://postgres:postgres@localhost/postgres")

@rq.subscribe("messages", "tasks")
def worker(job: Job):
    do_work(job.payload)

# This loop will run forever, picking up jobs from the "messages" and "tasks"
# queues every second as their scheduled time approaches.
worker.run()
```

In async mode, the `subscribe()` decorator works the same way:

```python
import asyncio
from raquel import AsyncRaquel, Job

rq = AsyncRaquel("postgresql+asyncpg://postgres:postgres@localhost/postgres")

@rq.subscribe("messages", "tasks")
async def worker(job: Job):
    await do_work(job.payload)

asyncio.run(worker.run())
```

#### Stop the subscription

You can stop the subscription by raising the `StopSubscription` exception
from within the decorated function:

```python
import itertools
counter = itertools.count()

@rq.subscribe("tasks")
def worker(job: Job):
    do_work(job.payload)

    # Stop the subscription after the first 10 jobs are processed
    if next(counter) > 10:
        raise StopSubscription
```

### The `dequeue()` context manager

This is what does 99% of work behind the `subscribe()` decorator. Using
it directly offers a bit more flexibility but you have to make a call to the
`dequeue()` method each time you want to process a new job.

The important thing to note about the `dequeue()` is that it's a context
manager that needs to be used with a `with` or `await with` statement, just like
any a call to `open()` or any other context manager. It yields a `Job` object
for you to work with. If there is no job to process, it will yield `None`
instead.

The `dequeue()` method works by making three consecutive and independent SQL
transactions:

* **Transaction 1 (optional)**: Looks for jobs whose `max_age` is not null and
whose `scheduled_at + max_age` is in the past and updates their status to
`expired`.
* **Transaction 2**: Selects the next job from the queue and sets its status
to `claimed`, all in one go. It either succeeds in claiming the job or not.
* **Transaction 3**: Places a database lock on that "claimed" row with the
job details for the entire duration of the processing and then updates the
job with an appropriate status value:

  * `success` if the job is processed successfully and we are done with it.
  * `failed` if an exception was caught by the context manager or the job was
    manually marked as failed. The job will be rescheduled for a retry.
  * `queued` if the job was manually rejected or manually rescheduled for a
    later time.
  * `cancelled` if the job is manually cancelled.
  * `exhausted` if the job has reached the maximum number of retries.

All of that happens inside the context manager itself.

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

## The `jobs` table

Raquel uses **a single database table** called `jobs`.
This is all it needs. Can you believe it?

Here is what this table consists of:

| Column | Type | Description | Default | Nullable |
|--------|------|-------------|-------------|--------|
| id | UUID | Unique identifier of the job. | | No |
| queue | TEXT | Name of the queue. | `"default"` | No |
| payload | TEXT | Payload of the job. It can be anything. Just needs to be serializable to text. | Null | Yes |
| status | TEXT | Status of the job. | `"queued"` | No |
| max_age | INTEGER | Maximum age of the job in milliseconds. | Null | Yes |
| max_retry_count | INTEGER | Maximum number of retries. | Null | Yes |
| min_retry_delay | INTEGER | Minimum delay between retries in milliseconds. | `1000` | Yes |
| max_retry_delay | INTEGER | Maximum delay between retries in milliseconds. | `12 * 3600 * 1000` | Yes |
| backoff_base | INTEGER | Base in milliseconds for exponential retry backoff. | `1000` | Yes |
| enqueued_at | BIGINT | Time when the job was enqueued in milliseconds since epoch (UTC). | `now` | No |
| scheduled_at | BIGINT | Time when the job is scheduled to run in milliseconds since epoch (UTC). | `now` | No |
| attempts | INTEGER | Number of attempts to execute the job. | `0` | No |
| error | TEXT | Error message if the job failed. | Null | Yes |
| error_trace | TEXT | Error traceback if the job failed. | Null | Yes |
| claimed_by | TEXT | ID or name of the worker that claimed the job. | Null | Yes |
| claimed_at | BIGINT | Time when the job was claimed in milliseconds since epoch (UTC). | Null | Yes |
| finished_at | BIGINT | Time when the job was finished in milliseconds since epoch (UTC). | Null | Yes |

## Job status

![Job status](https://raw.githubusercontent.com/vduseev/raquel/master/docs/job_status.png)

Jobs can have the following statuses:

* `queued` - Job is waiting to be picked up by a worker.
* `claimed` - Job is currently locked and is being processed by a worker
  (in databases such as PostgreSQL, MySQL, etc., once the job is claimed,
  its row is locked until the worker is done with it).
* `success` - Job was successfully executed.
* `failed` - Job failed to execute. This happens when an exception was
  caught by the `dequeue()` context manager. The last error message and
  traceback are stored in the `error` and `error_trace` columns. Job will be
  retried again, meaning it will be rescheduled with an exponential
  backoff delay.
* `cancelled` - Job was manually cancelled.
* `expired` - Job was not picked up by a worker in time (when `max_age`
  is set).
* `exhausted` - Job has reached the maximum number of retries (when
  `max_retry_count` is set).

The job can be picked up by a worker in either of the following three states:
`queued`, `failed`, or `claimed`.

The first two are most common. The job will be picked up if:

1. Job status is `queued` (scheduled or rejected) or `failed`
  (failed to be processed and is being retried);
1. And its `scheduled_at` time is in the past;
1. And its `max_age` is not set or its `scheduled_at + max_age` is in the future.

The job in `claimed` state is a special case and happens when some worker
marked the job as claimed but failed to process it. In this case the row,
representing the job, is not locked in the database. If more than a minute
passed since the job was claimed (`claimed_at + 1 minute`) and the row is
not locked (meaning the worker that claimed it is dead), any worker can
reclaim it for itself.

## How do we guarantee that the same job is not picked up by multiple workers?

Short answer: by locking the row and using the `claimed` status.

In PostgreSQL, we use the `SELECT FOR UPDATE SKIP LOCKED` statement is used.
In other databases that support this (such as Oracle, MySQL) a similar approach
is used.

In extremely simple databases, such as SQLite, the fact that the whole
database is locked during a write operation guarantees that no other worker
will be able to set the job status to `claimed` at the same time.

## What happens if a worker dies?

* If a worker dies while attempting to claim a job, the transaction opened by
  the worker is rolled back and the row is unlocked by the database. Another
  worker can claim it and process it.
* If a worker dies while processing a job, the row is unlocked by the database
  but remains in the `claimed` status. Another worker can pick it up and
  process it.

## Fancy ways to schedule jobs

All jobs are scheduled to run at a specific time. By default, this time is
set to the current time when the job is enqueued. You can set the
`scheduled_at` field to a future time to schedule the job to run at that time.

```python
rq.enqueue("my-jobs", 10_000, at=datetime.now() + timedelta(hours=10))
```

But you can also specify a delay to schedule the job to run in the future:

```python
# Same as above
rq.enqueue("my-jobs", 10_000, delay=timedelta(hours=10))
```

When enqueuing a job, you can specify all the parameters that are available
in the `jobs` table. For example:

* `max_age` - Maximum age of the job in milliseconds.
* `max_retry_count` - Maximum allowed number of retries.
* `min_retry_delay` - Minimum delay between retries in milliseconds.
* `max_retry_delay` - Maximum delay between retries in milliseconds.
* `backoff_base` - Base in milliseconds for exponential retry backoff.

## Reschedule a job

The `reschedule()` method is used to reprocess the job at a later time.
The job will remain in the queue with a new scheduled execution time, and the
current attempt won't count towards the maximum number of retries.

This method should only be called inside the `dequeue()` context manager or
the `subscribe()` decorator.

```python
@rq.subscribe("my-queue")
def worker(job: Job):
    # Check if we have everything ready to process the job, and if not,
    # reschedule the job to run 10 minutes from now
    if not is_everything_ready_to_process(job.payload):
        job.reschedule(delay=timedelta(minutes=10))
    else:
        # Otherwise, process the job
        do_work(job.payload)
```

When you reschedule a job, its `scheduled_at` field is either updated with
the new `at` and `delay` values or left unchanged. And the `finished_at` field
is cleared. If the `Job` object had any `error` or `error_trace` values, they
are saved to the database. The `attempts` field is incremented.

You can find all rescheduled jobs using SQL by filtering for those that are
queued, but have attempts and were claimed before.

```sql
SELECT * FROM jobs
WHERE status = 'queued'
AND attempts > 0
AND claimed_at IS NOT NULL
```

Here are some fancy ways to reschedule a job using the same `reschedule()`
method:

ℹ️ *Make sure to call `reschedule()` inside the `dequeue()` context manager
or the `subscribe()` decorator.*

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
allowing it to be immediately claimed by another worker.

This method should only be called inside the `dequeue()` context manager or
the `subscribe()` decorator.

It is very similar to rescheduling the job to run immediately. When you reject
the job, the `scheduled_at` field is left unchanged, but the `claimed_at` and
`claimed_by` fields are cleared. The job status is set to `queued`. And the
`attempts` field is incremented.

```python
@rq.subscribe("my-queue")
def worker(job: Job):
    # Maybe this worker doesn't have the necessary permissions to process
    # this job.
    if job.payload.get("requires_admin"):
        job.reject()
        return

    # Otherwise, process the job
    do_work(job.payload)
```

Here is how you can find all rejected jobs using SQL:

```sql
SELECT * FROM jobs
WHERE status = 'queued'
AND attempts > 0
AND claimed_at IS NULL
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

By default, when jobs are dequeued by a worker, the job that has the earliest
`scheduled_at` time is picked up from any queue. If you want to dequeue jobs
from specific queues, you can specify the queue names as arguments to the
`subscribe()` decorator or the `dequeue()` context manager:

```python
# Just one job will be picked up from any of the queues.
with rq.dequeue("my-queue", "another-queue", "as-many-as-you-want"):
    do_work(job.payload)

# Continuously picks up jobs from any of the specified queues.
@rq.subscribe("my-queue", "another-queue", "as-many-as-you-want")
def worker(job: Job):
    do_work(job.payload)

worker.run()
```

Here is how you can find all jobs from a specific queue using SQL:

```sql
SELECT * FROM jobs
WHERE queue = 'my-queue'
```

And this query can show number of jobs per queue:

```sql
SELECT queue, COUNT(*) FROM jobs
GROUP BY queue
```

## Job retries

Jobs are retried when they fail. When an exception is caught by the `dequeue()`
context manager, the job is rescheduled with an exponential backoff delay.
The same logic applies within the `subscribe()` decorator.

By default, the job will be retried indefinitely. You can set the `max_retry_count`
or `max_age` fields to limit the number of retries or the maximum age of the job.

```python
@rq.subscribe()
def worker(job: Job):
    # Let the context manager handle the exception for you.
    # The exception will be caught and the job will be retried.
    # Under the hood, context manager will call `job.fail()` for you.
    raise Exception("Oh no")
    do_work(job.payload)
```

You can always handle the exception manually:

```python
@rq.subscribe()
def worker(job: Job):
    # Catch an exception manually
    try:
        do_work(job.payload)
    except Exception as e:
        # If you mark the job as failed, it will be retried.
        job.fail(str(e))
```

Whenever job fails, the error and the traceback are stored in the `error` and
`error_trace` columns. The job status is set to `failed` and the job will
be retried. The attempt number is incremented.

Here is how you can find all failed jobs using SQL:

```sql
SELECT * FROM jobs
WHERE status = 'failed'
```

## Retry delay

The next retry time after a failed job is calculated as follows:

* Take the current `scheduled_at` time.
* Add the time it took to process the job (aka duration).
* Add the retry delay.

The retry delay itself is calculated as follows:

```python
backoff_base * 2 ^ attempt
```

But this is just a *planned* retry delay. The *actual* retry delay is capped
between the `min_retry_delay` and `max_retry_delay` values. The `min_retry_delay`
defaults to 1 second and the `max_retry_delay` defaults to 12 hours. The
`backoff_base` defaults to 1 second.

In other words, here is how your job will be retried (assuming there is
always a worker available and the job takes almost no time to process):

| Retry   | delay |
|---------|-------------|
| 1       | 1 second after 1st attempt |
| 2       | 2 seconds after 2nd attempt |
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

ℹ️ Remember, that all durations and timestamps are in milliseconds. So
10 seconds is `10 * 1000 = 10000` milliseconds. 1 minute is
`60 * 1000 = 60000` milliseconds.

## Preparing your database

### Create the `jobs` table by calling a function

You can configure the table using the `create_all()` method, which will
automatically use the supported syntax for the database you are using (it is
safe to run it multiple times, it only creates the table once.).

```python
# Works for all databases
rq.create_all()
```

### Create the table using SQL

Alternatively, you can create the table **manually**. For example, in
**Postgres** you can use the following SQL:

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

### Create the table using Alembic migrations

Import Raquel's metadata and add it as a target inside the
`context.configure()` calls in the `env.py` file:

```python
# Alembic's env.py configuration file

from raquel.models.base_sql import BaseSQL as RaquelBaseSQL

# This code already exists in the Alembic configuration file
def run_migrations_offline() -> None:
    # ...
    context.configure(
        # ...
        target_metadata=[
            target_metadata,
            RaquelBaseSQL.metadata,  # <- Add Raquel's metadata
        ],
        # ...
    )

# Same for online migrations
def run_migrations_online() -> None:
    # ...
    context.configure(
        # ...
        target_metadata=[
            target_metadata,
            RaquelBaseSQL.metadata,  # <- Add Raquel's metadata
        ],
        # ...
    )
```

Once you do that, Alembic will automatically add the `jobs` table and the
appropriate indexes to your migration file.

## Fun facts

* Raquel is named after the famous actress Raquel Welch, who I have never seen in a movie.
  But her poster was one of the cutouts hanging in the shrine dedicated to
  Arnold Schwarzenegger in the local gym I used to attend. Don't ask me why and how.
* The name Raquel is also a combination of the words "queue" and "SQL".
* The `payload` can be empty or Null (`None`). You can use it to schedule jobs
  without any payload, for example, to send a notification or to run a periodic
  task.

## Should I trust Raquel with my production?

Yes, you should! Here is why:

* Raquel is dead simple. Unbelievably so.

  You can rewrite it in any programming language using plain SQL in about a
  day. We are not going to overcomplicate things here, because everyone is already
  sick of Celery and Co.

* It's incredibly reliable.

  The jobs are stored in a relational database and exclusive locks and rollbacks
  are handled through ACID transactions.

* It relies on a single dependency: SQLAlchemy.

  The most mature and well-tested database library for Python. The codebase
  of Raquel can remain same for ages, but you still get support for all the
  new databases and bugfixes. We do not pin SQLAlchemy dependency strictly.

* Raquel is already used in production by several companies.

  Examples:
  * [Dynatrace](https://www.dynatrace.com)

* Raquel is licensed under the [Apache 2.0 license](LICENSE).

  You can use it in any project or fork it and do whatever you want with it.

* Raquel is actively maintained by [Vagiz Duseev](https://github.com/vduseev).

  I'm also the author of some other popular Python packages such as
  [opensearch-logger](https://github.com/vduseev/opensearch-logger).

* Raquel is as platform and database agnostic.

  Save yourself the pain of migrating between database vendors or versions.
  All timestamps are stored as milliseconds since epoch (UTC timezone).
  Payloads are stored as text. Job IDs are random UUIDs to allow migration
  between databases and HA setups.

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
