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
rq.enqueue('Hello, World!')
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
    await rq.enqueue({'my': {'name_is': 'Slim Shady'}})

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

| Column | Type | Description | Can be NULL |
|--------|------|-------------|-------------|
| id | UUID | Unique identifier of the job. | No |
| queue | TEXT | Name of the queue. By default jobs are placed into the *"default"* queue. | No |
| payload | TEXT | Payload of the job. It can by anything. Just needs to be serializable to text.| Yes |
| status | TEXT | Status of the job. | No |
| max_age | INTEGER | Maximum age of the job in milliseconds. | Yes |
| max_retry_count | INTEGER | Maximum number of retries. | Yes |
| max_retry_exponent | INTEGER | Exponential backoff factor. | No |
| min_retry_delay | INTEGER | Minimum delay between retries in milliseconds. | No |
| max_retry_delay | INTEGER | Maximum delay between retries in milliseconds. | No |
| enqueued_at | BIGINT | Time when the job was enqueued in milliseconds since epoch (UTC). | No |
| scheduled_at | BIGINT | Time when the job is scheduled to run in milliseconds since epoch (UTC). | No |
| attempts | INTEGER | Number of attempts to run the job. | No |
| error | TEXT | Error message if the job failed. | Yes |
| error_trace | TEXT | Error traceback if the job failed. | Yes |
| claimed_by | TEXT | ID or name of the worked that claimed the job. | Yes |
| claimed_at | BIGINT | Time when the job was claimed in milliseconds since epoch (UTC). | Yes |
| finished_at | BIGINT | Time when the job was finished in milliseconds since epoch (UTC). | Yes |

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

## How we guarantee that same job is not picked up by multiple workers?

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

## Queue names

By default, all jobs are placed into the `"default"` queue. You can specify
the queue name when enqueuing a job:

```python
rq.enqueue("Hello, World!", queue="my-queue")
```

When jobs are dequeued by a worker, they are dequeued from whatever queue,
whichever job is scheduled to run first. If you want to dequeue jobs from
a specific queue, you can specify the queue name in the `dequeue()` context
manager:

```python
while True:
    with rq.dequeue(queue="my-queue") as job:
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

Now, the retry delay is calculated as 2 to the power of number `attempts`
(`2 ^ attempts`). However, the power is limited by the `max_retry_exponent` field,
which defaults to 32. So, even if the number of attempts is 100, the *planned*
retry delay will not exceed 2^32 milliseconds or about 50 days.

That's the *planned* retry delay. The *actual* retry delay is capped by the
`min_retry_delay` and `max_retry_delay` fields. The `min_retry_delay` defaults
to 1 second and the `max_retry_delay` defaults to 12 hours.

In other words, here is how your job will be retried (assuming there is
always a worker available and the job takes almost no time to process):

* *First retry:* in 1 second
* *Second retry:* after 1 second 2 milliseconds
* *Third retry:* after 1 second 4 milliseconds
* ...
* *7'th retry:* after 1 second 128 milliseconds
* ...
* *10'th retry:* after 2 seconds 24 milliseconds
* *11'th retry:* after 3 seconds 48 milliseconds
* ...
* *15'th retry:* after about 33.7 seconds
* ...
* *20'th retry:* after about 17.5 minutes
* ...
* *25'th retry:* after about 9 hours 19 minutes
* ... and so on with the maximum delay of 12 hours, set by `max_retry_delay`.

In some tasks it makes sense to chill out a bit before retrying. For example,
Firebase API's have a rate limit or some kind of a Cloud Function is not
ready to reply immediately. In such cases, you can set the `min_retry_delay`
to a higher value, such as 10 or 30 seconds.

Remeber, that all durations and timestampts are in milliseconds. So 10 seconds
is `10 * 1000 = 10000` milliseconds. 1 minute is `60 * 1000 = 60000` milliseconds.

## Prepare your database

You can configure the table using the ``create_all()`` method, which will
automatically use the supported syntax for the database you are using (it is
safe to run it multiple times, it only creates the table once.).

```python
rq.create_all()
```

Alternatively, you can create the table manually. For example, in Postgres:

```sql
CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    queue TEXT NOT NULL,
    payload TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    max_age BIGINT,
    max_retry_count INTEGER,
    max_retry_exponent INTEGER NOT NULL DEFAULT 32,
    min_retry_delay INTEGER NOT NULL DEFAULT 1000,
    max_retry_delay INTEGER NOT NULL DEFAULT 43200000,
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
