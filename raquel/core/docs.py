from raquel.models import Job
from .core_sync import Raquel
from .core_async import AsyncRaquel


raquel_init_doc = """Initialize Raquel instance.

{examples}

Args:
    connection_or_pool (Connection | ConnectionPool | Engine): Python
        DPAPI Database connection, connection pool, or SQLAlchemy
        Engine.

The database dialect is automatically detected based on the
``connection.__class__.__module__`` value, if not provided. If
initialized with an SQLAlchemy engine, the dialect is detected
based on the engine's connection dialect.
"""
sync_raquel_init_examples = Raquel.__init__.__doc__
async_raquel_init_examples = AsyncRaquel.__init__.__doc__
Raquel.__init__.__doc__ = raquel_init_doc.format(examples=sync_raquel_init_examples)
AsyncRaquel.__init__.__doc__ = raquel_init_doc.format(examples=async_raquel_init_examples)


raquel_enqueue_doc = """Enqueue a job for processing.

You can pass a ``Job`` object as the payload, a string, or any
serializable object. When the payload is a ``Job`` object, its
parameters will be used to create a new job. However, the rest of
the parameters will be used to override the job parameters, if you
provide them.

If the payload is not a string, it will be serialized to text using
``json.dumps()``. For objects that cannot be serialized, such as
``bytes``, you should serialize them yourself and pass the string
as a payload directly.

{examples}

Args:
    payload (Any | Job | None): Job payload. Defaults to None.
    queue (str): Name of the queue. Defaults to "default".
    at (datetime | int | None): Scheduled time (UTC). 
        Defaults to ``now()``. The job will not be processed before
        this time. You can pass a ``datetime`` object or a unix epoch
        timestamp in milliseconds. Whatever is passed is considered
        to be in UTC.
    delay (int | timedelta | None): Delay the processing.
        Defaults to None. The delay is added to the ``at`` time.
    max_age (int | timedelta | None): Maximum time from enqueuing
        to processing. If the job is not processed within this time,
        it will not be processed at all. Defaults to None.
    max_retry_count (int | None): Maximum number of retries.
        Defaults to None.
    max_retry_exponent (int): Maximum retry delay exponent.
        Defaults to 32. The delay between retries is calculated as
        ``min_retry_delay * 2 ** min(attempt_num, max_retry_exponent)``.
    min_retry_delay (int | timedelta): Minimum retry delay.
        Defaults to 1 second.
    max_retry_delay (int | timedelta): Maximum retry delay.
        Defaults to 12 hours. Can't be less than ``min_retry_delay``.

Returns:
    Job: The created job.
"""
sync_raquel_enqueue_examples = Raquel.enqueue.__doc__
async_raquel_enqueue_examples = AsyncRaquel.enqueue.__doc__
Raquel.enqueue.__doc__ = raquel_enqueue_doc.format(examples=sync_raquel_enqueue_examples)
AsyncRaquel.enqueue.__doc__ = raquel_enqueue_doc.format(examples=async_raquel_enqueue_examples)


raquel_dequeue_doc = """Process the oldest job from the queue within a context manager.

Here is an example of how to use the ``dequeue()`` context manager
within a worker.

{examples}

Args:
    queue (str): Name of the queue. Defaults to "default".
    lock_as (str | None): Optional parameter to identify whoever
        is locking the job. Defaults to None.

Yields:
    (Job | None): The oldest job in the queue or None if no job is available.
"""
sync_raquel_dequeue_examples = Raquel.dequeue.__doc__
async_raquel_dequeue_examples = AsyncRaquel.dequeue.__doc__
Raquel.dequeue.__doc__ = raquel_dequeue_doc.format(examples=sync_raquel_dequeue_examples)
AsyncRaquel.dequeue.__doc__ = raquel_dequeue_doc.format(examples=async_raquel_dequeue_examples)


raquel_get_job_doc = """Get a job by ID.

Args:
    job_id (int): Job ID.

Returns:
    Job | None: The job or None if not found.
"""
sync_raquel_get_job_examples = Raquel.get_job.__doc__
async_raquel_get_job_examples = AsyncRaquel.get_job.__doc__
Raquel.get_job.__doc__ = raquel_get_job_doc.format(examples=sync_raquel_get_job_examples)
AsyncRaquel.get_job.__doc__ = raquel_get_job_doc.format(examples=async_raquel_get_job_examples)


raquel_acquire_job_doc = """Acquire the oldest job from the queue and lock it for processing.

Before acquiring the job, it runs an ``UPDATE`` query in a separate
transactionto cancel any jobs that have expired.

This is a low level API. Feel free to use it, but you'll have to
handle exceptions, retries, and update the job status manually. It is
recommended to use the ``dequeue()`` context manager instead. 

Args:
    queue (str): Name of the queue. Defaults to "default".
    at (datetime | int | None): Look for jobs scheduled at or before
        this time. Defaults to now (UTC).
    lock_as (str):  Optional parameter to identify whoever
        is locking the job. Defaults to None.

Returns:
    (Job | None): The acquired job or None if no job is available.
"""
sync_raquel_acquire_job_examples = Raquel.acquire_job.__doc__
async_raquel_acquire_job_examples = AsyncRaquel.acquire_job.__doc__
Raquel.acquire_job.__doc__ = raquel_acquire_job_doc.format(examples=sync_raquel_acquire_job_examples)
AsyncRaquel.acquire_job.__doc__ = raquel_acquire_job_doc.format(examples=async_raquel_acquire_job_examples)



raquel_list_queues_doc = """List all queues.

Returns:
    dict[str, QueueStats]: All queues and their statistics.
"""
sync_raquel_list_queues_examples = Raquel.list_queues.__doc__
async_raquel_list_queues_examples = AsyncRaquel.list_queues.__doc__
Raquel.list_queues.__doc__ = raquel_list_queues_doc.format(examples=sync_raquel_list_queues_examples)
AsyncRaquel.list_queues.__doc__ = raquel_list_queues_doc.format(examples=async_raquel_list_queues_examples)


raquel_list_jobs_doc = """List all jobs in the queue from latest to oldest.

Args:
    queue (str): Name of the queue. Defaults to "default".

Returns:
    list[Job]: List of jobs in the queue.
"""
sync_raquel_list_jobs_examples = Raquel.list_jobs.__doc__
async_raquel_list_jobs_examples = AsyncRaquel.list_jobs.__doc__
Raquel.list_jobs.__doc__ = raquel_list_jobs_doc.format(examples=sync_raquel_list_jobs_examples)
AsyncRaquel.list_jobs.__doc__ = raquel_list_jobs_doc.format(examples=async_raquel_list_jobs_examples)


raquel_count_jobs_doc = """Count the number of jobs in the queue with a specific status.

Args:
    queue (str): Name of the queue. Defaults to "default".
    status (str): Job status. Defaults to all statuses.
Returns:
    int: Number of jobs in the queue with the specified status.
"""
sync_raquel_count_jobs_examples = Raquel.count_jobs.__doc__
async_raquel_count_jobs_examples = AsyncRaquel.count_jobs.__doc__
Raquel.count_jobs.__doc__ = raquel_count_jobs_doc.format(examples=sync_raquel_count_jobs_examples)
AsyncRaquel.count_jobs.__doc__ = raquel_count_jobs_doc.format(examples=async_raquel_count_jobs_examples)
