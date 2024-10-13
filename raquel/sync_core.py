import json
import logging
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from typing import Any, Generator, Literal

from .dbapi_types import Connection, ConnectionPool
from .models import Job, QueueStats
from .queries import (
    Dialect,
    PostgresQueries,
    SQLiteQueries,
)


logger = logging.getLogger(__name__)


class Raquel:
    """Raquel is a simple and reliable job queue for Python.

    It is recommended to use ``Raquel`` with a connection pool (SQLAlchemy
    engine is a connection pool too). Another recommendation is to use
    ``Raquel.dequeue`` method as a context manager, when dequeuing jobs.
    """

    QUEUED = "queued"
    LOCKED = "locked"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    EXHAUSTED = "exhausted"

    def __init__(
        self,
        connection_or_pool: Connection | ConnectionPool,
        dialect: Dialect | None = None,
    ) -> None:
        """Initialize the Raquel instance.

        Examples:
            Using SQLite database connection:
            .. code-block:: python
                import sqlite3
                conn = sqlite3.connect("my.db")
                rq = Raquel(conn)

            Using PostgreSQL database connection:
            .. code-block:: python
                import psycopg2
                conn = psycopg2.connect(dbname="postgres", user="postgres", host="localhost")
                rq = Raquel(conn)

            Using PostgreSQL connection pool:
            .. code-block:: python
                import psycopg2
                pool = psycopg2.pool.ThreadedConnectionPool(dbname="postgres", user="postgres", host="localhost")
                rq = Raquel(pool)

            Using SQLAlchemy engine:
            .. code-block:: python
                from sqlalchemy import create_engine
                engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost/postgres")
                rq = Raquel(engine)

        Args:
            connection_or_pool (Connection | ConnectionPool | Engine): Python
                DPAPI Database connection, connection pool, or SQLAlchemy
                Engine. 
            dialect (Dialect | None): Database dialect. Defaults to
                automatically detected dialiect.

        The database dialect is automatically detected based on the
        ``connection.__class__.__module__`` value, if not provided. If
        initialized with an SQLAlchemy engine, the dialect is detected
        based on the engine's connection dialect.
        """

        self.connection_or_pool = connection_or_pool
        # Check if the connection is an SQLAlchemy connection pool
        self.is_pool = hasattr(connection_or_pool, "connect")

        if dialect is not None:  # pragma: no cover
            self.dialect = dialect
        else:  # pragma: no cover
            module = connection_or_pool.__class__.__module__.lower()
            if "sqlalchemy" in module:
                module = connection_or_pool.dialect.__class__.__name__

            if "sqlite" in module:
                self.dialect = Dialect.SQLITE
                self.q = SQLiteQueries
            elif "mysql" in module:
                self.dialect = Dialect.MYSQL
                self.q = PostgresQueries
            elif "oracle" in module:
                self.dialect = Dialect.ORACLE
            elif "psycopg2" in module or "postgresql" in module:
                self.dialect = Dialect.POSTGRES
                self.q = PostgresQueries
            elif "mssql" in module:
                self.dialect = Dialect.MSSQL
            else:
                raise ValueError("Unsupported database connection, pool, or engine")
            logger.debug(f"Detected database connection dialect: {self.dialect.value}")

        if (
            hasattr(connection_or_pool, "getconn")
            or hasattr(connection_or_pool, "get_connection")
            or hasattr(connection_or_pool, "connect")
        ):
            self.is_pool = True

    @contextmanager
    def _yield_connection(self) -> Generator[Connection, Any, Any]:
        # Single connection
        if not self.is_pool:
            yield self.connection_or_pool
            return
        
        # Connection pool
        match self.dialect:
            case Dialect.POSTGRES:
                connection: Connection = None
                connection_attempts = 0
                while connection_attempts < 3:
                    try:
                        connection_attempts += 1
                        connection = self.connection_or_pool.getconn()
                        break
                    except Exception as e:
                        logger.error(f"Failed to get a connection from the pool: {e}", exc_info=e)
                        time.sleep(1)
                if connection is None:
                    raise RuntimeError(f"Failed to get a connection from the pool after {connection_attempts} attempts")
                yield connection
                self.connection_or_pool.putconn(connection)
                    
            case Dialect.MYSQL:
                connection = self.connection_or_pool.get_connection()
                yield connection
                connection.close()  # Return the connection to the pool
            
            case _:  # SQLAlchemy
                connection = self.connection_or_pool.connect()
                yield connection
                connection.close()  # Return the connection to the pool

    def enqueue(
        self,
        payload: Any | list[Any] | None = None,
        queue: str | None = None,
        at: datetime | int | None = None,
        delay: int | timedelta | None = None,
        max_age: int | timedelta = None,
        max_retry_count: int | None = None,
        max_retry_exponent: int | None = None,
        min_retry_delay: int | timedelta | None = None,
        max_retry_delay: int | timedelta | None = None,
    ) -> Job:
        """Enqueue a job for processing.

        You can pass a ``Job`` object as the payload, a string, or any
        serializable object. When the payload is a ``Job`` object, its
        parameters will be used to create a new job. However, the rest of
        the parameters will be used to override the job parameters, if you
        provide them.
        
        Payload can also be a ``list`` of any of these.
        When you pass a list as a payload, all of them will be enqueued
        as separate jobs in a single batch.
        
        If the payload is not a string, it will be serialized to text using
        ``json.dumps()``. For objects that cannot be serialized, such as
        ``bytes``, you should serialize them yourself and pass the string
        as a payload directly.

        Examples:

        .. code-block:: python
            # Enqueue a single job in the "default" queue for immediate processing
            raquel.enqueue({"foo": "bar"})

            # Enqueue multiple jobs in a batch
            raquel.enqueue(["a", 2, {"c": 3}], queue="jobs")

            # Enqueue an empty payload in the "default" queue
            raquel.enqueue()

            # Enqueue a job object
            job = Job(queue="ingest", payload="data", scheduled_at=now())
            raquel.enqueue(job)
        
        Args:
            payload (Any | list[Any] | None): Job payload. Defaults to None.
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
            (Job | None): The created job for a single payload or None if
                multiple payloads were scheduled in a batch.
        """
        provided_payload = payload

        if isinstance(provided_payload, Job):
            payload = provided_payload.payload
            queue = queue or provided_payload.queue
            at = at or provided_payload.scheduled_at
            max_age = max_age or provided_payload.max_age
            max_retry_count = max_retry_count or provided_payload.max_retry_count
            max_retry_exponent = max_retry_exponent or provided_payload.max_retry_exponent
            min_retry_delay = min_retry_delay or provided_payload.min_retry_delay
            max_retry_delay = max_retry_delay or provided_payload.max_retry_delay

        queue = queue or "default"
        if not isinstance(queue, str):
            raise ValueError("Queue name must be a string")

        max_retry_exponent = max_retry_exponent or 32
        if not isinstance(max_retry_exponent, int):
            raise ValueError("max_retry_exponent must be an integer")
        
        if max_retry_count and not isinstance(max_retry_count, int):
            raise ValueError("max_retry_count must be an integer")

        min_retry_delay = min_retry_delay or 1000
        max_retry_delay = max_retry_delay or 12 * 3600 * 1000

        # Determine the scheduled_at time
        now = datetime.now(timezone.utc)
        scheduled_at = at or now
        if isinstance(at, int):
            scheduled_at = datetime.fromtimestamp(at / 1000, timezone.utc)

        if delay:
            if isinstance(delay, int):
                delay = timedelta(milliseconds=delay)
            scheduled_at += delay

        # Serialize the payload
        serialized_payload = None
        if isinstance(payload, str):
            serialized_payload = payload
        elif payload and not isinstance(payload, str):
            serialized_payload = json.dumps(payload)

        # Convert config
        if isinstance(max_age, timedelta):
            max_age = int(max_age.total_seconds() * 1000)
        if isinstance(min_retry_delay, timedelta):
            min_retry_delay = int(min_retry_delay.total_seconds() * 1000)
        if isinstance(max_retry_delay, timedelta):
            max_retry_delay = int(max_retry_delay.total_seconds() * 1000)

        if max_retry_delay < min_retry_delay:
            raise ValueError("max_retry_delay cannot be less than min_retry_delay")

        # Convert timestatmps to milliseconds since epoch
        enqueued_at_ms = int(now.timestamp() * 1000)
        scheduled_at_ms = int(scheduled_at.timestamp() * 1000)

        # Insert the job
        with self._yield_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                self.q.insert_job,
                (
                    queue,
                    serialized_payload,
                    self.QUEUED,
                    max_age,
                    max_retry_count,
                    max_retry_exponent,
                    min_retry_delay,
                    max_retry_delay,
                    enqueued_at_ms,
                    scheduled_at_ms,
                )
            )
            job_id = cur.fetchone()[0]
            cur.close()
            conn.commit()

        return Job(
            id=job_id,
            queue=queue,
            payload=payload,
            status=self.QUEUED,
            max_age=max_age,
            max_retry_count=max_retry_count,
            max_retry_exponent=max_retry_exponent,
            min_retry_delay=min_retry_delay,
            max_retry_delay=max_retry_delay,
            enqueued_at=enqueued_at_ms,
            scheduled_at=scheduled_at_ms,
        )

    @contextmanager
    def dequeue(
        self,
        queue: str = "default",
        lock_as: str | None = None,
    ) -> Generator[Job | None, Any, Any]:
        """Process the oldest job from the queue within a context manager.

        Here is an example of how to use the ``dequeue()`` context manager
        within a worker.

        .. code-block:: python
            while True:
                # Get the oldest scheduled job from the "default" queue and
                # lock it for processing with a "locked" status.
                with rq.dequeue("default", lock_as="worker-1") as job:
                    # If such job was found, process it
                    if job:
                        # If process_job() raises an exception, the job
                        # will be rescheduled for a retry.
                        process_job(job)
                        # If no exception is raised, the job status
                        # will be updated to "success"
                        print("Job processed successfully")
                time.sleep(1)

        Args:
            queue (str): Name of the queue. Defaults to "default".
            lock_as (str | None): Optional parameter to identify whoever
                is locking the job. Defaults to None.

        Yields:
            (Job | None): The oldest job in the queue or None if no job is available.
        """

        if not isinstance(queue, str):
            raise ValueError("Queue name must be a string")
        
        if not isinstance(lock_as, str) and lock_as is not None:
            raise ValueError("lock_as must be a string or None")

        now = datetime.now(timezone.utc)

        # Acquire the job
        job = self.acquire_job(queue, now, lock_as)
        if not job:
            yield None
            return

        # Increment the number of attempts
        attempt_num = job.attempts + 1
        exception: BaseException | None = None
        try:
            # Yield the job to the caller. At this point, the job is
            # being processed by the caller code.
            yield job
            logger.debug(f"Job {job.id} processed successfully (attempt {attempt_num})")

        except BaseException as be:
            exception = be
            logger.error(f"Failed to process job {job.id} (attempt {attempt_num}): {be}", exc_info=be)

        finally:
            finish = datetime.now(timezone.utc)
            finished_at_ms = int(finish.timestamp() * 1000)
            duration = (finish - job.locked_at).total_seconds()
            logger.debug(f"Job {job.id} ran for {duration:.2f} seconds")

            # Job processed successfully with no exceptions
            if exception is None:
                # Update the job status to "success"
                with self._yield_connection() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        self.q.update_job_done,
                        (self.SUCCESS, attempt_num, None, finished_at_ms, job.id),
                    )
                    cur.close()
                    conn.commit()
                return

            # Calculate when to schedule the next attempt
            exponent = min(attempt_num, job.max_retry_exponent)
            planned_delay = 2 ** exponent
            actual_delay = max(job.min_retry_delay, planned_delay)
            schedule_at = job.scheduled_at + timedelta(
                seconds=duration,
            ) + timedelta(
                milliseconds=min(actual_delay, job.max_retry_delay),
            )

            with self._yield_connection() as conn:
                # Make sure we can still reschedule the job and we haven't
                # violated any limits.
                if (
                    job.max_age is not None
                    and schedule_at >= job.enqueued_at + timedelta(milliseconds=job.max_age)
                ):
                    # Do not retry an expired job.
                    logger.debug(f"Job {job.id} has exceeded the maximum age ({job.max_age} ms)")
                    cur = conn.cursor()
                    cur.execute(
                        self.q.update_job_done
                        (self.CANCELLED, attempt_num, self.EXPIRED, finished_at_ms, job.id),
                    )
                    cur.close()

                elif (
                    job.max_retry_count is not None
                    and attempt_num + 1 > job.max_retry_count
                ):
                    # Do not retry a job that has exceeded the maximum number
                    # of retries.
                    logger.debug(f"Job {job.id} has exceeded the maximum number of retries ({job.max_retry_count})")
                    cur = conn.cursor()
                    cur.execute(
                        self.q.update_job_done,
                        (self.CANCELLED, attempt_num, self.EXHAUSTED, finished_at_ms, job.id),
                    )
                    cur.close()

                else:
                    # Mark the job as failed and schedule the next attempt.
                    logger.debug(f"Rescheduling job {job.id} for {schedule_at}")
                    stack_trace = "\n".join(traceback.format_exception(exception))
                    cur = conn.cursor()
                    cur.execute(
                        self.q.update_job_retry,
                        (
                            int(schedule_at.timestamp() * 1000),
                            attempt_num,
                            str(exception),
                            stack_trace,
                            finished_at_ms,
                            job.id,
                        )
                    )
                    cur.close()
                    
                # Commit the transaction
                conn.commit()

    def get_job(self, job_id: int) -> Job | None:
        """Get a job by ID.

        Args:
            job_id (int): Job ID.

        Returns:
            Job | None: The job or None if not found.
        """

        if not isinstance(job_id, int):
            raise ValueError("Job ID must be an integer")

        with self._yield_connection() as conn:
            cur = conn.cursor()
            cur.execute(self.q.select_job, (job_id,))
            row = cur.fetchone()
            if not row:
                return None
            job = Job.from_row(row)
            cur.close()
        return job
    
    def acquire_job(
        self,
        queue: str = "default",
        at: datetime | int | None = None,
        lock_as: str | None = None,
    ) -> Job | None:
        """Acquire the oldest job from the queue and lock it for processing.

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

        if not isinstance(queue, str):
            raise ValueError("Queue name must be a string")
        
        if not isinstance(lock_as, str) and lock_as is not None:
            raise ValueError("lock_as must be a string or None")

        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        if at is None:
            at = now
        if isinstance(at, datetime):
            at = int(at.timestamp() * 1000)

        with self._yield_connection() as conn:
            # Retrieve the earliest scheduled job in the queue and lock the row.
            oldest_job_cur = conn.cursor()
            oldest_job_cur.execute(self.q.select_oldest_job, (queue, at, now))
            job_row = oldest_job_cur.fetchone()
            if not job_row:
                logger.debug(f"No job available in queue {queue}")
                return None

            # Create the job object
            job = Job.from_row(job_row)
            oldest_job_cur.close()

            # Lock the job
            lock_job_cur = conn.cursor()
            lock_job_cur.execute(self.q.update_job_lock, (at, lock_as, job.id))
            lock_job_cur.close()
            conn.commit()

        job.status = self.LOCKED
        job.locked_at = datetime.fromtimestamp(at / 1000, timezone.utc)
        return job

    def list_queues(self) -> dict[str, QueueStats]:
        """List all queues.

        Returns:
            dict[str, QueueStats]: All queues and their statistics.
        """

        stats: dict[str, QueueStats] = {}
        with self._yield_connection() as conn:
            cur = conn.cursor()
            cur.execute(self.q.select_queues)
            for row in cur.fetchall():
                queue_stats = QueueStats.from_row(row)
                stats[queue_stats.name] = queue_stats
            cur.close()
        return stats

    def list_jobs(self, queue: str = "default") -> list[Job]:
        """List all jobs in the queue from latest to oldest.

        Args:
            queue (str): Name of the queue. Defaults to "default".

        Returns:
            list[Job]: List of jobs in the queue.
        """

        if not isinstance(queue, str):
            raise ValueError("Queue name must be a string")

        jobs: list[Job] = []
        with self._yield_connection() as conn:
            cur = conn.cursor()
            cur.execute(self.q.select_jobs, (queue,))
            for row in cur.fetchall():
                job = Job.from_row(row)
                jobs.append(job)
            cur.close()
        return jobs
    
    def count_jobs(
        self,
        queue: str = "default",
        status: Literal["queued", "locked", "success", "failed", "cancelled"] | None = None,
    ) -> int:
        """Count the number of jobs in the queue with a specific status.

        Args:
            queue (str): Name of the queue. Defaults to "default".
            status (str): Job status. Defaults to all statuses.
        Returns:
            int: Number of jobs in the queue with the specified status.
        """

        if not isinstance(queue, str):
            raise ValueError("Queue name must be a string")
        
        if status and not isinstance(status, str):
            raise ValueError("Status must be a string")

        with self._yield_connection() as conn:
            select_query = self.q.select_jobs_count
            params = (queue,)
            if status:
                select_query = self.q.select_jobs_count_status
                params += (status,)

            cur = conn.cursor()
            cur.execute(select_query, params)
            first_row = cur.fetchone()
            count = first_row[0]
            cur.close()
            return count
    
    def setup(self) -> None:
        """Create the jobs table and indexes."""
        with self._yield_connection() as conn:
            cur = conn.cursor()
            cur.execute(self.q.create_jobs_table)
            cur.execute(self.q.create_jobs_index_queue)
            cur.execute(self.q.create_jobs_index_status)
            cur.execute(self.q.create_jobs_index_scheduled_at)
            cur.close()
            conn.commit()

    def teardown(self) -> None:
        """Drop the jobs table and indexes."""
        with self._yield_connection() as conn:
            cur = conn.cursor()
            cur.execute(self.q.drop_jobs_table)
            cur.close()
            conn.commit()
