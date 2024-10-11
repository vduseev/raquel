import json
import logging
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from typing import Any, Generator

from .dbapi_types import Connection
from .models import Job, QueueStats, Dialect
from .queries import common as qc, sqlite as qs, postgres as qp


logger = logging.getLogger(__name__)


class Raquel:
    def __init__(
        self,
        connection: Connection,
        dialect: Dialect | None = None,
    ) -> None:
        """Initialize the Raquel instance.

        Use in-memory SQLite database connection:
        .. code-block:: python
            import sqlite3
            from raquel import Raquel
            conn = sqlite3.connect(":memory:")
            rq = Raquel(conn)

        Use PostgreSQL database connection:
        .. code-block:: python
            import psycopg2
            from raquel import Raquel
            conn = psycopg2.connect(dbname="postgres", user="postgres", host="localhost")
            rq = Raquel(conn)

        Use SQLAlchemy connection:
        .. code-block:: python
            from sqlalchemy import create_engine
            from raquel import Raquel
            engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost/postgres")
            conn = engine.connect()
            rq = Raquel(conn.connection)

        Use async SQLAlchemy connection:
        .. code-block:: python
            from sqlalchemy.ext.asyncio import create_async_engine
            from raquel import Raquel
            engine = create_async_engine("postgresql+asyncpg://postgres:postgres@localhost/postgres")
            conn = await engine.connect()
            connection_fairy = await conn.get_raw_connection()
            rq = Raquel(connection_fairy)

        Args:
            connection (Connection): Python DPAPI Database connection.
            dialect (Dialect | None): Database dialect. Defaults to
                automatically detected dialiect or to SQLite.

        The database dialect is automatically detected based on the
        ``connection.__class__.__module__`` value.
        If detection fails, SQLite is assumed as the default dialect, which
        uses the most basic features and is compatible with all databases.
        You can also force the dialect by passing it as a parameter.
        """
        self.connection = connection

        if dialect is not None:
            self.dialect = dialect
        else:
            module = connection.__class__.__module__
            if module.startswith("sqlite"):
                self.dialect = Dialect.SQLITE
            elif module.startswith("psycopg2"):
                self.dialect = Dialect.POSTGRES
            elif module.startswith("asyncpg"):
                self.dialect = Dialect.POSTGRES
            elif module.startswith("mysql"):
                self.dialect = Dialect.MYSQL
            elif module.startswith("cx_Oracle"):
                self.dialect = Dialect.ORACLE
            else:
                self.dialect = Dialect.SQLITE
            logger.debug(f"Detected database connection dialect: {self.dialect.value}")

    def enqueue(
        self,
        queue: str,
        payload: Any | str | None = None,
        at: datetime | int | None = None,
        delay: int | timedelta | None = None,
        max_age: int | timedelta = None,
        max_retry_count: int | None = None,
        max_retry_exponent: int = 32,
        min_retry_delay: int | timedelta = timedelta(seconds=1),
        max_retry_delay: int | timedelta = timedelta(hours=12),
    ) -> Job:
        """Enqueue a job for processing.
        
        Args:
            queue (str): Name of the queue.
            payload (Any | str | None): Job payload. Will be serialized to
                text. If ``payload`` is not already a string, it will be
                serialized to text using ``json.dumps()``.
                For objects that cannot be serialized, such as ``bytes``,
                you should serialize them yourself and pass the string as a
                payload directly.
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
        with self.connection as conn:
            cur = conn.execute(
                qc.insert_job,
                (
                    queue,
                    serialized_payload,
                    "queued",
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

        return Job(
            id=job_id,
            queue=queue,
            payload=serialized_payload,
            status="queued",
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
        queue: str,
        lock_as: str | None = None,
    ) -> Generator[Job | None, Any, Any]:
        """Context manager to process the oldest job from the queue.

        .. code-block:: python
            # Get the oldest scheduled job from the "default" queue and lock
            # it for processing with a "locked" status.
            with rq.dequeue("default", lock_as="worker-1") as job:
                # If such job was found, process it
                if job:
                    # If any exception is raised during processing, the job
                    # will be rescheduled for a retry.
                    # If there is a limit to retries and too many have been
                    # attempted, the job status will be updated to "failed".
                    process_job(job)
                    # If the job is processed successfully, the job status
                    # will be updated to "success"
                    print("Job processed successfully")

        Args:
            queue (str): Name of the queue.
            lock_as (str | None): Optional parameter to identify whoever
                is locking the job. Defaults to None.

        Yields:
            (Job | None): The oldest job in the queue or None if no job is available.
        """

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
                with self.connection as conn:
                    conn.execute(
                        qc.update_job_done,
                        ("success", attempt_num, None, finished_at_ms, job.id),
                    )
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

            with self.connection as conn:
                # Make sure we can still reschedule the job and we haven't
                # violated any limits.
                if (
                    job.max_age is not None
                    and schedule_at >= job.enqueued_at + timedelta(milliseconds=job.max_age)
                ):
                    # Do not retry an expired job.
                    logger.debug(
                        f"Job {job.id} has exceeded the maximum age ({job.max_age} ms)"
                    )
                    conn.execute(
                        qc.update_job_done,
                        ("cancelled", attempt_num, "expired", finished_at_ms, job.id),
                    )

                elif (
                    job.max_retry_count is not None
                    and attempt_num + 1 > job.max_retry_count
                ):
                    # Do not retry a job that has exceeded the maximum number
                    # of retries.
                    logger.debug(
                        f"Job {job.id} has exceeded the maximum number of retries ({job.max_retry_count})"
                    )
                    conn.execute(
                        qc.update_job_done,
                        ("cancelled", attempt_num, "exhausted", finished_at_ms, job.id),
                    )

                else:
                    # Mark the job as failed and schedule the next attempt.
                    logger.debug(
                        f"Rescheduling job {job.id} for {schedule_at}"
                    )

                    stack_trace = "\n".join(traceback.format_exception(exception))
                    conn.execute(
                        qc.update_job_retry,
                        (
                            int(schedule_at.timestamp() * 1000),
                            attempt_num,
                            str(exception),
                            stack_trace,
                            finished_at_ms,
                            job.id,
                        )
                    )
                    
                # Commit the transaction
                conn.commit()

    def get_job(self, job_id: int) -> Job | None:
        """Get a job by ID.

        Args:
            job_id (int): Job ID.

        Returns:
            Job | None: The job or None if not found.
        """

        with self.connection as conn:
            cur = conn.execute(
                qc.select_job,
                (job_id,)
            )
            row = cur.fetchone()
            if not row:
                return None
            return Job.from_row(row)
    
    def acquire_job(
        self,
        queue: str,
        at: datetime | int | None = None,
        lock_as: str | None = None,
    ) -> Job | None:
        """Acquire the oldest job from the queue and lock it for processing.

        This is a low level API. Feel free to use it, but you'll have to
        handle exceptions, retries, and update the job status manually. It is
        recommended to use the ``dequeue()`` context manager instead. 

        Args:
            queue (str): Name of the queue.
            at (datetime | int | None): Look for jobs scheduled at or before
                this time. Defaults to now (UTC).
            lock_as (str):  Optional parameter to identify whoever
                is locking the job. Defaults to None.

        Returns:
            (Job | None): The acquired job or None if no job is available.
        """

        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        if at is None:
            at = now
        if isinstance(at, datetime):
            at = int(at.timestamp() * 1000)

        with self.connection as conn:
            # Retrieve the earliest scheduled job in the queue and lock the row.
            select_query = qc.select_oldest_job
            if self.dialect == Dialect.POSTGRES:
                select_query = qp.select_oldest_job
                
            oldest_job_cur = conn.execute(
                select_query,
                (queue, at, now),
            )
            job_row = oldest_job_cur.fetchone()
            if not job_row:
                logger.debug(f"No job available in queue {queue}")
                return None

            # Create the job object
            job = Job.from_row(job_row)

            # Lock the job
            conn.execute(qc.update_job_lock, (at, lock_as, job.id))
            conn.commit()

        job.status = "locked"
        job.locked_at = datetime.fromtimestamp(at / 1000, timezone.utc)
        return job

    def list_queues(self) -> dict[str, QueueStats]:
        """List all queues.

        Returns:
            dict[str, QueueStats]: All queues and their statistics.
        """

        stats: dict[str, QueueStats] = {}
        with self.connection as conn:
            cursor = conn.cursor()
            cursor.execute(qc.select_queues)
            for row in cursor.fetchall():
                queue_stats = QueueStats.from_row(row)
                stats[queue_stats.name] = queue_stats
        return stats

    def list_jobs(self, queue: str) -> list[Job]:
        """List all jobs in the queue from latest to oldest.

        Args:
            queue (str): Name of the queue.

        Returns:
            list[Job]: List of jobs in the queue.
        """

        jobs: list[Job] = []
        with self.connection as conn:
            cursor = conn.cursor()
            cursor.execute(
                qc.select_jobs,
                (queue,)
            )
            for row in cursor.fetchall():
                job = Job.from_row(row)
                jobs.append(job)
        return jobs
    
    def setup(self) -> None:
        """Create the jobs table and indexes."""
        with self.connection as conn:
            if self.dialect == Dialect.POSTGRES:
                conn.execute(qp.create_jobs_table)
            else:
                conn.execute(qs.create_jobs_table)
            conn.execute(qc.create_jobs_index_queue)
            conn.execute(qc.create_jobs_index_status)
            conn.execute(qc.create_jobs_index_scheduled_at)
            conn.commit()
