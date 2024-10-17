import json
import logging
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from typing import Any, Generator, Literal

from raquel.dialects.dbapi_types import Connection, ConnectionPool
from raquel.dialects import (
    Dialect,
    PostgresQueries,
    SQLiteQueries,
)
from raquel.models import Job, QueueStats
from .core_base import BaseRaquel


logger = logging.getLogger(__name__)


class Raquel(BaseRaquel):
    def __init__(
        self,
        connection_or_pool: Connection | ConnectionPool,
    ) -> None:
        """Examples:

            .. code-block:: python
                # Using SQLite database connection
                import sqlite3
                conn = sqlite3.connect("my.db")
                rq = Raquel(conn)

                # Using PostgreSQL database connection
                import psycopg2
                conn = psycopg2.connect(dbname="postgres", user="postgres", host="localhost")
                rq = Raquel(conn)

                # Using PostgreSQL connection pool
                import psycopg2
                pool = psycopg2.pool.ThreadedConnectionPool(dbname="postgres", user="postgres", host="localhost")
                rq = Raquel(pool)

                # Using SQLAlchemy engine
                from sqlalchemy import create_engine
                engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost/postgres")
                rq = Raquel(engine)
        """
        self.connection_or_pool = connection_or_pool
        self.is_pool = False
        self.is_sqlalchemy = False

        module = connection_or_pool.__class__.__module__.lower()
        if "sqlalchemy" in module:
            module = connection_or_pool.dialect.__class__.__name__
            self.is_sqlalchemy = True
            self.is_pool = hasattr(connection_or_pool, "connect")

        if "sqlite" in module:
            self.dialect = Dialect.SQLITE3
            self.q = SQLiteQueries
        elif "psycopg2" in module:
            self.dialect = Dialect.PSYCOPG2
            self.q = PostgresQueries
            self.is_pool |= hasattr(connection_or_pool, "getconn")
        else:
            raise ValueError("Unsupported database connection, pool, or engine")
        logger.debug(f"Detected database connection dialect: {self.dialect.value}")

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
        """Examples:

            .. code-block:: python
                # Enqueue a single job in the "default" queue for immediate processing
                raquel.enqueue({"foo": "bar"})

                # Enqueue an empty payload in the "default" queue
                raquel.enqueue()

                # Enqueue a job object
                job = Job(queue="ingest", payload="data", scheduled_at=now())
                raquel.enqueue(job)
        """
        p = self._parse_enqueue_params(
            payload,
            queue,
            at,
            delay,
            max_age,
            max_retry_count,
            max_retry_exponent,
            min_retry_delay,
            max_retry_delay,
        )

        # Insert the job
        with self._yield_connection() as conn:
            cur = conn.cursor()
            cur.execute(self.q.insert_job, p.to_insert_row())
            job_id = cur.fetchone()[0]
            cur.close()
            conn.commit()

        return Job.from_enqueue_params(job_id, p)

    @contextmanager
    def dequeue(
        self,
        queue: str = "default",
        lock_as: str | None = None,
    ) -> Generator[Job | None, Any, Any]:
        """Examples:

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
                if (
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
        p = self._parse_get_job_params(job_id)
        with self._yield_connection() as conn:
            cur = conn.cursor()
            cur.execute(self.q.select_job, (p.job_id,))
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
        p = self._parse_acquire_job_params(queue, at, lock_as)

        # Cancel expired jobs
        with self._yield_connection() as conn:
            expired_jobs_cur = conn.cursor()
            expired_jobs_cur.execute(self.q.update_jobs_cancel_expired, (p.now_ms,))
            conn.commit()

        # Acquire a job
        with self._yield_connection() as conn:
            # Retrieve the earliest scheduled job in the queue and lock the row.
            oldest_job_cur = conn.cursor()
            oldest_job_cur.execute(
                self.q.select_oldest_job,
                (p.queue, p.at_ms, p.now_ms),
            )
            job_row = oldest_job_cur.fetchone()
            if not job_row:
                logger.debug(f"No job available in queue {queue}")
                return None

            # Create the job object
            job = Job.from_row(job_row)
            oldest_job_cur.close()

            # Lock the job
            lock_job_cur = conn.cursor()
            lock_job_cur.execute(
                self.q.update_job_lock,
                (p.at_ms, p.lock_as, job.id),
            )
            lock_job_cur.close()
            conn.commit()

        job.status = self.LOCKED
        job.locked_at = datetime.fromtimestamp(p.at_ms / 1000, timezone.utc)
        return job

    def list_queues(self) -> dict[str, QueueStats]:
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
        p = self._parse_list_jobs_params(queue)
        jobs: list[Job] = []
        with self._yield_connection() as conn:
            cur = conn.cursor()
            cur.execute(self.q.select_jobs, (p.queue,))
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
        p = self._parse_count_jobs_params(queue, status)
        with self._yield_connection() as conn:
            select_query = self.q.select_jobs_count
            params = (p.queue,)
            if p.status:
                select_query = self.q.select_jobs_count_status
                params += (p.status,)

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

    @contextmanager
    def _yield_connection(self) -> Generator[Connection, Any, Any]:
        # Single connection
        if not self.is_pool:
            yield self.connection_or_pool
            return
        
        # Connection pool
        match self.dialect:
            case Dialect.PSYCOPG2:
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
            
            case _:
                if self.is_sqlalchemy:
                    connection = self.connection_or_pool.connect()
                    yield connection
                    connection.close()
                else:
                    raise ValueError("Unsupported database connection pool")
