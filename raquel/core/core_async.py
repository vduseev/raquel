import json
import logging
import time
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Any, AsyncGenerator, Literal

from raquel.dialects.dbapi_types import AsyncConnection, AsyncConnectionPool
from raquel.dialects import (
    Dialect,
    PostgresQueries,
    SQLiteQueries,
)
from raquel.models import Job, QueueStats
from .core_base import BaseRaquel


logger = logging.getLogger(__name__)


class AsyncRaquel(BaseRaquel):
    def __init__(
        self,
        connection_or_pool: AsyncConnection | AsyncConnectionPool,
    ) -> None:
        self.connection_or_pool = connection_or_pool
        self.is_pool = False
        self.is_sqlalchemy = False

        module = connection_or_pool.__class__.__module__.lower()
        if "sqlalchemy" in module:
            module = connection_or_pool.dialect.__class__.__name__.lower()
            self.is_sqlalchemy = True
            self.is_pool = hasattr(connection_or_pool, "connect")

        if "aiosqlite" in module:
            self.dialect = Dialect.SQLITE3
            self.q = SQLiteQueries
        elif "asyncpg" in module:
            self.dialect = Dialect.ASYNCPG
            self.q = PostgresQueries
            self.is_pool |= hasattr(connection_or_pool, "acquire")
        else:
            raise ValueError("Unsupported database connection, pool, or engine")
        logger.debug(f"Detected database connection dialect: {self.dialect.value}")

    async def enqueue(
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
        async with self._yield_connection() as conn:
            stmt = await conn.prepare(self.q.insert_job)
            cur = await conn.cursor()
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

    @asynccontextmanager
    async def dequeue(
        self,
        queue: str = "default",
        lock_as: str | None = None,
    ) -> AsyncGenerator[Job | None, None]:
        """Examples:

            .. code-block:: python
                while True:
                    # Get the oldest scheduled job from the "default" queue and
                    # lock it for processing with a "locked" status.
                    async with rq.dequeue("default", lock_as="worker-1") as job:
                        # If such job was found, process it
                        if job:
                            # If process_job() raises an exception, the job
                            # will be rescheduled for a retry.
                            await process_job(job)
                            # If no exception is raised, the job status
                            # will be updated to "success"
                            print("Job processed successfully")
                    asyncio.sleep(1)
        """
        p = self._parse_acquire_job_params(queue, None, lock_as)

        # Acquire the job
        job = await self.acquire_job(p.queue, p.now_ms, p.lock_as)
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
                async with self._yield_connection() as conn:
                    cur = await conn.cursor()
                    await cur.execute(
                        self.q.update_job_done,
                        (self.SUCCESS, attempt_num, None, finished_at_ms, job.id),
                    )
                    await cur.close()
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

    async def get_job(self, job_id: int) -> Job | None:
        p = self._parse_get_job_params(job_id)
        async with self._yield_connection() as conn:
            cur = await conn.cursor()
            await cur.execute(self.q.select_job, (p.job_id,))
            row = await cur.fetchone()
            if not row:
                return None
            job = Job.from_row(row)
            await cur.close()
        return job
    
    async def acquire_job(
        self,
        queue: str = "default",
        at: datetime | int | None = None,
        lock_as: str | None = None,
    ) -> Job | None:
        p = self._parse_acquire_job_params(queue, at, lock_as)

        # Cancel expired jobs
        async with self._yield_connection() as conn:
            expired_jobs_cur = await conn.cursor()
            await expired_jobs_cur.execute(self.q.update_jobs_cancel_expired, (p.now_ms,))
            await conn.commit()

        # Acquire a job
        async with self._yield_connection() as conn:
            # Retrieve the earliest scheduled job in the queue and lock the row.
            oldest_job_cur = await conn.cursor()
            await oldest_job_cur.execute(
                self.q.select_oldest_job,
                (p.queue, p.at_ms, p.now_ms),
            )
            job_row = await oldest_job_cur.fetchone()
            if not job_row:
                logger.debug(f"No job available in queue {queue}")
                return None

            # Create the job object
            job = Job.from_row(job_row)
            await oldest_job_cur.close()

            # Lock the job
            lock_job_cur = await conn.cursor()
            await lock_job_cur.execute(
                self.q.update_job_lock,
                (p.at_ms, p.lock_as, job.id),
            )
            await lock_job_cur.close()
            await conn.commit()

        job.status = self.LOCKED
        job.locked_at = datetime.fromtimestamp(p.at_ms / 1000, timezone.utc)
        return job

    async def list_queues(self) -> dict[str, QueueStats]:
        stats: dict[str, QueueStats] = {}
        async with self._yield_connection() as conn:
            cur = await conn.cursor()
            await cur.execute(self.q.select_queues)
            for row in await cur.fetchall():
                queue_stats = QueueStats.from_row(row)
                stats[queue_stats.name] = queue_stats
            await cur.close()
        return stats

    async def list_jobs(self, queue: str = "default") -> list[Job]:
        p = self._parse_list_jobs_params(queue)
        jobs: list[Job] = []
        async with self._yield_connection() as conn:
            cur = await conn.cursor()
            await cur.execute(self.q.select_jobs, (queue,))
            for row in await cur.fetchall():
                job = Job.from_row(row)
                jobs.append(job)
            await cur.close()
        return jobs
    
    async def count_jobs(
        self,
        queue: str = "default",
        status: Literal["queued", "locked", "success", "failed", "cancelled"] | None = None,
    ) -> int:
        p = self._parse_count_jobs_params(queue, status)
        async with self._yield_connection() as conn:
            select_query = self.q.select_jobs_count
            params = (p.queue,)
            if status:
                select_query = self.q.select_jobs_count_status
                params += (p.status,)

            cur = await conn.cursor()
            await cur.execute(select_query, params)
            first_row = await cur.fetchone()
            count = first_row[0]
            await cur.close()
            return count
    
    async def setup(self) -> None:
        """Create the jobs table and indexes."""
        async with self._yield_connection() as conn:
            cur = await conn.cursor()
            await cur.execute(self.q.create_jobs_table)
            await cur.execute(self.q.create_jobs_index_queue)
            await cur.execute(self.q.create_jobs_index_status)
            await cur.execute(self.q.create_jobs_index_scheduled_at)
            await cur.close()
            await conn.commit()

    async def teardown(self) -> None:
        """Drop the jobs table and indexes."""
        async with self._yield_connection() as conn:
            cur = await conn.cursor()
            await cur.execute(self.q.drop_jobs_table)
            await cur.close()
            await conn.commit()

    @asynccontextmanager
    async def _yield_connection(self) -> AsyncGenerator[AsyncConnection, None]:
        # Single connection
        if not self.is_pool:
            yield self.connection_or_pool
            return
        
        # Connection pool
        match self.dialect:
            case Dialect.PSYCOPG2:
                async with self.connection_or_pool.acquire() as connection:
                    async with connection.transaction():
                        yield connection
            
            case _:
                if self.is_sqlalchemy:
                    async with self.connection_or_pool.connect() as connection:
                        yield connection
                else:
                    raise ValueError("Unsupported database connection pool")
