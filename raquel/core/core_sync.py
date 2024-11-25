import logging
import time
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from uuid import UUID
from typing import Any, Generator, Literal

from sqlalchemy import case, create_engine, desc, select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from sqlalchemy.engine.url import URL

from raquel.models.job import Job, RawJob
from raquel.models.queue_stats import QueueStats
from raquel.models.base_sql import BaseSQL
from raquel.models.base_raquel import BaseRaquel
from raquel.core import common


logger = logging.getLogger(__name__)


class Raquel(BaseRaquel):
    """Raquel is a simple and reliable job queue for Python.

    It is recommended to use ``Raquel.dequeue`` method as a context manager,
    when processing jobs.

    Examples:

        Initialize with SQLite
        >>> rq = Raquel("sqlite:///jobs.db")

        Initialize with PostgreSQL
        >>> rq = Raquel("postgresql+psycopg2://postgres:postgres@localhost/postgres")

        Create the jobs table or make sure it exists
        >>> rq.create_all()

        Enqueue a job
        >>> rq.enqueue(payload={"foo": "bar"})

        Process jobs, one by one
        >>> while True:
        ...     with rq.dequeue() as job:
        ...         if job:
        ...             process_job(job)
        ...     time.sleep(1)

        Put the job back in the queue without processing it if you don't
        want to process it yet for some reason. For example, if the payload
        is empty.
        >>> with rq.dequeue("my-tasks) as job:
        ...     if job and not job.payload:
        ...         job.reject()

        Cancel the job before it is processed or between retries
        >>> rq.cancel(job.id)

    Args:
        url (str): Database URL.
        **kwargs: Additional keyword arguments to pass to SQLAlchemy's
            ``create_engine()`` function
    """
    def __init__(self, url: str | URL, **kwargs: Any) -> None:
        self.engine = create_engine(url, **kwargs, )

    def enqueue(
        self,
        queue: str | None = None,
        payload: Any | None = None,
        at: datetime | int | None = None,
        delay: int | timedelta | None = None,
        max_age: int | timedelta = None,
        max_retry_count: int | None = None,
        min_retry_delay: int | timedelta | None = None,
        max_retry_delay: int | timedelta | None = None,
        backoff_base: int | timedelta | None = None,
    ) -> Job:
        """Enqueue a job for processing.

        You can pass a ``Job`` object as the payload, a string, or any
        serializable object. When the payload is a ``Job`` object, its
        parameters will be used to create a new job. However, the rest of
        the parameters will be used to override the job parameters, if you
        provide them.

        If the payload is not a string, it will be serialized to text using
        ``json.dumps()``. For objects that cannot be serialized, such as
        ``bytes``, you should serialize them yourself and pass the string
        as a payload directly.

        Examples:

            Enqueue a single job in the "default" queue for immediate processing
            >>> rq.enqueue(payload={"foo": "bar"})

            Enqueue an empty payload in the "my_jobs" queue
            >>> rq.enqueue("my_jobs")

            Enqueue a job object
            >>> job = Job(queue="ingest", payload="data", scheduled_at=now())
            >>> rq.enqueue(payload=job)

        Args:
            queue (str): Name of the queue. Defaults to "default".
            payload (Any | Job | None): Job payload. Defaults to None.
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
            min_retry_delay (int | timedelta): Minimum retry delay.
                Defaults to 1 second.
            max_retry_delay (int | timedelta): Maximum retry delay.
                Defaults to 12 hours. Can't be less than ``min_retry_delay``.
            backoff_base (int | timedelta | None): Base for exponential backoff.
                Defaults to 1 second. The delay between retries is calculated as
                ``backoff_base * 2 ** retry`` in milliseconds. Then it is
                clamped between ``min_retry_delay`` and ``max_retry_delay``.

        Returns:
            Job: The created job.
        """
        p = common.parse_enqueue_params(
            queue,
            payload,
            at,
            delay,
            max_age,
            max_retry_count,
            min_retry_delay,
            max_retry_delay,
            backoff_base=backoff_base,
        )

        # Insert the job
        with Session(self.engine) as session:
            raw_job = RawJob.from_enqueue_params(p)
            session.add(raw_job)
            session.commit()

            job = Job.from_raw_job(raw_job)
            return job

    def subscribe(
        self,
        *queues,
        claim_as: str | None = None,
        expire: bool = True,
        sleep: int = 1000,
    ) -> Generator[Job, Any, Any]:
        """Subscribe to one or more queues and process jobs as they arrive.
        
        This method is a blocking generator that processes jobs as they appear.
        It is similar to the ``dequeue()`` method, but implements a continuous
        loop to process jobs as they arrive.

        Unless you explicitly close the generator using `break` or `close()`
        method, it will keep running indefinitely.

        Examples:

            Subscribe to the "default" queue
            >>> for job in rq.subscribe():
            ...     process_job(job)

            Subscribe to the "default" and "high-priority" queues
            >>> for job in rq.subscribe("default", "high-priority"):
            ...     process_job(job)

            Use ``break`` to stop iterating
            >>> for job in rq.subscribe():
            ...     process_job(job)
            ...     break  # Stop after processing the first job

        Args:
            queues (str): One or more queue names.
            claim_as (str | None): Optional parameter to identify whoever
                is locking the job. Defaults to None.
            expire (bool): Cancel expired jobs by running an
                ``UPDATE`` query in a separate transaction before claiming
                a new job. Defaults to True.
            sleep (int): Time to sleep between iterations in milliseconds.
                Defaults to 1000 ms.
        """
        while True:
            with self.dequeue(*queues, claim_as=claim_as, expire=expire) as job:
                try:
                    if job:
                        yield job
                    else:
                        time.sleep(sleep / 1000)
                except GeneratorExit:
                    return

    @contextmanager
    def dequeue(
        self,
        *queues: str,
        before: datetime | int | None = None,
        claim_as: str | None = None,
        expire: bool = True,
    ) -> Generator[Job | None, Any, Any]:
        """Process the oldest job from the queue within a context manager.

        Then it updates the job status to "claimed" and starts the processing
        of the job in a separate transaction, by first locking the job for
        processing using a ``SELECT ... FOR UPDATE`` database lock, when such
        functionality is supported by the database (PostgreSQL, Oracle, MySQL).

        Within the same transaction, it then updates the job status to "success"
        or "failed" depending on the outcome of the processing. If the job
        fails, it schedules the next attempt based on the job's retry
        parameters.

        If the job has exceeded the maximum number of retries, it changes
        the job status to "exhausted".

        Examples:

            >>> while True:
            ...     with rq.dequeue("my-tasks", claim_as="worker-1") as job:
            ...         if job:
            ...             process_job(job)
            ...             print("Job processed successfully")
            ...     time.sleep(1)

        Args:
            queues (str): One or more queue names.
            before (datetime | int | None): Look for jobs scheduled at or
                before this timestamp. Defaults to now (UTC).
            claim_as (str | None): Optional parameter to identify whoever
                is locking the job. Defaults to None.
            expire (bool): Cancel expired jobs by running an
                ``UPDATE`` query in a separate transaction before claiming
                a new job. Defaults to True.

        Yields:
            (Job | None): The oldest job in the queue or None if no job is available.
        """
        # Cancel any expired jobs before attempting to acquire a new job
        if expire:
            self.expire(*queues)

        # Acquire the job
        job = self.claim(*queues, before=before, claim_as=claim_as)
        if not job:
            yield None
            return

        with Session(self.engine) as session:
            # Lock the job with database lock
            lock_job_stmt = (
                select(RawJob)
                .where(RawJob.id == job.id)
                .with_for_update()
            )
            _ = session.execute(lock_job_stmt).scalars().first()

            # Increment the number of attempts
            attempt_num = job.attempts + 1
            exception: Exception | None = None
            try:
                # Yield the job to the caller. At this point, the job is
                # being processed by the caller code.
                yield job
                logger.debug(f"Job {job.id} processed successfully (attempt {attempt_num})")

            except Exception as pe:
                exception = pe
                logger.error(f"Failed to process job {job.id} (attempt {attempt_num}): {pe}", exc_info=pe)

            finally:
                finished_at = datetime.now(timezone.utc)
                finished_at_ms = int(finished_at.timestamp() * 1000)
                duration = (finished_at - job.claimed_at).total_seconds()
                logger.debug(f"Job {job.id} ran for {duration:.2f} seconds")

                # Job processed successfully with no exceptions
                if exception is None and not job._failed:
                    if job._rejected:
                        # Put the job back in the queue
                        stmt = self._reject_statement(job.id)
                    elif job._rescheduled:
                        # Reschedule the job for later
                        stmt = self._reschedule_statement(job, job._rescheduled_at)
                    else:
                        # Update the job status to "success"
                        stmt = self._success_statement(job.id, attempt_num, finished_at_ms)

                # Job processing failed
                else:
                    # If the exception was not manually caught by the developer,
                    # mark the job as failed.
                    if exception:
                        job.fail(exception)

                    if (
                        job.max_retry_count is not None
                        and attempt_num + 1 > job.max_retry_count
                    ):
                        # Do not retry a job that has exceeded the maximum number
                        # of retries.
                        logger.debug(f"Job {job.id} has exceeded the maximum number of retries ({job.max_retry_count})")
                        stmt = self._exhausted_statement(job.id, attempt_num, finished_at_ms)

                    else:
                        # Mark the job as failed and schedule the next attempt.
                        logger.debug(f"Rescheduling job {job.id}")
                        stmt = self._failed_statement(job, attempt_num, finished_at_ms)

                session.execute(stmt)
                session.commit()
    
    def claim(
        self,
        *queues: str,
        before: datetime | int | None = None,
        claim_as: str | None = None,
    ) -> Job | None:
        """Claim the oldest job in the queue and lock it for processing.

        This is a low level API. Feel free to use it, but you'll have to
        handle exceptions, retries, and update the job status manually. It is
        recommended to use the ``dequeue()`` context manager instead.

        Examples:
            Acquire the oldest job scheduled to be executed an hour ago.
            >>> before = datetime.now(timezone.utc) - timedelta(hours=1)
            >>> job = raquel.claim("default", before=before, claim_as="worker-1")

        Args:
            queues (str): One or more queue names.
            before (datetime | int | None): Look for jobs scheduled at or before
                this time. Defaults to now (UTC).
            claim_as (str):  Optional parameter to identify whoever
                is locking the job. Defaults to None.

        Returns:
            (Job | None): The acquired job or None if no job is available.
        """
        p = common.parse_claim_params(*queues, before=before, claim_as=claim_as)
        with Session(self.engine) as session:
            # Retrieve the earliest scheduled job in the queue and lock the row.
            where_clause = (
                RawJob.status.in_([self.QUEUED, self.FAILED]),
                RawJob.scheduled_at <= p.before_ms,
                RawJob.max_age.is_(None) | (RawJob.enqueued_at + RawJob.max_age >= p.now_ms),
            )
            if p.queues:
                where_clause = (RawJob.queue.in_(p.queues),) + where_clause

            select_oldest_stmt = (
                select(RawJob)
                .where(*where_clause)
                .order_by(RawJob.scheduled_at)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            raw_job = session.execute(select_oldest_stmt).scalars().first()
            if not raw_job:
                logger.debug(f"No job available in queue {p.queues}")
                return None
            
            # Lock the job
            update_claim_stmt = (
                update(RawJob)
                .where(RawJob.id == raw_job.id)
                .values(
                    status=self.CLAIMED,
                    claimed_at=p.before_ms,
                    claimed_by=p.claim_as,
                )
            )
            session.execute(update_claim_stmt)
            session.commit()

            job = Job.from_raw_job(raw_job)
            job.status = self.CLAIMED
            job.claimed_at = datetime.fromtimestamp(p.before_ms / 1000, timezone.utc)
            return job

    def expire(self, *queues: str) -> int:
        """Cancel all expired jobs in the queue.

        Args:
            queues (str): One or more queue names.

        Returns:
            int: Number of jobs cancelled.
        """
        for queue in queues:
            common.validate_queue_name(queue)

        with Session(self.engine) as session:
            where_clause = (
                RawJob.status.in_([self.QUEUED, self.FAILED]),
                RawJob.max_age.is_not(None),
                RawJob.enqueued_at + RawJob.max_age <= int(datetime.now(timezone.utc).timestamp() * 1000),
            )
            if queues:
                where_clause = (RawJob.queue.in_(queues),) + where_clause

            stmt = (
                update(RawJob)
                .where(*where_clause)
                .values(status=self.EXPIRED)
            )
            result = session.execute(stmt)
            session.commit()
            return result.rowcount

    def get(self, job_id: UUID) -> Job | None:
        """Get a job by ID.

        Args:
            job_id (UUID): Job ID.

        Returns:
            Job | None: The job or None if not found.
        """
        common.validate_job_id(job_id)
        with Session(self.engine) as session:
            stmt = select(RawJob).where(RawJob.id == job_id)
            raw_job = session.execute(stmt).scalars().first()
            if not raw_job:
                return None
            job = Job.from_raw_job(raw_job)
            return job
        
    def reject(self, job_id: UUID) -> bool:
        """Reverse the claim on a job.

        This will remove the claim on the job allowing it to be claimed by
        another worker.

        Only jobs that are in the "claimed" status can be rejected. For
        anything else, this method will have no effect.

        **Warning**: This method shold not be used inside the ``dequeue()``
        context manager.

        Args:
            job_id (UUID): Job ID.
        """
        common.validate_job_id(job_id)
        with Session(self.engine) as session:
            stmt = self._reject_statement(job_id)
            result = session.execute(stmt)
            session.commit()
            return result.rowcount == 1

    def cancel(self, job_id: UUID) -> bool:
        """Cancel a job before it is processed.

        Only jobs in the "queued" or "failed" status can be cancelled. For
        anything else, this method will have no effect.

        **Warning**: Calling this method inside the ``dequeue()`` context
        manager will not have any effect.

        Args:
            job_id (UUID): Job ID.
        """
        common.validate_job_id(job_id)
        with Session(self.engine) as session:
            stmt = (
                update(RawJob)
                .where(
                    RawJob.id == job_id,
                    RawJob.status.in_([self.QUEUED, self.FAILED]),
                )
                .values(status=self.CANCELLED)
            )
            result = session.execute(stmt)
            session.commit()
            return result.rowcount == 1

    def resolve(self, job_id: UUID, attempt_num: int = 1, finished_at: datetime | None = None) -> bool:
        """Mark the job as processed successfully.

        This method is used to mark a job as successfully processed. It can be
        used when you are processing jobs outside the ``dequeue()`` context
        manager.

        **Warning**: This method should not be used inside the ``dequeue()``
        context manager.

        Args:
            job_id (UUID): Job ID.
            attempt_num (int): Number of attempts it took to process this job.
                Defaults to 1.
            finished_at (datetime | None): Time when the job was finished.
                Defaults to now (UTC).
        """
        common.validate_job_id(job_id)
        with Session(self.engine) as session:
            if not finished_at:
                finished_at = datetime.now(timezone.utc)
            finished_at_ms = int(finished_at.timestamp() * 1000)
            stmt = self._success_statement(job_id, attempt_num, finished_at_ms)
            result = session.execute(stmt)
            session.commit()
            return result.rowcount == 1
        
    def fail(
        self,
        job: Job,
        attempt_num: int = 1,
        exception: str | Exception | None = None,
        finished_at: datetime | None = None,
    ) -> bool:
        """Mark the job as failed and reschedule it for another attempt.

        This method is used to mark a job as failed. It can be used when you
        are processing jobs outside the ``dequeue()`` context manager.

        **Warning**: This method should not be used inside the ``dequeue()``
        context manager. Use ``job.fail()`` method instead.

        Args:
            job (Job): The job that failed.
            attempt_num (int): Number of attempts it took to process this job.
                Defaults to 1.
            exception (str | Exception | None): Error or exception.
            finished_at (datetime | None): Time when the job was finished.
                Defaults to now (UTC).
        """
        common.validate_job_id(job.id)
        job.fail(exception)

        with Session(self.engine) as session:
            if not finished_at:
                finished_at = datetime.now(timezone.utc)
            finished_at_ms = int(finished_at.timestamp() * 1000)
            stmt = self._failed_statement(job, attempt_num, finished_at_ms)
            result = session.execute(stmt)
            session.commit()
            return result.rowcount == 1

    def queues(self) -> list[str]:
        """List all queues.

        Returns:
            list[str]: List of all queues.
        """
        with Session(self.engine) as session:
            queues: list[str] = []
            stmt = select(RawJob.queue).group_by(RawJob.queue)
            result = session.execute(stmt)
            for row in result:
                queues.append(row[0])
            return queues

    def stats(self, queue: str | None = None) -> dict[str, QueueStats]:
        """Compute stats for queues.

        Args:
            queue (str | None): Name of the queue. Defaults to all queues.

        Returns:
            dict[str, QueueStats]: All queues and their statistics.
        """
        with Session(self.engine) as session:
            stats: dict[str, QueueStats] = {}
            stmt = select(
                RawJob.queue,
                func.count(1),
                func.sum(case((RawJob.status == self.QUEUED, 1), else_=0)),
                func.sum(case((RawJob.status == self.CLAIMED, 1), else_=0)),
                func.sum(case((RawJob.status == self.SUCCESS, 1), else_=0)),
                func.sum(case((RawJob.status == self.FAILED, 1), else_=0)),
                func.sum(case((RawJob.status == self.EXPIRED, 1), else_=0)),
                func.sum(case((RawJob.status == self.EXHAUSTED, 1), else_=0)),
                func.sum(case((RawJob.status == self.CANCELLED, 1), else_=0)),
            )

            if queue:
                stmt = stmt.where(RawJob.queue == queue)
            stmt = stmt.group_by(RawJob.queue)

            result = session.execute(stmt)
            for row in result:
                queue_stats = QueueStats.from_row(row)
                stats[queue_stats.name] = queue_stats
            return stats

    def jobs(self, queue: str | None = None) -> list[Job]:
        """List all jobs in the queue from latest to oldest.

        Args:
            queue (str | None): Name of the queue. Defaults to all queues.

        Returns:
            list[Job]: List of jobs in the queue.
        """
        common.validate_queue_name(queue)
        with Session(self.engine) as session:
            results: list[Job] = []
            stmt = select(RawJob)
            if queue:
                stmt = stmt.where(RawJob.queue == queue)
            stmt = stmt.order_by(desc(RawJob.scheduled_at))
            for raw_job in session.scalars(stmt):
                job = Job.from_raw_job(raw_job)
                results.append(job)
            return results
    
    def count(
        self,
        queue: str | None = None,
        status: Literal["queued", "claimed", "success", "failed", "expired", "exhausted", "cancelled"] | None = None,
    ) -> int:
        """Count the number of jobs in the queue with a specific status.

        Args:
            queue (str | None): Name of the queue. Defaults to all queues.
            status (str | None): Job status. Defaults to all statuses.
        Returns:
            int: Number of jobs in the queue with the specified status.
        """
        common.validate_queue_name(queue)
        common.validate_status(status)
        with Session(self.engine) as session:
            stmt = select(func.count(RawJob.id))
            if queue:
                stmt = stmt.where(RawJob.queue == queue)
            if status:
                stmt = stmt.where(RawJob.status == status)
            result = session.execute(stmt).scalar()
            return result

    def create_all(self) -> None:
        """Create the jobs table and indexes.
        
        Only creates the objects if they do not exist.
        """
        BaseSQL.metadata.create_all(self.engine, checkfirst=True)

    def drop_all(self) -> None:
        """Drop the jobs table and indexes.
        
        Only drops the objects if they exist.
        """
        BaseSQL.metadata.drop_all(self.engine, checkfirst=True)
