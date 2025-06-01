import asyncio
import logging
from datetime import datetime, timezone, timedelta
from uuid import UUID
from typing import Any, Callable, Awaitable, Iterable

from sqlalchemy import (
    case,
    desc,
    select,
    update,
    and_,
    or_,
    Update,
)
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
)
from sqlalchemy.sql import func
from sqlalchemy.engine.url import URL

from raquel.models.base_job import JobStatusValueType
from raquel.models.job import Job, RawJob
from raquel.models.queue_stats import QueueStats
from raquel.models.base_sql import BaseSQL
from raquel.core.base import BaseRaquel, StopSubscription, DecoratedCallable
from raquel.core import common


logger = logging.getLogger(__name__)


class AsyncSubscription:
    def __init__(
        self,
        fn: DecoratedCallable,
        broker: "AsyncRaquel",
        queues: tuple[str],
        claim_as: str | None = None,
        expire: bool = True,
        sleep: int = 1000,
        raise_stop_on_unhandled_exc: bool = False,
        reclaim_after: int = 60 * 1000,
    ) -> None:
        self.fn = fn
        self.broker = broker
        self.queues = queues
        self.claim_as = claim_as
        self.expire = expire
        self.sleep = sleep
        self.raise_stop_on_unhandled_exc = raise_stop_on_unhandled_exc
        self.reclaim_after = reclaim_after
        self.stop_event = asyncio.Event()        

    async def run(self) -> None:
        """Launch the subscription.

        Runs the subscribed function in a loop, processing jobs as they arrive.
        """
        logger.info(
            f"Starting {self.fn.__module__}.{self.fn.__qualname__} subscription to queues {self.queues}"
        )
        while True:
            try:
                if self.stop_event.is_set():
                    raise StopSubscription

                async with self.broker.dequeue(
                    *self.queues,
                    claim_as=self.claim_as,
                    expire=self.expire,
                    raise_stop_on_unhandled_exc=self.raise_stop_on_unhandled_exc,
                    reclaim_after=self.reclaim_after,
                ) as job:
                    if job:
                        await self.fn(job)
                    else:
                        await asyncio.sleep(self.sleep / 1000)

            except StopSubscription:
                logger.debug(
                    f"Subscription interrupted by StopSubscription signal"
                )
                return
            except KeyboardInterrupt:
                logger.info(
                    f"Subscription interrupted by KeyboardInterrupt signal"
                )
                return
            except asyncio.CancelledError:
                logger.info(
                    f"Subscription interrupted by CancelledError signal"
                )
                return

    def stop(self) -> None:
        """Request the subscriber to stop.

        The subscriber will stop after processing the current job or after
        the current wait period is over.
        """
        self.stop_event.set()


class AsyncDequeueContextManager:
    def __init__(
        self,
        broker: "AsyncRaquel",
        queues: tuple[str],
        before: datetime | int | None = None,
        claim_as: str | None = None,
        expire: bool = True,
        raise_stop_on_unhandled_exc: bool = False,
        reclaim_after: int = 60 * 1000,
    ) -> None:
        self.broker = broker
        self.queues = queues
        self.before = before
        self.claim_as = claim_as
        self.expire = expire
        self.raise_stop_on_unhandled_exc = raise_stop_on_unhandled_exc
        self.reclaim_after = reclaim_after

        self.job: Job | None = None
        self.session: AsyncSession | None = None
        self.exception: Exception | None = None
        self.attempt_num: int = 0

    async def __aenter__(self) -> Job | None:
        # Cancel any expired jobs before attempting to acquire a new job
        if self.expire:
            await self.broker.expire(*self.queues)

        # Acquire the job
        self.job = await self.broker.claim(
            *self.queues,
            before=self.before,
            claim_as=self.claim_as,
            reclaim_after=self.reclaim_after,
        )

        if not self.job:
            return None

        self.session = self.broker.async_session_factory()

        # Lock the job with database lock
        lock_job_stmt = (
            select(RawJob).where(RawJob.id == self.job.id).with_for_update()
        )
        await self.session.execute(lock_job_stmt)

        # Increment the number of attempts
        self.attempt_num = self.job.attempts + 1

        # Yield the job to the caller. At this point, the job is
        # being processed by the caller code.
        return self.job

    async def __aexit__(
        self, exc_type: Any, exc_value: Any, traceback: Any
    ) -> None:
        """Handle exceptions and update the job status.

        This method is called after the context manager exits. No matter what
        has happened. There are several possible scenarios:

        A. There was no job to process.

            The session was not created, so there is nothing to do. Just
            return True to indicate that there was no exception.

        B. SIGINT was received while processing the job.

            It doesn't matter at which stage of the processing we were.

            B-1) If there was no job, fine. We exit anyway but we return
            False to propagate the interruption signal up the call stack.

            B-2) If we were in the middle of processing the job, then in
            addition to rolling back the current transaction we also have to
            unclaim the job.

        C. The StopSubscription signal was raised while processing the job.

            This is a healthy way to stop the subscription. We treat it as
            a successful execution and finish processing and updating the job
            properly. We then return False to propagate the signal up to
            the `subscribe()` decorator.

        D. An unhandled exception occurred while processing the job.

            We mark the job as failed and reschedule it for another attempt.

            If raise_stop_on_unhandled_exc is True, we also raise a
            StopSubscription signal to stop the subscription.

        E. The job was processed successfully.

            We mark the job as successful and return True to indicate that
            there was no exception.
        """

        if exc_type in (KeyboardInterrupt, asyncio.CancelledError):
            if self.session:
                await self.session.rollback()
                await self.session.close()
            if self.job:
                await self.broker.unclaim(self.job.id)
            return False

        if not self.job:
            return True

        finished_at = datetime.now(timezone.utc)
        finished_at_ms = int(finished_at.timestamp() * 1000)
        duration = (finished_at - self.job.claimed_at).total_seconds()
        logger.debug(f"Job {self.job.id} ran for {duration:.2f} seconds")

        # If the exception was not manually caught by the developer,
        # mark the job as failed.
        if exc_type and exc_type is not StopSubscription:
            self.exception = exc_value
            logger.error(
                f"Failed to process job {self.job.id} (attempt {self.attempt_num}): {self.exception}",
                exc_info=self.exception,
            )

            self.job.fail(self.exception)

        # Statement to execute after processing the job
        stmt: Update | None = None

        if self.job._failed:
            # Do not retry a job that has exceeded the maximum number
            # of retries.
            if (
                self.job.max_retry_count is not None
                and self.attempt_num + 1 > self.job.max_retry_count
            ):
                logger.debug(
                    f"Job {self.job.id} has exceeded the maximum number of retries ({self.job.max_retry_count})"
                )
                stmt = self.broker._exhausted_statement(
                    self.job.id, self.attempt_num, finished_at_ms
                )

            # Mark the job as failed and schedule the next attempt.
            else:
                logger.debug(f"Rescheduling job {self.job.id}")
                stmt = self.broker._failed_statement(
                    self.job, self.attempt_num, finished_at_ms
                )

        else:
            logger.debug(
                f"Job {self.job.id} processed successfully (attempt {self.attempt_num})"
            )

            if self.job._rejected:
                # Put the job back in the queue
                stmt = self.broker._reject_statement(
                    self.job.id, self.attempt_num
                )
            elif self.job._rescheduled:
                # Reschedule the job for later
                stmt = self.broker._reschedule_statement(
                    self.job,
                    self.job._rescheduled_at,
                    attempt_num=self.attempt_num,
                )
            else:
                # Update the job status to "success"
                stmt = self.broker._success_statement(
                    self.job.id, self.attempt_num, finished_at_ms
                )

        await self.session.execute(stmt)
        await self.session.commit()
        await self.session.close()

        # An option to force-quit the subscribe() loop if an unhandled
        # exception occurs while processing the job.
        if self.raise_stop_on_unhandled_exc and exc_type:
            raise StopSubscription

        # If StopSubscription is raised inside the context manager, allow
        # it to propagate.
        if exc_type is StopSubscription:
            return False

        # Indicate that all other uncaught exceptions were handled inside the
        # context manager and should be suppressed.
        return True


class AsyncRaquel(BaseRaquel):
    """Raquel is a simple and reliable job queue for Python.

    It is recommended to use ``Raquel.dequeue`` method as a context manager,
    when processing jobs.

    Examples:

        Initialize with in memory SQLite database
        >>> engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        >>> rq = AsyncRaquel(engine)

        Initialize with PostgreSQL
        >>> rq = AsyncRaquel("postgresql+asyncpg://postgres:postgres@localhost/postgres")

        Create the jobs table or make sure it exists
        >>> await rq.create_all()

        Enqueue a job
        >>> await rq.enqueue("default", {"foo": "bar"})

        Process jobs, one by one
        >>> while True:
        ...     async with rq.dequeue() as job:
        ...         if job:
        ...             await process_job(job)
        ...     asyncio.sleep(1)

        Put the job back in the queue without processing it if you don't
        want to process it yet for some reason. For example, if the payload
        is empty.
        >>> async with rq.dequeue("my-tasks") as job:
        ...     if job and not job.payload:
        ...         await job.reject()

        Cancel the job before it is processed or between retries
        >>> await rq.cancel(job.id)

    Args:
        engine_or_url (AsyncEngine | str | URL): SQLAlchemy async engine or
            database connection string.
        **kwargs: Additional keyword arguments to pass to SQLAlchemy's
            ``create_engine()`` function
    """

    def __init__(self, engine_or_url: AsyncEngine | str | URL, **kwargs: Any) -> None:
        if isinstance(engine_or_url, AsyncEngine):
            self.engine = engine_or_url
        else:
            self.engine = create_async_engine(engine_or_url, **kwargs)
        self.async_session_factory = async_sessionmaker(
            self.engine, expire_on_commit=False
        )
        self.subscriptions: list[AsyncSubscription] = []

    async def enqueue(
        self,
        queue: str | None = None,
        payload: Any | None = None,
        at: datetime | int | None = None,
        delay: int | timedelta | None = None,
        max_age: int | timedelta = None,
        max_retry_count: int | None = None,
        min_retry_delay: int | timedelta | None = None,
        max_retry_delay: int | timedelta | None = None,
        backoff_base: int | None = None,
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
            >>> await rq.enqueue("default", {"foo": "bar"})

            Enqueue an empty payload in the "my_jobs" queue
            >>> await rq.enqueue("my_jobs")

            Enqueue a job object
            >>> job = Job(queue="ingest", payload="data", scheduled_at=now())
            >>> await rq.enqueue(payload=job)

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
            queue=queue,
            payload=payload,
            at=at,
            delay=delay,
            max_age=max_age,
            max_retry_count=max_retry_count,
            min_retry_delay=min_retry_delay,
            max_retry_delay=max_retry_delay,
            backoff_base=backoff_base,
        )

        # Insert the job
        job: Job | None = None
        async with self.async_session_factory() as session:
            raw_job = RawJob.from_enqueue_params(p)
            session.add(raw_job)
            await session.commit()

            job = Job.from_raw_job(raw_job)
        return job
    
    def add_subscription(
        self,
        fn: DecoratedCallable,
        queues: str | Iterable[str],
        claim_as: str | None = None,
        expire: bool = True,
        sleep: int = 1000,
        raise_stop_on_unhandled_exc: bool = False,
        reclaim_after: int = 60 * 1000,
    ) -> AsyncSubscription:
        """This is an experimental API. Subject to change.
        
        Add a subscription to the broker that will execute the callback function
        in a loop, processing dequeued jobs. If an unhandled exception occurs,
        the job will be automatically marked as failed and rescheduled based on
        the job's retry parameters.

        The first and only argument of the callback function should always be
        a ``Job``, which will be passed by the subscription for every new job
        that arrives.

        Examples:
            Subscribe a function to the "default" queue
            >>> async def process_job(job) -> None:
            ...     print(f"All good: {job.payload}")
            >>> subscription = rq.add_subscription(process_job, "default")
            >>> await subscription.run()

        Args:
            fn (DecoratedCallable): Callback function to process jobs.
            queues (str | Iterable[str]): The queues to subscribe to.
            claim_as (str | None): Optional parameter to identify whoever
                is locking the job. Defaults to None.
            expire (bool): Cancel expired jobs by running an
                ``UPDATE`` query in a separate transaction before claiming
                a new job. Defaults to True.
            sleep (int): Time to sleep between iterations in milliseconds.
                Defaults to 1000 ms.
            raise_stop_on_unhandled_exc (bool): Raise a ``StopSubscription``
                exception if an unhandled exception occurs while processing
                the job. Defaults to False.
            reclaim_after (int): Optional parameter to specify the
                reclaim delay in milliseconds. If a job was claimed but
                wasn't processed and the row remains unlocked, then it can
                be reclaimed after this delay. The delay is calculated from
                the previous ``claimed_at`` time. Defaults to 1 minute.

        Returns:
            AsyncSubscription: The subscription object.
        """
        if isinstance(queues, str):
            queues = [queues]

        subscription = AsyncSubscription(
            fn=fn,
            broker=self,
            queues=queues,
            claim_as=claim_as,
            expire=expire,
            sleep=sleep,
            raise_stop_on_unhandled_exc=raise_stop_on_unhandled_exc,
            reclaim_after=reclaim_after,
        )
        self.subscriptions.append(subscription) 
        return subscription

    def subscribe(
        self,
        *queues,
        claim_as: str | None = None,
        expire: bool = True,
        sleep: int = 1000,
        raise_stop_on_unhandled_exc: bool = False,
        reclaim_after: int = 60 * 1000,
    ) -> Callable[[DecoratedCallable], AsyncSubscription]:
        """This is an experimental API. Subject to change.
        
        Decorate a function to subscribe to one or more queues and process
        jobs as they arrive.

        The decorated function will be executed in a loop, processing dequeued
        jobs. If an unhandled exception occurs, the job will be automatically
        marked as failed and rescheduled based on the job's retry parameters.

        The first and only argument of the decorated function should always be
        a ``Job``, which will be passed by the subscription for every new job
        that arrives.

        Examples:

            Subscribe to the "default" and "high-priority" queues
            >>> @rq.subscribe("default", "high-priority", claim_as="worker-1"):
            ... async def process_job(job) -> None:
            ...     print(f"All good: {job.payload}")

            Launch all subscriptions
            >>> rq.run_subscriptions()

            Stop the subscription after processing the first job
            >>> @rq.subscribe("default"):
            ... def process_job(job: Job) -> None:
            ...     print(f"Processing job {job.id}")
            ...     raise StopSubscription

        Args:
            queues (str): One or more queue names.
            claim_as (str | None): Optional parameter to identify whoever
                is locking the job. Defaults to None.
            expire (bool): Cancel expired jobs by running an
                ``UPDATE`` query in a separate transaction before claiming
                a new job. Defaults to True.
            sleep (int): Time to sleep between iterations in milliseconds.
                Defaults to 1000 ms.
            raise_stop_on_unhandled_exc (bool): Raise a ``StopSubscription``
                exception if an unhandled exception occurs while processing
                the job. Defaults to False.
        """

        def decorator(
            fn: DecoratedCallable,
        ) -> DecoratedCallable:
            self.add_subscription(
                fn=fn,
                queues=queues,
                claim_as=claim_as,
                expire=expire,
                sleep=sleep,
                raise_stop_on_unhandled_exc=raise_stop_on_unhandled_exc,
                reclaim_after=reclaim_after,
            )
            return fn
        return decorator

    async def run_subscriptions(self) -> None:
        """This is an experimental API. Subject to change.
        
        Run all registered subscriptions.
        
        This method will spawn a task for each subscription and run it in
        a loop, processing dequeued jobs.

        The call to this method will block until the process is interrupted
        by a signal.
        """
        if not self.subscriptions:
            logger.warning("No subscriptions registered")
            return
        
        tasks = [asyncio.create_task(s.run()) for s in self.subscriptions]
        await asyncio.gather(*tasks)
        logger.info("All subscriptions have finished")

    def dequeue(
        self,
        *queues: str,
        before: datetime | int | None = None,
        claim_as: str | None = None,
        expire: bool = True,
        raise_stop_on_unhandled_exc: bool = False,
        reclaim_after: int = 60 * 1000,
    ) -> AsyncDequeueContextManager:
        """Take the oldest job and process it inside a context manager.

        It updates the job status to "claimed" and starts the processing
        of the job in a separate transaction. It first locks the job for
        processing using a ``SELECT ... FOR UPDATE`` database lock, when such
        functionality is supported by the database (PostgreSQL, Oracle, MySQL).

        Within the same transaction, the job status is then updated to "success"
        or "failed" depending on the outcome of the processing. If the job
        fails, the next attempt is scheduled based on the job's retry
        parameters.

        If the job has exceeded the maximum number of retries, the job status
        is changed to "exhausted".

        Examples:

            >>> while True:
            ...     async with rq.dequeue("my-tasks", claim_as="worker-1") as job:
            ...         if job:
            ...             await process_job(job)
            ...             print("Job processed successfully")
            ...     asyncio.sleep(1)

        Args:
            queues (str): One or more queue names.
            before (datetime | int | None): Look for jobs scheduled at or
                before this timestamp. Defaults to now (UTC).
            claim_as (str | None): Optional parameter to identify whoever
                is locking the job. Defaults to None.
            expire (bool): Cancel expired jobs by running an
                ``UPDATE`` query in a separate transaction before claiming
                a new job. Defaults to True.
            raise_stop_on_unhandled_exc (bool): Raise a ``StopSubscription``
                exception if an unhandled exception occurs while processing
                the job. Defaults to False.

        Yields:
            (Job | None): The oldest job in the queue or None if no job is available.
        """
        return AsyncDequeueContextManager(
            broker=self,
            queues=queues,
            before=before,
            claim_as=claim_as,
            expire=expire,
            raise_stop_on_unhandled_exc=raise_stop_on_unhandled_exc,
            reclaim_after=reclaim_after,
        )

    async def claim(
        self,
        *queues: str,
        before: datetime | int | None = None,
        claim_as: str | None = None,
        reclaim_after: int = 60 * 1000,
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
            reclaim_after (int | None): Optional parameter to specify the
                reclaim delay in milliseconds. If a job was claimed but
                wasn't processed and the row remains unlocked, then it can
                be reclaimed after this delay. The delay is calculated from
                the previous ``claimed_at`` time. Defaults to 1 minute.

        Returns:
            (Job | None): The acquired job or None if no job is available.
        """
        p = common.parse_claim_params(
            *queues, before=before, claim_as=claim_as
        )
        job: Job | None = None
        # Retrieve the earliest scheduled job in the queue and lock the row.
        async with self.async_session_factory() as session:
            where_clause = (
                or_(
                    RawJob.status.in_([self.QUEUED, self.FAILED]),
                    and_(
                        RawJob.status == self.CLAIMED,
                        RawJob.claimed_at + reclaim_after <= p.now_ms,
                    ),
                ),
                RawJob.scheduled_at <= p.before_ms,
                or_(
                    RawJob.max_age.is_(None),
                    RawJob.enqueued_at + RawJob.max_age >= p.now_ms,
                ),
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
            select_oldest_result = await session.execute(select_oldest_stmt)
            raw_job = select_oldest_result.scalars().first()
            if not raw_job:
                logger.debug(f"No job available in queue {queues}")
            else:
                # Lock the job
                update_claim_stmt = (
                    update(RawJob)
                    .where(RawJob.id == raw_job.id)
                    .values(
                        status=self.CLAIMED,
                        claimed_at=p.now_ms,
                        claimed_by=p.claim_as,
                    )
                )
                await session.execute(update_claim_stmt)
                await session.commit()

                job = Job.from_raw_job(raw_job)
                job.status = self.CLAIMED
                job.claimed_at = datetime.fromtimestamp(
                    p.before_ms / 1000, timezone.utc
                )
                job.claimed_by = p.claim_as
        return job

    async def unclaim(self, job_id: UUID) -> bool:
        """Release the claim on a job.

        This method is used to release the claim lock on a job that was
        previously claimed. The job will be available for processing by
        another worker.

        Only updates the job if it is in the "claimed" status. Sets the
        status to "queued" and clears the claimed_at and claimed_by fields.

        Args:
            job_id (UUID): Job ID.

        Returns:
            bool: True if the job was successfully unclaimed.
        """
        common.validate_job_id(job_id)
        async with self.async_session_factory() as session:
            stmt = (
                update(RawJob)
                .where(RawJob.id == job_id, RawJob.status == self.CLAIMED)
                .values(status=self.QUEUED, claimed_at=None, claimed_by=None)
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount == 1

    async def expire(self, *queues: str) -> int:
        """Cancel all expired jobs in the queue.

        Args:
            queue (str | None): Name of the queue. Defaults to all queues.

        Returns:
            int: Number of jobs cancelled.
        """
        for queue in queues:
            common.validate_queue_name(queue)

        async with self.async_session_factory() as session:
            where_clause = (
                RawJob.status.in_([self.QUEUED, self.FAILED]),
                RawJob.max_age.is_not(None),
                RawJob.scheduled_at + RawJob.max_age
                <= int(datetime.now(timezone.utc).timestamp() * 1000),
            )
            if queues:
                where_clause = (RawJob.queue.in_(queues),) + where_clause

            stmt = (
                update(RawJob).where(*where_clause).values(status=self.EXPIRED)
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount

    async def get(self, job_id: UUID) -> Job | None:
        """Get a job by ID.

        Args:
            job_id (UUID): Job ID.

        Returns:
            Job | None: The job or None if not found.
        """
        common.validate_job_id(job_id)
        async with self.async_session_factory() as session:
            stmt = select(RawJob).where(RawJob.id == job_id)
            result = await session.execute(stmt)
            raw_job = result.scalars().first()
            if not raw_job:
                return None
            job = Job.from_raw_job(raw_job)
            return job

    async def reject(self, job_id: UUID, attempt_num: int) -> bool:
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
        async with self.async_session_factory() as session:
            stmt = self._reject_statement(job_id, attempt_num)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount == 1

    async def cancel(self, job_id: UUID) -> bool:
        """Cancel a job before it is processed.

        Only jobs in the "queued" or "failed" status can be cancelled. For
        anything else, this method will have no effect.

        **Warning**: Calling this method inside the ``dequeue()`` context
        manager will not have any effect.

        Args:
            job_id (UUID): Job ID.
        """
        common.validate_job_id(job_id)
        async with self.async_session_factory() as session:
            stmt = (
                update(RawJob)
                .where(
                    RawJob.id == job_id,
                    RawJob.status.in_([self.QUEUED, self.FAILED]),
                )
                .values(status=self.CANCELLED)
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount == 1

    async def resolve(
        self,
        job_id: UUID,
        attempt_num: int = 1,
        finished_at: datetime | None = None,
    ) -> bool:
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
        async with self.async_session_factory() as session:
            if not finished_at:
                finished_at = datetime.now(timezone.utc)
            finished_at_ms = int(finished_at.timestamp() * 1000)
            stmt = self._success_statement(job_id, attempt_num, finished_at_ms)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount == 1

    async def fail(
        self,
        job: Job,
        attempt_num: int = 1,
        exception: Exception | None = None,
        finished_at: datetime | None = None,
    ) -> bool:
        """Mark the job as failed and reschedule it for another attempt.

        This method is used to mark a job as failed. It can be used when you
        are processing jobs outside the ``dequeue()`` context manager.

        **Warning**: This method should not be used inside the ``dequeue()``
        context manager.

        Args:
            job (Job): The job that failed.
            attempt_num (int): Number of attempts it took to process this job.
                Defaults to 1.
            error (str | None): Error message.
            error_trace (str | None): Error traceback. Defaults to None.
            finished_at (datetime | None): Time when the job was finished.
                Defaults to now (UTC).
        """
        common.validate_job_id(job.id)
        async with self.async_session_factory() as session:
            if not finished_at:
                finished_at = datetime.now(timezone.utc)
            finished_at_ms = int(finished_at.timestamp() * 1000)
            stmt = self._failed_statement(
                job, attempt_num, finished_at_ms, exception
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount == 1

    async def queues(self) -> list[str]:
        """List all queues.

        Returns:
            list[str]: List of all queues.
        """
        async with self.async_session_factory() as session:
            queues: list[str] = []
            stmt = select(RawJob.queue).group_by(RawJob.queue)
            result = await session.execute(stmt)
            for row in result:
                queues.append(row[0])
            return queues

    async def stats(self, *queues: str) -> dict[str, QueueStats]:
        """Compute stats for queues.

        Args:
            queues (str): One or more queue names. Defaults to all queues.

        Returns:
            dict[str, QueueStats]: All queues and their statistics.
        """
        async with self.async_session_factory() as session:
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

            if queues:
                if len(queues) == 1:
                    stmt = stmt.where(RawJob.queue == queues[0])
                else:
                    stmt = stmt.where(RawJob.queue.in_(queues))
            stmt = stmt.group_by(RawJob.queue)

            result = await session.execute(stmt)
            for row in result:
                queue_stats = QueueStats.from_row(row)
                stats[queue_stats.name] = queue_stats
            return stats

    async def jobs(self, *queues: str) -> list[Job]:
        """List all jobs in the queue from latest to oldest.

        Args:
            queues (str): One or more queue names. Defaults to all queues.

        Returns:
            list[Job]: List of jobs in the queue.
        """
        for queue in queues:
            common.validate_queue_name(queue)

        async with self.async_session_factory() as session:
            results: list[Job] = []
            stmt = select(RawJob)
            if queues:
                if len(queues) == 1:
                    stmt = stmt.where(RawJob.queue == queues[0])
                else:
                    stmt = stmt.where(RawJob.queue.in_(queues))
            stmt = stmt.order_by(desc(RawJob.scheduled_at))
            result = await session.execute(stmt)
            for raw_job in result.scalars():
                job = Job.from_raw_job(raw_job)
                results.append(job)
            return results

    async def count(
        self,
        queue: str | None = None,
        status: JobStatusValueType
        | Iterable[JobStatusValueType]
        | None = None,
    ) -> int:
        """Count the number of jobs in the queue with a specific status.

        Examples:

            Count the number of jobs in the "default" queue:
            >>> await rq.count("default")
            ... 10

            Count the number of jobs in the "default" queue with the "queued"
            or "failed" status:

            >>> await rq.count("default", [rq.QUEUED, rq.FAILED])
            ... 5

            Count the number of jobs in all queues with the "claimed" status:

        Args:
            queue (str): Queue name.
            status (str | Iterable[str] | None): Job statuses to count.
                Can be a single status or an iterabe of statuses. Defaults
                to all statuses.

        Returns:
            int: Number of jobs in the queue with the specified status.
        """
        common.validate_queue_name(queue)

        if status is not None:
            if isinstance(status, str):
                status = [status]
            elif not isinstance(status, Iterable):
                raise ValueError(
                    "status must be a string or an iterable of strings"
                )
            for s in status:
                common.validate_status(s)

        async with self.async_session_factory() as session:
            stmt = select(func.count(RawJob.id))
            if queue:
                stmt = stmt.where(RawJob.queue == queue)
            if status:
                if len(status) == 1:
                    stmt = stmt.where(RawJob.status == status[0])
                else:
                    stmt = stmt.where(RawJob.status.in_(status))
            result = await session.execute(stmt)
            num = result.scalar()
            return num

    async def create_all(self) -> None:
        """Create the jobs table and indexes.

        Only creates the objects if they do not exist.
        """
        async with self.engine.begin() as conn:
            await conn.run_sync(BaseSQL.metadata.create_all, checkfirst=True)

    async def drop_all(self) -> None:
        """Drop the jobs table and indexes.

        Only drops the objects if they exist.
        """
        async with self.engine.begin() as conn:
            await conn.run_sync(BaseSQL.metadata.drop_all, checkfirst=True)
