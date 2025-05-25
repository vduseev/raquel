import json
import logging
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from uuid import uuid4, UUID
from typing import Any

from .base_job import BaseJob, JobStatusValueType
from .raw_job import RawJob


logger = logging.getLogger(__name__)


@dataclass
class Job(BaseJob):
    id: UUID = field(default_factory=uuid4)
    """The unique identifier for the job.

    Raquel uses UUID4 to generate unique identifiers for each job. The UUID4
    or random UUID is purposefully used to avoid collisions between jobs,
    even in distributed system as well as to allow for an easy data migration.

    If you have 103 trillion jobs in the same database, the probability of
    a single collision is 1 in a billion.
    
    This is generated automatically on the client side."""
    queue: str = field(default="default")
    """The name of the queue that the job belongs to."""
    payload: Any | None = field(default=None)
    """The payload of the job."""
    status: JobStatusValueType | None = field(default=None)
    """The status of the job.
    
    When jobs are placed in the database, they are initially in the "queued"
    state, ready to be claimed and processed. Then when a worker claims a job
    for processing, it is marked as "claimed". If the job is processed
    successfully, it chanes its status to "success". If the job fails, it
    changes to "failed".
    
    Finnaly, if the job is not processed within the maximum age, it changes
    to "expired". And if the job has reached the maximum number of retries,
    it changes to "exhausted".
    """
    max_age: int | None = field(default=None)
    """The maximum allowed time in milliseconds from enqueuing to processing.
    
    If the job is not processed within this time, it will not be processed
    at all.
    """
    max_retry_count: int | None = field(default=None)
    """The maximum number of retries.
    
    If the job fails, it will be retried up to this number of times.
    """
    min_retry_delay: int | None = field(default=1000)
    """The minimum retry delay in milliseconds.

    This is the minimum amount of time to wait before retrying a failed job.
    The delay between retries won't be less than this value.
    """
    max_retry_delay: int | None = field(default=12 * 3600 * 1000)
    """The maximum retry delay in milliseconds.

    This is the maximum amount of time to wait before retrying a failed job.
    The delay between retries won't exceed this value.
    """
    backoff_base: int | None = field(default=1000)
    """The base delay in milliseconds for exponential backoff during retry.

    The delay between retries is calculated as ``base * 2 ** retry`` in milliseconds.
    Then it is clamped between ``min_retry_delay`` and ``max_retry_delay``.
    """
    enqueued_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    """The time when the job was enqueued.
    
    Represented as a datetime object in UTC. In database, this is stored as
    a Unix epoch timestamp in milliseconds in UTC timezone.
    """
    scheduled_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    """The time when the job is scheduled for processing.

    The job will not be processed before this time.

    Represented as a datetime object in UTC. In database, this is stored as
    a Unix epoch timestamp in milliseconds in UTC timezone.
    """
    attempts: int = field(default=0)
    """The number of attempts made to process the job."""
    error: str | None = field(default=None)
    """The error message if the job failed."""
    error_trace: str | None = field(default=None)
    """The stack trace if the job failed."""
    claimed_by: str | None = field(default=None)
    """The name of the worker that claimed the job."""
    claimed_at: datetime | None = field(default=None)
    """The time when the job was claimed by a worker.
    
    Represented as a datetime object in UTC. In database, this is stored as
    a Unix epoch timestamp in milliseconds in UTC timezone.
    """
    finished_at: datetime | None = field(default=None)
    """The time when the job was finished processing.

    Represented as a datetime object in UTC. In database, this is stored as
    a Unix epoch timestamp in milliseconds in UTC timezone.
    """
    _rejected: bool = field(default=False)
    _failed: bool = field(default=False)
    _rescheduled: bool = field(default=False)
    _rescheduled_at: datetime | None = field(default=None)

    @staticmethod
    def from_raw_job(raw_job: RawJob) -> "Job":
        # Attempt to deserialize the payload
        payload = Job.deserialize_payload(raw_job.payload)

        # Convert epoch timestamps in milliseconds to datetime objects
        enqueued_at_ts = datetime.fromtimestamp(
            raw_job.enqueued_at / 1000, timezone.utc
        )
        scheduled_at_ts = datetime.fromtimestamp(
            raw_job.scheduled_at / 1000, timezone.utc
        )
        claimed_at_t = (
            datetime.fromtimestamp(raw_job.claimed_at / 1000, timezone.utc)
            if raw_job.claimed_at
            else None
        )
        finished_at_ts = (
            datetime.fromtimestamp(raw_job.finished_at / 1000, timezone.utc)
            if raw_job.finished_at
            else None
        )

        job = Job(
            id=raw_job.id,
            queue=raw_job.queue,
            payload=payload,
            status=raw_job.status,
            max_age=raw_job.max_age,
            max_retry_count=raw_job.max_retry_count,
            min_retry_delay=raw_job.min_retry_delay,
            max_retry_delay=raw_job.max_retry_delay,
            backoff_base=raw_job.backoff_base,
            enqueued_at=enqueued_at_ts,
            scheduled_at=scheduled_at_ts,
            attempts=raw_job.attempts,
            error=raw_job.error,
            error_trace=raw_job.error_trace,
            claimed_by=raw_job.claimed_by,
            claimed_at=claimed_at_t,
            finished_at=finished_at_ts,
        )

        return job

    def reject(self) -> None:
        """Reject the job.

        The lock is removed from the rejected job, allowing it to be
        claimed by another worker. The removal of lock is performed by the
        ``dequeue()`` context manager, once the processing of the job is
        complete. The ``scheduled_at`` timestamp remains the same.

        Warning: This method should be called inside the
        ``dequeue()`` or ``subscribe()`` context manager only.
        """
        self._rejected = True

    def fail(self, exception: str | Exception | None = None) -> None:
        """Fail the job without raising an exception.

        The job is marked as failed and the error message and stack trace
        are derived from the exception.

        Warning: This method should be called inside the
        ``dequeue()`` or ``subscribe()`` context manager only.

        Args:
            exception (str | Exception | None): The exception that caused
                the job to fail. If a string is provided, it is used as the
                error message. If a Exception is provided, its string
                representation is used as the error message and stack trace.
        """
        self._failed = True
        if exception:
            self.error = str(exception)
            if isinstance(exception, Exception):
                stack_trace = "".join(traceback.format_exception(exception))
                self.error_trace = stack_trace

    def reschedule(
        self,
        delay: timedelta | int | None = None,
        at: datetime | int | None = None,
    ) -> None:
        """Reschedule the job.

        Reprocess the job at a later time. The job will remain in the queue
        with a new scheduled execution time, and the current attempt won't
        count towards the maximum number of retries.

        If you leave both ``at`` and ``delay`` as None, the job will be
        scheduled to be reprocessed after a minimal delay which defaults to
        ``min_retry_delay`` value of the current job. If both are provided,
        the new scheduled time will computed as ``at + delay``.

        If you simply want to put the job back in the queue for immediate
        reprocessing, use the ``reject()`` method instead.

        **Warning:** This method **should only be called** inside the
        ``dequeue()`` or ``subscribe()`` context manager.

        Args:
            delay (timedelta | int | None): Reprocess the job after the given
                delay. You can pass a ``timedelta`` object or an integer
                representing the delay in milliseconds. If None and ``at``
                argument is not provided, the job will be delayed by
                ``min_retry_delay``.
            at (datetime | int | None): The time when the job should be
                processed. You can pass a ``datetime`` object or a unix epoch
                timestamp in milliseconds. Whatever is passed is considered
                to be in UTC. If None, the job will be scheduled to be
                reprocessed after a minimal delay.
        """
        self._rescheduled = True

        if at is None:
            self._rescheduled_at = self.scheduled_at
        elif isinstance(at, datetime):
            self._rescheduled_at = at
        elif isinstance(at, int):
            self._rescheduled_at = datetime.fromtimestamp(
                at / 1000, timezone.utc
            )
        else:
            raise ValueError(
                "Invalid value for 'at' argument. Expected datetime or int."
            )

        delay_ms = 0
        if at is None and delay is None:
            delay_ms = self.min_retry_delay
        elif isinstance(delay, int):
            delay_ms = delay
        elif isinstance(delay, datetime):
            delay_ms = int(delay.total_seconds() * 1000)

        self._rescheduled_at = self._rescheduled_at + timedelta(
            milliseconds=delay_ms
        )

    @staticmethod
    def deserialize_payload(serialized_payload: str | None) -> Any | None:
        if not serialized_payload:
            return None

        try:
            return json.loads(serialized_payload)
        except json.JSONDecodeError:
            logger.debug(
                f"Failed to deserialize payload using JSON: {serialized_payload}"
            )
            return serialized_payload
