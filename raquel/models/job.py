import json
import logging
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import uuid4, UUID
from typing import Any, Literal

from .base_job import BaseJob
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
    queue: str = field(default='default')
    """The name of the queue that the job belongs to."""
    payload: Any | None = field(default=None)
    """The payload of the job."""
    status: Literal['queued', 'claimed', 'success', 'failed', 'expired', 'exhausted', 'cancelled'] | None = field(default=None)
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
    """The maximum allowed time from enqueuing to processing.
    
    If the job is not processed within this time, it will not be processed
    at all.
    """
    max_retry_count: int | None = field(default=None)
    """The maximum number of retries.
    
    If the job fails, it will be retried up to this number of times.
    """
    min_retry_delay: int = field(default=1000)
    """The minimum retry delay.

    This is the minimum amount of time to wait before retrying a failed job.
    The delay between retries won't be less than this value.
    """
    max_retry_delay: int = field(default=12 * 3600 * 1000)
    """The maximum retry delay.

    This is the maximum amount of time to wait before retrying a failed job.
    The delay between retries won't exceed this value.
    """
    backoff_base: int = field(default=1000)
    """The base delay for exponential backoff during retry.

    The delay between retries is calculated as ``base * 2 ** retry`` in milliseconds.
    Then it is clamped between ``min_retry_delay`` and ``max_retry_delay``.
    """
    enqueued_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    """The time when the job was enqueued.
    
    Represented as a datetime object in UTC. In database, this is stored as
    a Unix epoch timestamp in milliseconds in UTC timezone.
    """
    scheduled_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
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
    _delay: int | None = field(default=None)

    @staticmethod
    def from_raw_job(raw_job: RawJob) -> "Job":
        # Attempt to deserialize the payload
        payload = Job.deserialize_payload(raw_job.payload)

        # Convert epoch timestamps in milliseconds to datetime objects
        enqueued_at_ts = datetime.fromtimestamp(raw_job.enqueued_at / 1000, timezone.utc)
        scheduled_at_ts = datetime.fromtimestamp(raw_job.scheduled_at / 1000, timezone.utc)
        claimed_at_t = datetime.fromtimestamp(raw_job.claimed_at / 1000, timezone.utc) if raw_job.claimed_at else None
        finished_at_ts = datetime.fromtimestamp(raw_job.finished_at / 1000, timezone.utc) if raw_job.finished_at else None

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
            claimed_at=claimed_at_t,
            finished_at=finished_at_ts,
        )

        return job
    
    def reject(self) -> None:
        """Reject the job.
        
        The lock is removed from the rejected job, allowing it to be
        **immediately** claimed by another worker. The ``scheduled_at``
        timestamp remains the same.

        **Warning:** This method **should only be called** inside the
        ``dequeue()`` context manager.
        """
        self._rejected = True

    def fail(self, exception: str | BaseException | None = None) -> None:
        """Fail the job.

        The job is marked as failed and the error message and stack trace
        are derived from the exception.

        **Warning:** This method **should only be called** inside the
        ``dequeue()`` context manager.

        Args:
            exception (str | BaseException | None): The exception that caused
                the job to fail. If a string is provided, it is used as the
                error message. If a BaseException is provided, its string
                representation is used as the error message and stack trace.
        """
        self._failed = True
        if exception:
            self.error = str(exception)
            if isinstance(exception, BaseException):
                stack_trace = "".join(traceback.format_exception(exception))
                self.error_trace = stack_trace

    def delay(self, delay: int | None = None) -> None:
        """Delay the job.
        
        If you want the job to be processed at a later time, you can delay it
        using this method. The job will remain in the queue and this attempt
        won't count towards the maximum number of retries.

        **Warning:** This method **should only be called** inside the
        ``dequeue()`` context manager.

        Args:
            delay (int | None): The delay in milliseconds. If None, the job
                will be delayed by ``min_retry_delay``.
        """
        self._delay = delay or self.min_retry_delay
    
    @staticmethod
    def deserialize_payload(serialized_payload: str | None) -> Any | None:
        if not serialized_payload:
            return None

        try:
            return json.loads(serialized_payload)
        except json.JSONDecodeError:
            logger.debug(f"Failed to deserialize payload using JSON: {serialized_payload}")
            return serialized_payload 
