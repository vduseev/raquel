import json
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any, Literal

from .params import EnqueueParams


logger = logging.getLogger(__name__)


@dataclass
class Job:
    queue: str
    id: int | None = field(default=None)
    payload: Any | None = field(default=None)
    status: Literal['queued', 'locked', 'success', 'failed', 'cancelled'] | None = field(default=None)
    max_age: int | None = field(default=None)
    max_retry_count: int = field(default=20)
    max_retry_exponent: int = field(default=32)
    min_retry_delay: int = field(default=1000)
    max_retry_delay: int = field(default=12 * 3600 * 1000)
    enqueued_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    scheduled_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    attempts: int = field(default=0)
    failed_error: str | None = field(default=None)
    failed_traceback: str | None = field(default=None)
    cancelled_reason: str | None = field(default=None)
    locked_at: datetime | None = field(default=None)
    finished_at: datetime | None = field(default=None)

    @staticmethod
    def from_row(row: tuple) -> "Job":
        # Parse the job row
        (
            job_id,
            queue,
            serialized_payload,
            status,
            max_age,
            max_retry_count,
            max_retry_exponent,
            min_retry_delay,
            max_retry_delay,
            enqueued_at,
            scheduled_at,
            attempts,
            failed_error,
            failed_stack_trace,
            cancelled_reason,
            locked_at,
            finished_at,
        ) = row
        
        # Attempt to deserialize the payload
        payload = Job.deserialize_payload(serialized_payload)

        # Convert epoch timestamps in milliseconds to datetime objects
        enqueued_at_ts = datetime.fromtimestamp(enqueued_at / 1000, timezone.utc)
        scheduled_at_ts = datetime.fromtimestamp(scheduled_at / 1000, timezone.utc)
        locked_at_ts = datetime.fromtimestamp(locked_at / 1000, timezone.utc) if locked_at else None
        finished_at_ts = datetime.fromtimestamp(finished_at / 1000, timezone.utc) if finished_at else None

        # Create the job object to pass to the context manager
        job = Job(
            id=job_id,
            queue=queue,
            payload=payload,
            status=status,
            max_age=max_age,
            max_retry_count=max_retry_count,
            max_retry_exponent=max_retry_exponent,
            min_retry_delay=min_retry_delay,
            max_retry_delay=max_retry_delay,
            enqueued_at=enqueued_at_ts,
            scheduled_at=scheduled_at_ts,
            attempts=attempts,
            failed_error=failed_error,
            failed_traceback=failed_stack_trace,
            cancelled_reason=cancelled_reason,
            locked_at=locked_at_ts,
            finished_at=finished_at_ts,
        )

        return job

    @staticmethod
    def from_enqueue_params(job_id: int, enqueue_params: EnqueueParams) -> "Job":
        payload = Job.deserialize_payload(enqueue_params.serialized_payload)
        return Job(
            id=job_id,
            queue=enqueue_params.queue,
            payload=payload,
            status="queued",
            max_age=enqueue_params.max_age_ms,
            max_retry_count=enqueue_params.max_retry_count,
            max_retry_exponent=enqueue_params.max_retry_exponent,
            min_retry_delay=enqueue_params.min_retry_delay,
            max_retry_delay=enqueue_params.max_retry_delay,
            enqueued_at=datetime.fromtimestamp(enqueue_params.enqueued_at_ms / 1000, timezone.utc),
            scheduled_at=datetime.fromtimestamp(enqueue_params.scheduled_at_ms / 1000, timezone.utc),
        )
    
    @staticmethod
    def deserialize_payload(serialized_payload: str | None) -> Any | None:
        if not serialized_payload:
            return None

        try:
            return json.loads(serialized_payload)
        except json.JSONDecodeError:
            logger.debug(f"Failed to deserialize payload using JSON: {serialized_payload}")
            return serialized_payload
