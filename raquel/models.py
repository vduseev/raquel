import enum
import json
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any


logger = logging.getLogger(__name__)


class Dialect(str, enum.Enum):
    SQLITE = "sqlite"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    ORACLE = "oracle"
    MSSQL = "mssql"


@dataclass
class Job:
    id: int
    queue: str
    payload: Any | None = field(default=None)
    status: str | None = field(default=None)
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
        payload = None
        if serialized_payload:
            try:
                payload = json.loads(serialized_payload)
            except json.JSONDecodeError:
                logger.debug(f"Failed to deserialize payload using JSON: {serialized_payload}")
                payload = serialized_payload

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


@dataclass
class QueueStats:
    name: str
    total: int
    queued: int
    locked: int
    success: int
    failed: int
    cancelled: int

    @staticmethod
    def from_row(row: tuple) -> "QueueStats":
        name, total, queued, locked, success, failed, cancelled = row
        return QueueStats(
            name=name,
            total=total,
            queued=queued,
            locked=locked,
            success=success,
            failed=failed,
            cancelled=cancelled,
        )
