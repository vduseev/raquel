from dataclasses import dataclass
from datetime import datetime
from uuid import UUID
from typing import Any, Literal


JobStatusValueType = Literal[
    "queued",
    "claimed",
    "success",
    "failed",
    "expired",
    "exhausted",
    "cancelled",
]


@dataclass
class BaseJob:
    id: UUID
    queue: str
    payload: Any | None
    status: JobStatusValueType | None
    max_age: int | None
    max_retry_count: int | None
    min_retry_delay: int | None
    max_retry_delay: int | None
    backoff_base: int | None
    enqueued_at: datetime
    scheduled_at: datetime
    attempts: int
    error: str | None
    error_trace: str | None
    claimed_by: str | None
    claimed_at: datetime | None
    finished_at: datetime | None
