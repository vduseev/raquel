from dataclasses import dataclass
from datetime import datetime
from uuid import UUID
from typing import Any, Literal


@dataclass
class BaseJob:
    id: UUID
    queue: str
    payload: Any | None
    status: Literal['queued', 'claimed', 'success', 'failed', 'expired', 'exhausted', 'cancelled'] | None
    max_age: int | None
    max_retry_count: int | None
    max_retry_exponent: int
    min_retry_delay: int
    max_retry_delay: int
    enqueued_at: datetime
    scheduled_at: datetime
    attempts: int
    error: str | None
    error_trace: str | None
    claimed_by: str | None
    claimed_at: datetime | None
    finished_at: datetime | None
