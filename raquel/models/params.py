from dataclasses import dataclass
from datetime import datetime


@dataclass
class EnqueueParams:
    queue: str
    serialized_payload: str | None
    max_age_ms: int | None
    max_retry_count: int | None 
    max_retry_exponent: int
    min_retry_delay: int
    max_retry_delay: int
    enqueued_at_ms: int
    scheduled_at_ms: int

    def to_insert_row(self) -> tuple[str, str | None, str, int | None, int | None, int, int, int, int, int]:
        return (
            self.queue,
            self.serialized_payload,
            "queued",
            self.max_age_ms,
            self.max_retry_count,
            self.max_retry_exponent,
            self.min_retry_delay,
            self.max_retry_delay,
            self.enqueued_at_ms,
            self.scheduled_at_ms,
        )


@dataclass
class GetJobParams:
    job_id: int


@dataclass
class AcquireJobParams:
    queue: str
    now_ms: int
    at_ms: int
    lock_as: str


@dataclass
class ListJobsParams:
    queue: str


@dataclass
class CountJobsParams:
    queue: str
    status: str
