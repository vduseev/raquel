from dataclasses import dataclass


@dataclass
class EnqueueParams:
    queue: str
    serialized_payload: str | None
    max_age_ms: int | None
    max_retry_count: int | None
    min_retry_delay: int
    max_retry_delay: int
    backoff_base: int
    enqueued_at_ms: int
    scheduled_at_ms: int


@dataclass
class ClaimParams:
    queues: list[str]
    now_ms: int
    before_ms: int
    claim_as: str
