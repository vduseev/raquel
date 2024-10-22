from dataclasses import dataclass


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


@dataclass
class ClaimParams:
    queue: str
    now_ms: int
    before_ms: int
    claim_as: str
