import json
from datetime import datetime, timezone, timedelta
from uuid import UUID
from typing import Any

from raquel.models.base_raquel import BaseRaquel
from raquel.models.base_job import BaseJob
from raquel.models.params import EnqueueParams, ClaimParams


def validate_queue_name(queue: str) -> None:
    if queue is not None and not isinstance(queue, str):
        raise ValueError("Queue name must be a string")


def validate_job_id(job_id: UUID) -> None:
    if not job_id or not isinstance(job_id, UUID):
        raise ValueError("Job ID must be a UUID")
    

def validate_claim_as(claim_as: str) -> None:
    if claim_as is not None and not isinstance(claim_as, str):
        raise ValueError("claim_as must be a string")
    

def validate_status(status: str) -> None:
    if status is not None and not isinstance(status, str):
        raise ValueError("Status must be a string")


def parse_enqueue_params(
    queue: str | None = None,
    payload: Any | list[Any] | None = None,
    at: datetime | int | None = None,
    delay: int | timedelta | None = None,
    max_age: int | timedelta = None,
    max_retry_count: int | None = None,
    max_retry_exponent: int | None = None,
    min_retry_delay: int | timedelta | None = None,
    max_retry_delay: int | timedelta | None = None,
) -> EnqueueParams:
    provided_payload = payload

    if isinstance(provided_payload, BaseJob):
        queue = queue or provided_payload.queue
        payload = provided_payload.payload
        at = at or provided_payload.scheduled_at
        max_age = max_age or provided_payload.max_age
        max_retry_count = max_retry_count or provided_payload.max_retry_count
        max_retry_exponent = max_retry_exponent or provided_payload.max_retry_exponent
        min_retry_delay = min_retry_delay or provided_payload.min_retry_delay
        max_retry_delay = max_retry_delay or provided_payload.max_retry_delay

    queue = queue or BaseRaquel.DEFAULT
    validate_queue_name(queue)

    max_retry_exponent = max_retry_exponent or 32
    if not isinstance(max_retry_exponent, int):
        raise ValueError("max_retry_exponent must be an integer")
    
    if max_retry_count and not isinstance(max_retry_count, int):
        raise ValueError("max_retry_count must be an integer")

    min_retry_delay = min_retry_delay or 1000
    max_retry_delay = max_retry_delay or 12 * 3600 * 1000

    # Determine the scheduled_at time
    now = datetime.now(timezone.utc)
    scheduled_at = at or now
    if isinstance(at, int):
        scheduled_at = datetime.fromtimestamp(at / 1000, timezone.utc)

    if delay:
        if isinstance(delay, int):
            delay = timedelta(milliseconds=delay)
        scheduled_at += delay

    # Serialize the payload
    serialized_payload = None
    if isinstance(payload, str):
        serialized_payload = payload
    elif payload and not isinstance(payload, str):
        serialized_payload = json.dumps(payload)

    # Convert config
    if isinstance(max_age, timedelta):
        max_age = int(max_age.total_seconds() * 1000)
    if isinstance(min_retry_delay, timedelta):
        min_retry_delay = int(min_retry_delay.total_seconds() * 1000)
    if isinstance(max_retry_delay, timedelta):
        max_retry_delay = int(max_retry_delay.total_seconds() * 1000)

    if max_retry_delay < min_retry_delay:
        raise ValueError("max_retry_delay cannot be less than min_retry_delay")

    # Convert timestatmps to milliseconds since epoch
    enqueued_at_ms = int(now.timestamp() * 1000)
    scheduled_at_ms = int(scheduled_at.timestamp() * 1000)

    return EnqueueParams(
        queue=queue,
        serialized_payload=serialized_payload,
        max_age_ms=max_age,
        max_retry_count=max_retry_count,
        max_retry_exponent=max_retry_exponent,
        min_retry_delay=min_retry_delay,
        max_retry_delay=max_retry_delay,
        enqueued_at_ms=enqueued_at_ms,
        scheduled_at_ms=scheduled_at_ms,
    )


def parse_claim_params(
    queue: str,
    before: datetime | int | None = None,
    claim_as: str | None = None,
) -> ClaimParams:
    validate_queue_name(queue)
    validate_claim_as(claim_as)

    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    if before is None:
        before = now
    if isinstance(before, datetime):
        before = int(before.timestamp() * 1000)

    return ClaimParams(
        queue=queue,
        now_ms=now,
        before_ms=before,
        claim_as=claim_as,
    )
