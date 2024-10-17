import json
import logging
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Generator, Literal

from raquel.dialects.dbapi_types import Connection, ConnectionPool
from raquel.dialects import (
    Dialect,
    PostgresQueries,
    SQLiteQueries,
)
from raquel.models import Job, QueueStats, params


logger = logging.getLogger(__name__)


class BaseRaquel:
    """Raquel is a simple and reliable job queue for Python.

    It is recommended to use ``Raquel`` with a connection pool (SQLAlchemy
    engine is a connection pool too). Another recommendation is to use
    ``Raquel.dequeue`` method as a context manager, when dequeuing jobs.
    """

    QUEUED = "queued"
    LOCKED = "locked"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    EXHAUSTED = "exhausted"

    @staticmethod
    def _parse_enqueue_params(
        payload: Any | list[Any] | None = None,
        queue: str | None = None,
        at: datetime | int | None = None,
        delay: int | timedelta | None = None,
        max_age: int | timedelta = None,
        max_retry_count: int | None = None,
        max_retry_exponent: int | None = None,
        min_retry_delay: int | timedelta | None = None,
        max_retry_delay: int | timedelta | None = None,
    ) -> params.EnqueueParams:
        provided_payload = payload

        if isinstance(provided_payload, Job):
            payload = provided_payload.payload
            queue = queue or provided_payload.queue
            at = at or provided_payload.scheduled_at
            max_age = max_age or provided_payload.max_age
            max_retry_count = max_retry_count or provided_payload.max_retry_count
            max_retry_exponent = max_retry_exponent or provided_payload.max_retry_exponent
            min_retry_delay = min_retry_delay or provided_payload.min_retry_delay
            max_retry_delay = max_retry_delay or provided_payload.max_retry_delay

        queue = queue or "default"
        if not isinstance(queue, str):
            raise ValueError("Queue name must be a string")

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

        return params.EnqueueParams(
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
    
    @staticmethod
    def _parse_get_job_params(job_id: int) -> params.GetJobParams:
        if not job_id or not isinstance(job_id, int):
            raise ValueError("Job ID must be an integer")
        
        return params.GetJobParams(job_id=job_id)

    @staticmethod
    def _parse_acquire_job_params(
        queue: str,
        at: datetime | int | None = None,
        lock_as: str | None = None,
    ) -> params.AcquireJobParams:
        if not isinstance(queue, str):
            raise ValueError("Queue name must be a string")
        
        if not isinstance(lock_as, str) and lock_as is not None:
            raise ValueError("lock_as must be a string or None")

        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        if at is None:
            at = now
        if isinstance(at, datetime):
            at = int(at.timestamp() * 1000)

        return params.AcquireJobParams(
            queue=queue,
            now_ms=now,
            at_ms=at,
            lock_as=lock_as,
        )
    
    @staticmethod
    def _parse_list_jobs_params(queue: str) -> params.ListJobsParams:
        if not queue or not isinstance(queue, str):
            raise ValueError("Queue name must be a string")
        
        return params.ListJobsParams(queue=queue)
 
    @staticmethod
    def _parse_count_jobs_params(queue: str, status: str) -> params.CountJobsParams:
        if not queue or not isinstance(queue, str):
            raise ValueError("Queue name must be a string")
        
        if status and not isinstance(status, str):
            raise ValueError("Status must be a string")
        
        return params.CountJobsParams(queue=queue, status=status)
