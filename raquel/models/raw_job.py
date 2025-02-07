from datetime import datetime, timezone
from uuid import uuid4, UUID
from typing import Optional

from sqlalchemy import Integer, BigInteger, String, Uuid
from sqlalchemy.orm import mapped_column, Mapped

from .params import EnqueueParams
from .base_sql import BaseSQL


def now_ms() -> int:
    return int(
        datetime.now(timezone.utc).timestamp() * 1000
    )  # pragma: no cover


class RawJob(BaseSQL):
    __tablename__ = "jobs"

    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    queue: Mapped[str] = mapped_column(String, nullable=False, index=True)
    payload: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    status: Mapped[str] = mapped_column(
        String(30), nullable=False, default="queued", index=True
    )
    max_age: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    max_retry_count: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True
    )
    min_retry_delay: Mapped[int] = mapped_column(
        Integer, nullable=True, default=1000
    )
    max_retry_delay: Mapped[int] = mapped_column(
        Integer, nullable=True, default=12 * 3600 * 1000
    )
    backoff_base: Mapped[int] = mapped_column(
        Integer, nullable=True, default=1000
    )
    enqueued_at: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=now_ms
    )
    scheduled_at: Mapped[int] = mapped_column(
        BigInteger, nullable=False, default=now_ms, index=True
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    error_trace: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    claimed_by: Mapped[Optional[str]] = mapped_column(
        String, nullable=True, index=True
    )
    claimed_at: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True
    )
    finished_at: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True
    )

    @staticmethod
    def from_enqueue_params(enqueue_params: EnqueueParams) -> "RawJob":
        return RawJob(
            queue=enqueue_params.queue,
            payload=enqueue_params.serialized_payload,
            status="queued",
            max_age=enqueue_params.max_age_ms,
            max_retry_count=enqueue_params.max_retry_count,
            min_retry_delay=enqueue_params.min_retry_delay,
            max_retry_delay=enqueue_params.max_retry_delay,
            backoff_base=enqueue_params.backoff_base,
            enqueued_at=enqueue_params.enqueued_at_ms,
            scheduled_at=enqueue_params.scheduled_at_ms,
        )
