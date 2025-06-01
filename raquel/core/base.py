from uuid import UUID
from datetime import datetime, timedelta
from typing import Callable, TypeVar, Any

from sqlalchemy import update, Update

from raquel.models.base_job import BaseJob
from raquel.models.raw_job import RawJob


DecoratedCallable = TypeVar("DecoratedCallable", bound=Callable[..., Any])


class StopSubscription(BaseException):
    pass


class BaseRaquel:
    """This class exists for proper type hinting and dependency inversion."""

    QUEUED = "queued"
    CLAIMED = "claimed"
    SUCCESS = "success"
    FAILED = "failed"
    EXPIRED = "expired"
    EXHAUSTED = "exhausted"
    CANCELLED = "cancelled"
    DEFAULT = "default"

    @staticmethod
    def _reject_statement(job_id: UUID, attempt_num: int) -> Update:
        stmt = (
            update(RawJob)
            .where(RawJob.id == job_id)
            .values(
                status=BaseRaquel.QUEUED,
                claimed_at=None,
                claimed_by=None,
                attempts=attempt_num,
            )
        )
        return stmt

    @staticmethod
    def _success_statement(
        job_id: UUID, attempt_num: int, finished_at: int
    ) -> Update:
        stmt = (
            update(RawJob)
            .where(RawJob.id == job_id)
            .values(
                status=BaseRaquel.SUCCESS,
                attempts=attempt_num,
                finished_at=finished_at,
            )
        )
        return stmt

    @staticmethod
    def _exhausted_statement(
        job_id: UUID, attempt_num: int, finished_at: int
    ) -> Update:
        stmt = (
            update(RawJob)
            .where(RawJob.id == job_id)
            .values(
                status=BaseRaquel.EXHAUSTED,
                attempts=attempt_num,
                finished_at=finished_at,
            )
        )
        return stmt

    @staticmethod
    def _reschedule_statement(
        job: BaseJob,
        rescheduled_at: datetime,
        status: str = QUEUED,
        attempt_num: int = 0,
        finished_at: int | None = None,
    ) -> Update:
        stmt = (
            update(RawJob)
            .where(RawJob.id == job.id)
            .values(
                status=status,
                attempts=attempt_num,
                error=job.error,
                error_trace=job.error_trace,
                scheduled_at=int(rescheduled_at.timestamp() * 1000),
                finished_at=finished_at,
            )
        )
        return stmt

    @staticmethod
    def _failed_statement(
        job: BaseJob,
        attempt_num: int,
        finished_at: int,
    ) -> Update:
        # Calculate when to schedule the next attempt
        planned_delay = job.backoff_base * 2**attempt_num
        # Clamp the delay to the min and max values
        actual_delay = min(
            max(job.min_retry_delay, planned_delay), job.max_retry_delay
        )
        # Compute how much time it took to process the job
        duration = finished_at - (job.claimed_at.timestamp() * 1000)
        # Reschedule based on this values
        reschedule_at = (
            job.claimed_at
            + timedelta(milliseconds=duration)
            + timedelta(milliseconds=actual_delay)
        )

        return BaseRaquel._reschedule_statement(
            job=job,
            rescheduled_at=reschedule_at,
            status=BaseRaquel.FAILED,
            attempt_num=attempt_num,
            finished_at=finished_at,
        )
