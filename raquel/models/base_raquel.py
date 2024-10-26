from uuid import UUID
from datetime import timedelta

from sqlalchemy import update, Update

from .base_job import BaseJob
from .raw_job import RawJob


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
    def _reject_statement(job_id: UUID) -> Update:
        stmt = (
            update(RawJob)
            .where(RawJob.id == job_id)
            .values(status=BaseRaquel.QUEUED, claimed_at=None, claimed_by=None)
        )
        return stmt

    @staticmethod
    def _success_statement(job_id: UUID, attempt_num: int, finished_at: int) -> Update:
        stmt = (
            update(RawJob)
            .where(RawJob.id == job_id)
            .values(status=BaseRaquel.SUCCESS, attempts=attempt_num, finished_at=finished_at)
        )
        return stmt
    
    @staticmethod
    def _exhausted_statement(job_id: UUID, attempt_num: int, finished_at: int) -> Update:
        stmt = (
            update(RawJob)
            .where(RawJob.id == job_id)
            .values(status=BaseRaquel.EXHAUSTED, attempts=attempt_num, finished_at=finished_at)
        )
        return stmt
    
    @staticmethod
    def _failed_statement(
        job: BaseJob,
        attempt_num: int,
        finished_at: int,
    ) -> Update:
        # Calculate when to schedule the next attempt
        exponent = min(attempt_num, job.max_retry_exponent)
        planned_delay = 2 ** exponent
        actual_delay = max(job.min_retry_delay, planned_delay)
        duration = (finished_at - (job.claimed_at.timestamp() * 1000)) / 1000
        schedule_at = (
            job.scheduled_at
            + timedelta(seconds=duration)
            + timedelta(milliseconds=min(actual_delay, job.max_retry_delay))
        )

        stmt = (
            update(RawJob)
            .where(RawJob.id == job.id)
            .values(
                status=BaseRaquel.FAILED,
                attempts=attempt_num,
                error=job.error,
                error_trace=job.error_trace,
                scheduled_at=int(schedule_at.timestamp() * 1000),
                finished_at=finished_at,
            )
        )
        return stmt
