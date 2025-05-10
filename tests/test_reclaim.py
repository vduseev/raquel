import multiprocessing
import time

import pytest

from raquel import Raquel, AsyncRaquel
from .fixtures import rq_psycopg2, rq_asyncpg


def dequeue_job_in_other_process():
    # different process, needs its own instance
    raquel = Raquel("postgresql+psycopg2://postgres:postgres@localhost:6432/postgres")
    with raquel.dequeue("reclaim_killed_worker") as job:
        # These asserts likely do nothing, as they are executed in another process?
        assert job is not None
        assert job.payload == 1
        time.sleep(10) # Simulate long processing time

def wait_until_job_is_claimed(rq_psycopg2, job):
    while True:
        updated_job = rq_psycopg2.get(job.id)
        if updated_job.status == rq_psycopg2.CLAIMED:
            break
        time.sleep(0.01)

def is_job_locked_in_db(rq_psycopg2, job):
    from raquel.models.raw_job import RawJob
    from sqlalchemy import select
    get_non_locked_job = (
        select(RawJob)
        .where(RawJob.id == job.id)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    with rq_psycopg2.engine.begin() as session:
        return session.execute(get_non_locked_job).one_or_none() is None

def test_reklaim_on_exit(rq_psycopg2: Raquel):
    job = rq_psycopg2.enqueue("reclaim_killed_worker", 1)
    dequeue_process = multiprocessing.Process(target=dequeue_job_in_other_process)

    dequeue_process.start()
    wait_until_job_is_claimed(rq_psycopg2, job)
    assert is_job_locked_in_db(rq_psycopg2, job)

    dequeue_process.terminate()

    updated_job = rq_psycopg2.get(job.id)
    assert updated_job is not None
    assert updated_job.status == rq_psycopg2.CLAIMED
    assert not is_job_locked_in_db(rq_psycopg2, job)
