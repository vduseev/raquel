import asyncio
import multiprocessing
import time

import pytest
from raquel import AsyncRaquel, Raquel

from .fixtures import rq_asyncpg, rq_psycopg2


def dequeue_in_another_process(dsn: str):
    # Independent process, needs its own instance
    rq = Raquel(dsn)
    with rq.dequeue("reclaim_killed_worker", claim_as="worker-1") as job:
        time.sleep(10)  # Simulate long processing time


def test_reclaim_unlocked_row(rq_psycopg2: Raquel, postgres_dsn_sync: str):
    """
    Enqueue a job. Dequeue it for for processing in a completely independent
    process. This marks the job as claimed and locks the row.

    Then we will the process that locked the row. And test that we can reclaim
    the job.
    """
    job = rq_psycopg2.enqueue("reclaim_killed_worker", 1)

    # Start the process that will dequeue the job and lock the row
    dequeue_process = multiprocessing.Process(
        target=dequeue_in_another_process,
        args=(postgres_dsn_sync,),
    )
    dequeue_process.start()

    # Wait until the job is claimed by that other process
    wait_counter = 0
    while True:
        updated_job = rq_psycopg2.get(job.id)
        if (
            updated_job.status == rq_psycopg2.CLAIMED
            and updated_job.claimed_by == "worker-1"
        ):
            break
        wait_counter += 1
        if wait_counter > 100:
            raise Exception("Job was not claimed by the other process")
        time.sleep(0.01)

    # Now try to claim the job within this process. It should fail because the
    # row is locked.
    assert rq_psycopg2.claim(
        "reclaim_killed_worker",
        claim_as="worker-2",
        reclaim_after=0,
    ) is None

    # Terminate the process that was dequeueing the job.
    dequeue_process.terminate()
    dequeue_process.join()

    # Now the job should be reclaimable by this process even though it is
    # still marked as claimed. However, the row has no database lock anymore.
    reclaimed_job = rq_psycopg2.claim(
        "reclaim_killed_worker",
        claim_as="worker-2",
        reclaim_after=0,
    )
    assert reclaimed_job is not None
    assert reclaimed_job.status == rq_psycopg2.CLAIMED
    assert reclaimed_job.claimed_by == "worker-2"
    assert reclaimed_job.payload == 1


@pytest.mark.asyncio
async def test_reclaim_unlocked_row_async(rq_asyncpg: AsyncRaquel, postgres_dsn_sync: str):
    """
    Enqueue a job. Dequeue it for for processing in a completely independent
    process. This marks the job as claimed and locks the row.

    Then we will the process that locked the row. And test that we can reclaim
    the job.
    """
    job = await rq_asyncpg.enqueue("reclaim_killed_worker", 1)

    # Start the process that will dequeue the job and lock the row
    dequeue_process = multiprocessing.Process(
        target=dequeue_in_another_process,
        args=(postgres_dsn_sync,),
    )
    dequeue_process.start()

    # Wait until the job is claimed by that other process
    wait_counter = 0
    while True:
        updated_job = await rq_asyncpg.get(job.id)
        if (
            updated_job.status == rq_asyncpg.CLAIMED
            and updated_job.claimed_by == "worker-1"
        ):
            break
        wait_counter += 1
        if wait_counter > 100:
            raise Exception("Job was not claimed by the other process")
        await asyncio.sleep(0.01)

    # Now try to claim the job within this process. It should fail because the
    # row is locked.
    assert await rq_asyncpg.claim(
        "reclaim_killed_worker",
        claim_as="worker-2",
        reclaim_after=0,
    ) is None

    # Terminate the process that was dequeueing the job.
    dequeue_process.terminate()
    dequeue_process.join()

    # Now the job should be reclaimable by this process even though it is
    # still marked as claimed. However, the row has no database lock anymore.
    reclaimed_job = await rq_asyncpg.claim(
        "reclaim_killed_worker",
        claim_as="worker-2",
        reclaim_after=0,
    )
    assert reclaimed_job is not None
    assert reclaimed_job.status == rq_asyncpg.CLAIMED
    assert reclaimed_job.claimed_by == "worker-2"
    assert reclaimed_job.payload == 1
