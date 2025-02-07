import asyncio
import threading
import time

import pytest

from raquel import Raquel, AsyncRaquel
from .fixtures import rq_psycopg2, rq_asyncpg


def test_conflict_cancel(rq_psycopg2: Raquel):
    job = rq_psycopg2.enqueue("conflict_cancel", 1)

    # Thread that will dequeue the job
    def dequeue_job():
        with rq_psycopg2.dequeue("conflict_cancel") as job:
            assert job.status == rq_psycopg2.CLAIMED
            assert job.payload == 1
            time.sleep(0.1)

    # Thread that will attempt to cancel the already dequeued job, but won't
    # do anything, since the job is already being processed.
    def cancel_job():
        assert rq_psycopg2.cancel(job.id) is False

    dequeue_thread = threading.Thread(target=dequeue_job)
    cancel_thread = threading.Thread(target=cancel_job)

    # First, start the dequeue thread
    dequeue_thread.start()

    # Wait a bit
    time.sleep(0.05)
    cancel_thread.start()
    dequeue_thread.join()
    cancel_thread.join()

    updated_job = rq_psycopg2.get(job.id)
    assert updated_job.status == rq_psycopg2.SUCCESS


@pytest.mark.asyncio
async def test_conflict_cancel_async(rq_asyncpg: AsyncRaquel):
    job = await rq_asyncpg.enqueue("conflict_cancel", 1)

    # Thread that will dequeue the job
    async def dequeue_job():
        async with rq_asyncpg.dequeue("conflict_cancel") as job:
            assert job.status == rq_asyncpg.CLAIMED
            assert job.payload == 1
            await asyncio.sleep(0.1)

    # Thread that will attempt to cancel the already dequeued job, but won't
    # do anything, since the job is already being processed.
    async def cancel_job():
        assert await rq_asyncpg.cancel(job.id) is False

    dequeue_task = asyncio.create_task(dequeue_job())
    cancel_task = asyncio.create_task(cancel_job())
    await asyncio.gather(dequeue_task, cancel_task)

    updated_job = await rq_asyncpg.get(job.id)
    assert updated_job.status == rq_asyncpg.SUCCESS


def test_nested_cancel(rq_psycopg2: Raquel):
    job = rq_psycopg2.enqueue("nested_cancel", 1)

    # Calling cancel inside the context manager has no effect
    with rq_psycopg2.dequeue("nested_cancel") as job:
        assert job.status == rq_psycopg2.CLAIMED
        assert job.payload == 1
        assert rq_psycopg2.cancel(job.id) is False

    updated_job = rq_psycopg2.get(job.id)
    assert updated_job.status == rq_psycopg2.SUCCESS


@pytest.mark.asyncio
async def test_nested_cancel_async(rq_asyncpg: AsyncRaquel):
    job = await rq_asyncpg.enqueue("nested_cancel", 1)

    # Calling cancel inside the context manager has no effect
    async with rq_asyncpg.dequeue("nested_cancel") as job:
        assert job.status == rq_asyncpg.CLAIMED
        assert job.payload == 1
        assert await rq_asyncpg.cancel(job.id) is False

    updated_job = await rq_asyncpg.get(job.id)
    assert updated_job.status == rq_asyncpg.SUCCESS
