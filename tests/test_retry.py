import asyncio
import time
from datetime import timedelta

import pytest

from raquel import Raquel, AsyncRaquel
from .fixtures import rq_sqlite, rq_asyncpg


def test_min_retry_delay_int_dequeue(rq_sqlite: Raquel):
    rq_sqlite.enqueue(
        payload={"foo": "bar"},
        min_retry_delay=100,
        backoff_base=100,
    )

    with rq_sqlite.dequeue() as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not rq_sqlite.claim()

    # Wait for the job to be ready for processing
    time.sleep(0.2)

    # Make sure the job is back in the queue
    with rq_sqlite.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 1


@pytest.mark.asyncio
async def test_min_retry_delay_int_dequeue_async(rq_asyncpg: AsyncRaquel):
    await rq_asyncpg.enqueue(
        payload={"foo": "bar"},
        min_retry_delay=100,
        backoff_base=100,
    )

    async with rq_asyncpg.dequeue() as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not await rq_asyncpg.claim()

    # Wait for the job to be ready for processing
    await asyncio.sleep(0.2)

    # Make sure the job is back in the queue
    async with rq_asyncpg.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 1


def test_max_retry_delay_int_dequeue(rq_sqlite: Raquel):
    rq_sqlite.enqueue(
        payload={"foo": "bar"},
        min_retry_delay=100,
        max_retry_delay=100,
    )

    with rq_sqlite.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not rq_sqlite.claim("default")

    # Wait for the job to be ready for processing
    time.sleep(0.2)

    # Make sure the job is back in the queue
    with rq_sqlite.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 1


@pytest.mark.asyncio
async def test_max_retry_delay_int_dequeue_async(rq_asyncpg: AsyncRaquel):
    await rq_asyncpg.enqueue(
        payload={"foo": "bar"},
        min_retry_delay=100,
        max_retry_delay=100,
    )

    async with rq_asyncpg.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not await rq_asyncpg.claim("default")

    # Wait for the job to be ready for processing
    await asyncio.sleep(0.2)

    # Make sure the job is back in the queue
    async with rq_asyncpg.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 1


def test_min_max_retry_delay_timedelta_dequeue(rq_sqlite: Raquel):
    rq_sqlite.enqueue(
        payload={"foo": "bar"},
        min_retry_delay=timedelta(milliseconds=100),
        max_retry_delay=timedelta(milliseconds=100),
    )

    with rq_sqlite.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not rq_sqlite.claim("default")

    # Wait for the job to be ready for processing
    time.sleep(0.2)

    # Make sure the job is back in the queue
    with rq_sqlite.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 1


@pytest.mark.asyncio
async def test_min_max_retry_delay_timedelta_dequeue_async(rq_asyncpg: AsyncRaquel):
    await rq_asyncpg.enqueue(
        payload={"foo": "bar"},
        min_retry_delay=timedelta(milliseconds=100),
        max_retry_delay=timedelta(milliseconds=100),
    )

    async with rq_asyncpg.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not await rq_asyncpg.claim("default")

    # Wait for the job to be ready for processing
    await asyncio.sleep(0.2)

    # Make sure the job is back in the queue
    async with rq_asyncpg.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 1
