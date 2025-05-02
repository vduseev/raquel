from datetime import datetime, timezone

import pytest

from raquel import Raquel, AsyncRaquel
from .fixtures import rq_sqlite, rq_asyncpg


def test_basic_dequeue(rq_sqlite: Raquel):
    rq_sqlite.enqueue("default", {"foo": "bar"})

    with rq_sqlite.dequeue() as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.max_age == None
        assert job.max_retry_count == None
        assert job.enqueued_at <= datetime.now(timezone.utc)
        assert job.enqueued_at == job.scheduled_at
        assert job.attempts == 0
        assert job.claimed_at <= datetime.now(timezone.utc)
        assert job.finished_at == None

    # Make sure the job is not in the queue
    assert not rq_sqlite.claim("default")


@pytest.mark.asyncio
async def test_basic_dequeue_async(rq_asyncpg: AsyncRaquel):
    await rq_asyncpg.enqueue("default", {"foo": "bar"})

    async with rq_asyncpg.dequeue() as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.max_age == None
        assert job.max_retry_count == None
        assert job.enqueued_at <= datetime.now(timezone.utc)
        assert job.enqueued_at == job.scheduled_at
        assert job.attempts == 0
        assert job.claimed_at <= datetime.now(timezone.utc)
        assert job.finished_at == None

    # Make sure the job is not in the queue
    assert not await rq_asyncpg.claim("default")


def test_no_job_dequeue(rq_sqlite: Raquel):
    with rq_sqlite.dequeue("default") as job:
        assert job is None


@pytest.mark.asyncio
async def test_no_job_dequeue_async(rq_asyncpg: AsyncRaquel):
    async with rq_asyncpg.dequeue("default") as job:
        assert job is None


def test_max_age_in_time_dequeue(rq_sqlite: Raquel):
    rq_sqlite.enqueue("default", {"foo": "bar"}, max_age=1000)

    with rq_sqlite.dequeue("default") as job:
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED


@pytest.mark.asyncio
async def test_max_age_in_time_dequeue_async(rq_asyncpg: AsyncRaquel):
    await rq_asyncpg.enqueue("default", {"foo": "bar"}, max_age=1000)

    async with rq_asyncpg.dequeue("default") as job:
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED



def test_dequeue_in_order(rq_sqlite: Raquel):
    # Enqueue multiple jobs
    rq_sqlite.enqueue("default", {"foo": 1})
    rq_sqlite.enqueue("default", {"foo": 2})
    assert rq_sqlite.count("default") == 2
    assert rq_sqlite.count("default", rq_sqlite.QUEUED) == 2

    with rq_sqlite.dequeue("default") as job1:
        assert job1.status == rq_sqlite.CLAIMED
        assert job1.payload == {"foo": 1}
        assert rq_sqlite.count("default") == 2
        assert rq_sqlite.count("default", rq_sqlite.QUEUED) == 1
        assert rq_sqlite.count("default", rq_sqlite.CLAIMED) == 1

    with rq_sqlite.dequeue("default") as job2:
        assert job2.status == rq_sqlite.CLAIMED
        assert job2.payload == {"foo": 2}
        assert rq_sqlite.count("default") == 2
        assert rq_sqlite.count("default", rq_sqlite.QUEUED) == 0
        assert rq_sqlite.count("default", rq_sqlite.CLAIMED) == 1
        assert rq_sqlite.count("default", rq_sqlite.SUCCESS) == 1

    assert rq_sqlite.count("default") == 2
    assert rq_sqlite.count("default", rq_sqlite.QUEUED) == 0
    assert rq_sqlite.count("default", rq_sqlite.CLAIMED) == 0
    assert rq_sqlite.count("default", rq_sqlite.SUCCESS) == 2


@pytest.mark.asyncio
async def test_dequeue_in_order_async(rq_asyncpg: AsyncRaquel):
    # Enqueue multiple jobs
    await rq_asyncpg.enqueue("default", {"foo": 1})
    await rq_asyncpg.enqueue("default", {"foo": 2})
    assert await rq_asyncpg.count("default") == 2
    assert await rq_asyncpg.count("default", rq_asyncpg.QUEUED) == 2

    async with rq_asyncpg.dequeue("default") as job1:
        assert job1.status == rq_asyncpg.CLAIMED
        assert job1.payload == {"foo": 1}
        assert await rq_asyncpg.count("default") == 2
        assert await rq_asyncpg.count("default", rq_asyncpg.QUEUED) == 1
        assert await rq_asyncpg.count("default", rq_asyncpg.CLAIMED) == 1

    async with rq_asyncpg.dequeue("default") as job2:
        assert job2.status == rq_asyncpg.CLAIMED
        assert job2.payload == {"foo": 2}
        assert await rq_asyncpg.count("default") == 2
        assert await rq_asyncpg.count("default", rq_asyncpg.QUEUED) == 0
        assert await rq_asyncpg.count("default", rq_asyncpg.CLAIMED) == 1
        assert await rq_asyncpg.count("default", rq_asyncpg.SUCCESS) == 1

    assert await rq_asyncpg.count("default") == 2
    assert await rq_asyncpg.count("default", rq_asyncpg.QUEUED) == 0
    assert await rq_asyncpg.count("default", rq_asyncpg.CLAIMED) == 0
    assert await rq_asyncpg.count("default", rq_asyncpg.SUCCESS) == 2


def test_reject(rq_sqlite: Raquel):
    enqueued_job = rq_sqlite.enqueue("default", {"foo": "bar"})

    with rq_sqlite.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 0

        job.reject()

    assert rq_sqlite.count("default") == 1
    assert rq_sqlite.count("default", rq_sqlite.QUEUED) == 1

    updated_job = rq_sqlite.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_sqlite.QUEUED
    assert updated_job.claimed_at == None


@pytest.mark.asyncio
async def test_reject_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("default", {"foo": "bar"})

    async with rq_asyncpg.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        job.reject()

    assert await rq_asyncpg.count("default") == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.QUEUED) == 1

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_asyncpg.QUEUED
    assert updated_job.claimed_at == None


def test_exception(rq_sqlite: Raquel):
    enqueued_job = rq_sqlite.enqueue("default", {"foo": "bar"})

    with rq_sqlite.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 0

        raise ValueError("Won't do")
    
    assert rq_sqlite.count("default", status="failed") == 1
    
    updated_job = rq_sqlite.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_sqlite.FAILED
    assert updated_job.error == "Won't do"


@pytest.mark.asyncio
async def test_exception_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("default", {"foo": "bar"})

    async with rq_asyncpg.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        raise ValueError("Won't do")
    
    assert await rq_asyncpg.count("default", status="failed") == 1

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_asyncpg.FAILED
    assert updated_job.error == "Won't do"


def test_manual_fail(rq_sqlite: Raquel):
    enqueued_job = rq_sqlite.enqueue("default", {"foo": "bar"})

    with rq_sqlite.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 0

        job.fail("Won't do")

    assert rq_sqlite.count("default") == 1
    assert rq_sqlite.count("default", rq_sqlite.FAILED) == 1

    updated_job = rq_sqlite.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_sqlite.FAILED
    assert updated_job.error == "Won't do"


@pytest.mark.asyncio
async def test_manual_fail_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("default", {"foo": "bar"})

    async with rq_asyncpg.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        job.fail("Won't do")

    assert await rq_asyncpg.count("default") == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.FAILED) == 1

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_asyncpg.FAILED
    assert updated_job.error == "Won't do"


def test_manual_catch_exception_and_fail(rq_sqlite: Raquel):
    enqueued_job = rq_sqlite.enqueue("default", {"foo": "bar"})

    with rq_sqlite.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 0

        try:
            raise ValueError("Won't do")
        except ValueError as e:
            job.fail(e)

    assert rq_sqlite.count("default") == 1
    assert rq_sqlite.count("default", rq_sqlite.FAILED) == 1

    updated_job = rq_sqlite.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_sqlite.FAILED
    assert updated_job.error == "Won't do"
    assert updated_job.error_trace is not None


@pytest.mark.asyncio
async def test_manual_catch_exception_and_fail_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("default", {"foo": "bar"})

    async with rq_asyncpg.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        try:
            raise ValueError("Won't do")
        except ValueError as e:
            job.fail(e)

    assert await rq_asyncpg.count("default") == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.FAILED) == 1

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_asyncpg.FAILED
    assert updated_job.error == "Won't do"
    assert updated_job.error_trace is not None


def test_manual_catch_exception_no_fail(rq_sqlite: Raquel):
    enqueued_job = rq_sqlite.enqueue("default", {"foo": "bar"})

    with rq_sqlite.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 0

        try:
            raise ValueError("Won't do")
        except ValueError as e:
            pass

    assert rq_sqlite.count("default") == 1
    assert rq_sqlite.count("default", rq_sqlite.SUCCESS) == 1

    updated_job = rq_sqlite.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_sqlite.SUCCESS
    assert updated_job.error is None
    assert updated_job.error_trace is None


@pytest.mark.asyncio
async def test_manual_catch_exception_no_fail_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("default", {"foo": "bar"})

    async with rq_asyncpg.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        try:
            raise ValueError("Won't do")
        except ValueError as e:
            pass

    assert await rq_asyncpg.count("default") == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.SUCCESS) == 1

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_asyncpg.SUCCESS
    assert updated_job.error is None
    assert updated_job.error_trace is None
