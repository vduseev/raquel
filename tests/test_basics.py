from datetime import datetime, timezone
from uuid import uuid4

import pytest

from raquel import Raquel, AsyncRaquel
from .fixtures import simple, asynchronous


def test_jobs(simple: Raquel):
    # Perform ENQUEUE
    simple.enqueue(payload={"foo": "bar"})

    # Get the job
    job = simple.jobs()[0]
    assert job.queue == "default"
    assert job.payload == {"foo": "bar"}
    assert job.status == simple.QUEUED
    assert job.max_age == None
    assert job.max_retry_count == None
    assert job.enqueued_at <= datetime.now(timezone.utc)
    assert job.enqueued_at == job.scheduled_at
    assert job.attempts == 0
    assert job.claimed_at == None
    assert job.finished_at == None


@pytest.mark.asyncio
async def test_jobs_async(asynchronous: AsyncRaquel):
    # Perform ENQUEUE
    await asynchronous.enqueue(payload={"foo": "bar"})

    # Get the job
    job = (await asynchronous.jobs())[0]
    assert job.queue == "default"
    assert job.payload == {"foo": "bar"}
    assert job.status == asynchronous.QUEUED
    assert job.max_age == None
    assert job.max_retry_count == None
    assert job.enqueued_at <= datetime.now(timezone.utc)
    assert job.enqueued_at == job.scheduled_at
    assert job.attempts == 0
    assert job.claimed_at == None
    assert job.finished_at == None


def test_get_nonexistent(simple: Raquel):
    # Get the job
    job = simple.get(uuid4())
    assert job is None


@pytest.mark.asyncio
async def test_get_nonexistent_async(asynchronous: AsyncRaquel):
    # Get the job
    job = await asynchronous.get(uuid4())
    assert job is None


def test_claim_in_order(simple: Raquel):
    # Enqueue multiple jobs
    simple.enqueue("default", {"foo": 1})
    simple.enqueue("default", {"foo": 2})
    assert simple.count() == 2
    assert simple.count("default", simple.QUEUED) == 2

    # Acquire the jobs
    job1 = simple.claim("default")
    assert job1.status == simple.CLAIMED
    assert job1.payload == {"foo": 1}
    assert simple.count("default") == 2
    assert simple.count("default", simple.QUEUED) == 1
    assert simple.count("default", simple.CLAIMED) == 1

    job2 = simple.claim()
    assert job2.status == simple.CLAIMED
    assert job2.payload == {"foo": 2}
    assert simple.count("default") == 2
    assert simple.count("default", simple.QUEUED) == 0
    assert simple.count("default", simple.CLAIMED) == 2


@pytest.mark.asyncio
async def test_claim_in_order_async(asynchronous: AsyncRaquel):
    # Enqueue multiple jobs
    await asynchronous.enqueue("default", {"foo": 1})
    await asynchronous.enqueue("default", {"foo": 2})
    assert await asynchronous.count() == 2
    assert await asynchronous.count("default", asynchronous.QUEUED) == 2

    # Acquire the jobs
    job1 = await asynchronous.claim("default")
    assert job1.status == asynchronous.CLAIMED
    assert job1.payload == {"foo": 1}
    assert await asynchronous.count("default") == 2
    assert await asynchronous.count("default", asynchronous.QUEUED) == 1
    assert await asynchronous.count("default", asynchronous.CLAIMED) == 1

    job2 = await asynchronous.claim()
    assert job2.status == asynchronous.CLAIMED
    assert job2.payload == {"foo": 2}
    assert await asynchronous.count("default") == 2
    assert await asynchronous.count("default", asynchronous.QUEUED) == 0
    assert await asynchronous.count("default", asynchronous.CLAIMED) == 2


def test_cancel(simple: Raquel):
    # Enqueue a job
    job = simple.enqueue("default", {"foo": 1})
    assert simple.count() == 1
    assert simple.count("default", simple.QUEUED) == 1

    assert simple.cancel(job.id) is True
    assert simple.count("default", simple.QUEUED) == 0
    assert simple.count("default", simple.CLAIMED) == 0
    assert simple.count("default", simple.CANCELLED) == 1


@pytest.mark.asyncio
async def test_cancel_async(asynchronous: AsyncRaquel):
    # Enqueue a job
    job = await asynchronous.enqueue("default", {"foo": 1})
    assert await asynchronous.count() == 1
    assert await asynchronous.count("default", asynchronous.QUEUED) == 1

    assert await asynchronous.cancel(job.id) is True
    assert await asynchronous.count("default", asynchronous.QUEUED) == 0
    assert await asynchronous.count("default", asynchronous.CLAIMED) == 0
    assert await asynchronous.count("default", asynchronous.CANCELLED) == 1


def test_dequeue_in_order(simple: Raquel):
    # Enqueue multiple jobs
    simple.enqueue("default", {"foo": 1})
    simple.enqueue("default", {"foo": 2})
    assert simple.count("default") == 2
    assert simple.count("default", simple.QUEUED) == 2

    # Dequeue the jobs
    with simple.dequeue("default") as job1:
        assert job1.status == simple.CLAIMED
        assert job1.payload == {"foo": 1}
        assert simple.count("default") == 2
        assert simple.count("default", simple.QUEUED) == 1
        assert simple.count("default", simple.CLAIMED) == 1

    with simple.dequeue("default") as job2:
        assert job2.status == simple.CLAIMED
        assert job2.payload == {"foo": 2}
        assert simple.count("default") == 2
        assert simple.count("default", simple.QUEUED) == 0
        assert simple.count("default", simple.CLAIMED) == 1
        assert simple.count("default", simple.SUCCESS) == 1

    assert simple.count("default") == 2
    assert simple.count("default", simple.QUEUED) == 0
    assert simple.count("default", simple.CLAIMED) == 0
    assert simple.count("default", simple.SUCCESS) == 2


@pytest.mark.asyncio
async def test_dequeue_in_order_async(asynchronous: AsyncRaquel):
    # Enqueue multiple jobs
    await asynchronous.enqueue("default", {"foo": 1})
    await asynchronous.enqueue("default", {"foo": 2})
    assert await asynchronous.count("default") == 2
    assert await asynchronous.count("default", asynchronous.QUEUED) == 2

    # Dequeue the jobs
    async with asynchronous.dequeue("default") as job1:
        assert job1.status == asynchronous.CLAIMED
        assert job1.payload == {"foo": 1}
        assert await asynchronous.count("default") == 2
        assert await asynchronous.count("default", asynchronous.QUEUED) == 1
        assert await asynchronous.count("default", asynchronous.CLAIMED) == 1

    async with asynchronous.dequeue("default") as job2:
        assert job2.status == asynchronous.CLAIMED
        assert job2.payload == {"foo": 2}
        assert await asynchronous.count("default") == 2
        assert await asynchronous.count("default", asynchronous.QUEUED) == 0
        assert await asynchronous.count("default", asynchronous.CLAIMED) == 1
        assert await asynchronous.count("default", asynchronous.SUCCESS) == 1

    assert await asynchronous.count("default") == 2
    assert await asynchronous.count("default", asynchronous.QUEUED) == 0
    assert await asynchronous.count("default", asynchronous.CLAIMED) == 0
    assert await asynchronous.count("default", asynchronous.SUCCESS) == 2


def test_stats(simple: Raquel):
    # Enqueue multiple jobs
    simple.enqueue("default", {"foo": 1})
    simple.enqueue("default", {"foo": 2})
    simple.enqueue("other", {"foo": 3})
    assert simple.count() == 3
    assert simple.count("other") == 1

    # Dequeue a job
    with simple.dequeue("default") as job:
        assert job.status == simple.CLAIMED
        assert job.payload == {"foo": 1}

    # Check stats
    stats = simple.stats()
    assert stats["default"].total == 2
    assert stats["default"].queued == 1
    assert stats["default"].claimed == 0
    assert stats["default"].success == 1
    assert stats["default"].failed == 0
    assert stats["default"].cancelled == 0
    assert stats["default"].expired == 0
    assert stats["default"].exhausted == 0

    assert stats["other"].total == 1


@pytest.mark.asyncio
async def test_stats_async(asynchronous: AsyncRaquel):
    # Enqueue multiple jobs
    await asynchronous.enqueue("default", {"foo": 1})
    await asynchronous.enqueue("default", {"foo": 2})
    await asynchronous.enqueue("other", {"foo": 3})
    assert await asynchronous.count() == 3
    assert await asynchronous.count("other") == 1

    # Dequeue a job
    async with asynchronous.dequeue("default") as job:
        assert job.status == asynchronous.CLAIMED
        assert job.payload == {"foo": 1}

    # Check stats
    stats = await asynchronous.stats()
    assert stats["default"].total == 2
    assert stats["default"].queued == 1
    assert stats["default"].claimed == 0
    assert stats["default"].success == 1
    assert stats["default"].failed == 0
    assert stats["default"].cancelled == 0
    assert stats["default"].expired == 0
    assert stats["default"].exhausted == 0

    assert stats["other"].total == 1
