from datetime import datetime, timezone
from uuid import uuid4

import pytest

from raquel import Raquel, AsyncRaquel
from .fixtures import rq_sqlite, rq_asyncpg


def test_job_enqueued(rq_sqlite: Raquel):
    rq_sqlite.enqueue(payload={"foo": "bar"})

    job = rq_sqlite.jobs()[0]
    assert job.queue == "default"
    assert job.payload == {"foo": "bar"}
    assert job.status == rq_sqlite.QUEUED
    assert job.max_age == None
    assert job.max_retry_count == None
    assert job.enqueued_at <= datetime.now(timezone.utc)
    assert job.enqueued_at == job.scheduled_at
    assert job.attempts == 0
    assert job.claimed_at == None
    assert job.finished_at == None


@pytest.mark.asyncio
async def test_job_enqueued_async(rq_asyncpg: AsyncRaquel):
    await rq_asyncpg.enqueue(payload={"foo": "bar"})

    job = (await rq_asyncpg.jobs())[0]
    assert job.queue == "default"
    assert job.payload == {"foo": "bar"}
    assert job.status == rq_asyncpg.QUEUED
    assert job.max_age == None
    assert job.max_retry_count == None
    assert job.enqueued_at <= datetime.now(timezone.utc)
    assert job.enqueued_at == job.scheduled_at
    assert job.attempts == 0
    assert job.claimed_at == None
    assert job.finished_at == None


def test_get_nonexistent_job(rq_sqlite: Raquel):
    # Get the job
    job = rq_sqlite.get(uuid4())
    assert job is None


@pytest.mark.asyncio
async def test_get_nonexistent_job_async(rq_asyncpg: AsyncRaquel):
    # Get the job
    job = await rq_asyncpg.get(uuid4())
    assert job is None


def test_claim_in_order(rq_sqlite: Raquel):
    # Enqueue multiple jobs
    rq_sqlite.enqueue("default", {"foo": 1})
    rq_sqlite.enqueue("default", {"foo": 2})
    assert rq_sqlite.count("default") == 2
    assert rq_sqlite.count("default", rq_sqlite.QUEUED) == 2

    job1 = rq_sqlite.claim("default")
    assert job1.status == rq_sqlite.CLAIMED
    assert job1.payload == {"foo": 1}
    assert rq_sqlite.count("default") == 2
    assert rq_sqlite.count("default", rq_sqlite.QUEUED) == 1
    assert rq_sqlite.count("default", rq_sqlite.CLAIMED) == 1

    job2 = rq_sqlite.claim()
    assert job2.status == rq_sqlite.CLAIMED
    assert job2.payload == {"foo": 2}
    assert rq_sqlite.count("default") == 2
    assert rq_sqlite.count("default", rq_sqlite.QUEUED) == 0
    assert rq_sqlite.count("default", rq_sqlite.CLAIMED) == 2


@pytest.mark.asyncio
async def test_claim_in_order_async(rq_asyncpg: AsyncRaquel):
    # Enqueue multiple jobs
    await rq_asyncpg.enqueue("default", {"foo": 1})
    await rq_asyncpg.enqueue("default", {"foo": 2})
    assert await rq_asyncpg.count("default") == 2
    assert await rq_asyncpg.count("default", rq_asyncpg.QUEUED) == 2

    job1 = await rq_asyncpg.claim("default")
    assert job1.status == rq_asyncpg.CLAIMED
    assert job1.payload == {"foo": 1}
    assert await rq_asyncpg.count("default") == 2
    assert await rq_asyncpg.count("default", rq_asyncpg.QUEUED) == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.CLAIMED) == 1

    job2 = await rq_asyncpg.claim()
    assert job2.status == rq_asyncpg.CLAIMED
    assert job2.payload == {"foo": 2}
    assert await rq_asyncpg.count("default") == 2
    assert await rq_asyncpg.count("default", rq_asyncpg.QUEUED) == 0
    assert await rq_asyncpg.count("default", rq_asyncpg.CLAIMED) == 2


def test_cancel_job(rq_sqlite: Raquel):
    job = rq_sqlite.enqueue("default", {"foo": 1})
    assert rq_sqlite.count("default") == 1
    assert rq_sqlite.count("default", rq_sqlite.QUEUED) == 1

    assert rq_sqlite.cancel(job.id) is True
    assert rq_sqlite.count("default", rq_sqlite.QUEUED) == 0
    assert rq_sqlite.count("default", rq_sqlite.CLAIMED) == 0
    assert rq_sqlite.count("default", rq_sqlite.CANCELLED) == 1


@pytest.mark.asyncio
async def test_cancel_job_async(rq_asyncpg: AsyncRaquel):
    job = await rq_asyncpg.enqueue("default", {"foo": 1})
    assert await rq_asyncpg.count("default") == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.QUEUED) == 1

    assert await rq_asyncpg.cancel(job.id) is True
    assert await rq_asyncpg.count("default", rq_asyncpg.QUEUED) == 0
    assert await rq_asyncpg.count("default", rq_asyncpg.CLAIMED) == 0
    assert await rq_asyncpg.count("default", rq_asyncpg.CANCELLED) == 1


def test_stats(rq_sqlite: Raquel):
    # Enqueue multiple jobs
    rq_sqlite.enqueue("default", {"foo": 1})
    rq_sqlite.enqueue("default", {"foo": 2})
    rq_sqlite.enqueue("other", {"foo": 3})
    assert rq_sqlite.count("default") == 2
    assert rq_sqlite.count("other") == 1

    # Check stats
    stats = rq_sqlite.stats()
    assert stats["default"].total == 2
    assert stats["default"].queued == 2
    assert stats["default"].claimed == 0
    assert stats["default"].success == 0
    assert stats["default"].failed == 0
    assert stats["default"].cancelled == 0
    assert stats["default"].expired == 0
    assert stats["default"].exhausted == 0
    assert stats["other"].total == 1


@pytest.mark.asyncio
async def test_stats_async(rq_asyncpg: AsyncRaquel):
    # Enqueue multiple jobs
    await rq_asyncpg.enqueue("default", {"foo": 1})
    await rq_asyncpg.enqueue("default", {"foo": 2})
    await rq_asyncpg.enqueue("other", {"foo": 3})
    assert await rq_asyncpg.count("default") == 2
    assert await rq_asyncpg.count("other") == 1

    # Check stats
    stats = await rq_asyncpg.stats()
    assert stats["default"].total == 2
    assert stats["default"].queued == 2
    assert stats["default"].claimed == 0
    assert stats["default"].success == 0
    assert stats["default"].failed == 0
    assert stats["default"].cancelled == 0
    assert stats["default"].expired == 0
    assert stats["default"].exhausted == 0
    assert stats["other"].total == 1
