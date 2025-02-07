from datetime import datetime, timezone

import pytest

from raquel import Raquel, AsyncRaquel, StopSubscription, Job
from .fixtures import rq_sqlite, rq_asyncpg


def test_basic_subscribe(rq_sqlite: Raquel):
    rq_sqlite.enqueue("default", {"foo": "bar"})
    rq_sqlite.enqueue("tasks", 1)

    count = 0

    @rq_sqlite.subscribe("default", "tasks")
    def worker(job: Job):
        nonlocal count
        count += 1

        if count == 1:
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
        elif count == 2:
            assert job.queue == "tasks"
            assert job.payload == 1
            raise StopSubscription
        
    # Launch the subscriber. It will stop after 2 iterations
    worker.run()

    # Make sure the jobs are not in the queues anymore
    assert not rq_sqlite.claim()


@pytest.mark.asyncio
async def test_basic_subscribe_async(rq_asyncpg: AsyncRaquel):
    await rq_asyncpg.enqueue("default", {"foo": "bar"})
    await rq_asyncpg.enqueue("tasks", 1)

    count = 0

    @rq_asyncpg.subscribe("default", "tasks")
    async def worker(job: Job):
        nonlocal count
        count += 1

        if count == 1:
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
        elif count == 2:
            assert job.queue == "tasks"
            assert job.payload == 1
            raise StopSubscription
        
    await worker.run()

    # Make sure the jobs are not in the queues anymore
    assert not await rq_asyncpg.claim()


def test_reject_subscribe(rq_sqlite: Raquel):
    enqueued_job = rq_sqlite.enqueue("default", {"foo": "bar"})

    @rq_sqlite.subscribe("default")
    def worker(job: Job, stop: bool = False):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 0

        job.reject()
        if stop:
            raise StopSubscription
    
    worker.run(stop=True)

    assert rq_sqlite.count("default") == 1
    assert rq_sqlite.count("default", rq_sqlite.QUEUED) == 1

    updated_job = rq_sqlite.get(enqueued_job.id)
    assert updated_job.attempts == 0
    assert updated_job.status == rq_sqlite.QUEUED
    assert updated_job.claimed_at == None


@pytest.mark.asyncio
async def test_reject_subscribe_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("default", {"foo": "bar"})

    @rq_asyncpg.subscribe("default")
    async def worker(job: Job, stop: bool = False):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        job.reject()
        if stop:
            raise StopSubscription
        
    await worker.run(stop=True)

    assert await rq_asyncpg.count("default") == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.QUEUED) == 1

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.attempts == 0
    assert updated_job.status == rq_asyncpg.QUEUED
    assert updated_job.claimed_at == None


def test_exception(rq_sqlite: Raquel):
    enqueued_job = rq_sqlite.enqueue("default", {"foo": "bar"})

    @rq_sqlite.subscribe("default", raise_stop_on_unhandled_exc=True)
    def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 0

        raise ValueError("Won't do")
    
    worker.run()

    assert rq_sqlite.count("default") == 1
    assert rq_sqlite.count("default", rq_sqlite.FAILED) == 1

    updated_job = rq_sqlite.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_sqlite.FAILED
    assert updated_job.error == "Won't do"


@pytest.mark.asyncio
async def test_exception_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("default", {"foo": "bar"})

    @rq_asyncpg.subscribe("default", raise_stop_on_unhandled_exc=True)
    async def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        raise ValueError("Won't do")
    
    await worker.run()
    
    assert await rq_asyncpg.count("default") == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.FAILED) == 1

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_asyncpg.FAILED
    assert updated_job.error == "Won't do"


def test_manual_fail(rq_sqlite: Raquel):
    enqueued_job = rq_sqlite.enqueue("default", {"foo": "bar"})

    @rq_sqlite.subscribe("default")
    def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 0

        job.fail("Won't do")
        raise StopSubscription
    
    worker.run()

    assert rq_sqlite.count("default") == 1
    assert rq_sqlite.count("default", rq_sqlite.FAILED) == 1

    updated_job = rq_sqlite.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_sqlite.FAILED
    assert updated_job.error == "Won't do"


@pytest.mark.asyncio
async def test_manual_fail_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("default", {"foo": "bar"})

    @rq_asyncpg.subscribe("default")
    async def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        job.fail("Won't do")
        raise StopSubscription
    
    await worker.run()

    assert await rq_asyncpg.count("default") == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.FAILED) == 1

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_asyncpg.FAILED
    assert updated_job.error == "Won't do"


def test_manual_catch_exception(rq_sqlite: Raquel):
    enqueued_job = rq_sqlite.enqueue("default", {"foo": "bar"})

    @rq_sqlite.subscribe("default")
    def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 0

        try:
            raise ValueError("Won't do")
        except ValueError as e:
            pass
        raise StopSubscription
    
    worker.run()

    assert rq_sqlite.count("default") == 1
    assert rq_sqlite.count("default", rq_sqlite.SUCCESS) == 1

    updated_job = rq_sqlite.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_sqlite.SUCCESS
    assert updated_job.error is None



@pytest.mark.asyncio
async def test_manual_catch_exception_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("default", {"foo": "bar"})

    @rq_asyncpg.subscribe("default")
    async def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        try:
            raise ValueError("Won't do")
        except ValueError as e:
            pass
        raise StopSubscription
    
    await worker.run()

    assert await rq_asyncpg.count("default") == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.SUCCESS) == 1

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_asyncpg.SUCCESS
    assert updated_job.error is None


def test_reschedule(rq_sqlite: Raquel):
    enqueued_job = rq_sqlite.enqueue("default", {"foo": "bar"})

    @rq_sqlite.subscribe("default")
    def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_sqlite.CLAIMED
        assert job.attempts == 0

        job.reschedule(delay=1000)
        raise StopSubscription
    
    worker.run()

    assert rq_sqlite.count("default") == 1
    assert rq_sqlite.count("default", rq_sqlite.QUEUED) == 1

    updated_job = rq_sqlite.get(enqueued_job.id)
    assert updated_job.attempts == 0
    assert updated_job.status == rq_sqlite.QUEUED
    assert updated_job.error is None
    assert updated_job.scheduled_at > datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_reschedule_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("default", {"foo": "bar"})

    @rq_asyncpg.subscribe("default")
    async def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        job.reschedule(delay=1000)
        raise StopSubscription
    
    await worker.run()

    assert await rq_asyncpg.count("default") == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.QUEUED) == 1

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.attempts == 0
    assert updated_job.status == rq_asyncpg.QUEUED
    assert updated_job.error is None
    assert updated_job.scheduled_at > datetime.now(timezone.utc)
