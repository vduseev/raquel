from datetime import datetime, timezone

import pytest

from raquel import Raquel, AsyncRaquel, StopSubscription, Job
from .fixtures import rq_psycopg2, rq_asyncpg


def test_basic_subscribe(rq_psycopg2: Raquel):
    rq_psycopg2.enqueue("default", {"foo": "bar"})
    rq_psycopg2.enqueue("tasks", 1)

    @rq_psycopg2.subscribe("default", "tasks")
    def worker(job: Job):
        if job.queue == "default":
            assert job.payload == {"foo": "bar"}
            assert job.status == rq_psycopg2.CLAIMED
            assert job.max_age == None
            assert job.max_retry_count == None
            assert job.enqueued_at <= datetime.now(timezone.utc)
            assert job.enqueued_at == job.scheduled_at
            assert job.attempts == 0
            assert job.claimed_at <= datetime.now(timezone.utc)
            assert job.finished_at == None
        elif job.queue == "tasks":
            assert job.payload == 1
            raise StopSubscription
        
    rq_psycopg2.run_subscriptions()

    # Make sure the jobs are not in the queues anymore
    assert not rq_psycopg2.claim()


@pytest.mark.asyncio
async def test_basic_subscribe_async(rq_asyncpg: AsyncRaquel):
    await rq_asyncpg.enqueue("default", {"foo": "bar"})
    await rq_asyncpg.enqueue("tasks", 1)

    @rq_asyncpg.subscribe("default", "tasks")
    async def worker(job: Job):
        if job.queue == "default":
            assert job.payload == {"foo": "bar"}
            assert job.status == rq_asyncpg.CLAIMED
            assert job.max_age == None
            assert job.max_retry_count == None
            assert job.enqueued_at <= datetime.now(timezone.utc)
            assert job.enqueued_at == job.scheduled_at
            assert job.attempts == 0
            assert job.claimed_at <= datetime.now(timezone.utc)
            assert job.finished_at == None
        elif job.queue == "tasks":
            assert job.payload == 1
            raise StopSubscription
        
    await rq_asyncpg.run_subscriptions()

    # Make sure the jobs are not in the queues anymore
    assert not await rq_asyncpg.claim()


def test_reject_subscribe(rq_psycopg2: Raquel):
    enqueued_job = rq_psycopg2.enqueue("default", {"foo": "bar"})

    def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_psycopg2.CLAIMED
        assert job.attempts == 0

        job.reject()
        raise StopSubscription
    
    subscription = rq_psycopg2.add_subscription(worker, "default")
    subscription.run()

    assert rq_psycopg2.count("default") == 1
    assert rq_psycopg2.count("default", rq_psycopg2.QUEUED) == 1

    updated_job = rq_psycopg2.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_psycopg2.QUEUED
    assert updated_job.claimed_at == None


@pytest.mark.asyncio
async def test_reject_subscribe_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("default", {"foo": "bar"})

    async def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        job.reject()
        raise StopSubscription
        
    subscription = rq_asyncpg.add_subscription(worker, "default")
    await subscription.run()

    assert await rq_asyncpg.count("default") == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.QUEUED) == 1

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_asyncpg.QUEUED
    assert updated_job.claimed_at == None


def test_exception(rq_psycopg2: Raquel):
    enqueued_job = rq_psycopg2.enqueue("default", {"foo": "bar"})

    @rq_psycopg2.subscribe("default", raise_stop_on_unhandled_exc=True)
    def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_psycopg2.CLAIMED
        assert job.attempts == 0

        raise ValueError("Won't do")
    
    rq_psycopg2.run_subscriptions()

    assert rq_psycopg2.count("default") == 1
    assert rq_psycopg2.count("default", rq_psycopg2.FAILED) == 1

    updated_job = rq_psycopg2.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_psycopg2.FAILED
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
    
    await rq_asyncpg.run_subscriptions()
    
    assert await rq_asyncpg.count("default") == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.FAILED) == 1

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_asyncpg.FAILED
    assert updated_job.error == "Won't do"


def test_manual_fail(rq_psycopg2: Raquel):
    enqueued_job = rq_psycopg2.enqueue("default", {"foo": "bar"})

    def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_psycopg2.CLAIMED
        assert job.attempts == 0

        job.fail("Won't do")
        raise StopSubscription
    
    subscription = rq_psycopg2.add_subscription(worker, "default")
    subscription.run()

    assert rq_psycopg2.count("default") == 1
    assert rq_psycopg2.count("default", rq_psycopg2.FAILED) == 1

    updated_job = rq_psycopg2.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_psycopg2.FAILED
    assert updated_job.error == "Won't do"


@pytest.mark.asyncio
async def test_manual_fail_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("default", {"foo": "bar"})

    async def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        job.fail("Won't do")
        raise StopSubscription
    
    subscription = rq_asyncpg.add_subscription(worker, "default")
    await subscription.run()

    assert await rq_asyncpg.count("default") == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.FAILED) == 1

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_asyncpg.FAILED
    assert updated_job.error == "Won't do"


def test_manual_catch_exception(rq_psycopg2: Raquel):
    enqueued_job = rq_psycopg2.enqueue("default", {"foo": "bar"})

    def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_psycopg2.CLAIMED
        assert job.attempts == 0

        try:
            raise ValueError("Won't do")
        except ValueError as e:
            pass
        raise StopSubscription
    
    subscription = rq_psycopg2.add_subscription(worker, "default")
    subscription.run()

    assert rq_psycopg2.count("default") == 1
    assert rq_psycopg2.count("default", rq_psycopg2.SUCCESS) == 1

    updated_job = rq_psycopg2.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_psycopg2.SUCCESS
    assert updated_job.error is None



@pytest.mark.asyncio
async def test_manual_catch_exception_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("default", {"foo": "bar"})

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
    
    subscription = rq_asyncpg.add_subscription(worker, "default")
    await subscription.run()

    assert await rq_asyncpg.count("default") == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.SUCCESS) == 1

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_asyncpg.SUCCESS
    assert updated_job.error is None


def test_reschedule(rq_psycopg2: Raquel):
    enqueued_job = rq_psycopg2.enqueue("default", {"foo": "bar"})

    def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_psycopg2.CLAIMED
        assert job.attempts == 0

        job.reschedule(delay=1000)
        raise StopSubscription
    
    subscription = rq_psycopg2.add_subscription(worker, "default")
    subscription.run()

    assert rq_psycopg2.count("default") == 1
    assert rq_psycopg2.count("default", rq_psycopg2.QUEUED) == 1

    updated_job = rq_psycopg2.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_psycopg2.QUEUED
    assert updated_job.error is None
    assert updated_job.scheduled_at > datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_reschedule_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("default", {"foo": "bar"})

    async def worker(job: Job):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == rq_asyncpg.CLAIMED
        assert job.attempts == 0

        job.reschedule(delay=1000)
        raise StopSubscription
    
    subscription = rq_asyncpg.add_subscription(worker, "default")
    await subscription.run()

    assert await rq_asyncpg.count("default") == 1
    assert await rq_asyncpg.count("default", rq_asyncpg.QUEUED) == 1

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == rq_asyncpg.QUEUED
    assert updated_job.error is None
    assert updated_job.scheduled_at > datetime.now(timezone.utc)
