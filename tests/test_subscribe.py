import asyncio
import time
from datetime import datetime, timezone

import pytest

from raquel import Raquel, AsyncRaquel
from .fixtures import simple, asynchronous


def test_basic_subscribe(simple: Raquel):
    simple.enqueue("default", {"foo": "bar"})
    simple.enqueue("tasks", 1)

    for i, job in enumerate(simple.subscribe("default", "tasks")):
        if i == 0:
            assert job.queue == "default"
            assert job.payload == {"foo": "bar"}
            assert job.status == simple.CLAIMED
            assert job.max_age == None
            assert job.max_retry_count == None
            assert job.enqueued_at <= datetime.now(timezone.utc)
            assert job.enqueued_at == job.scheduled_at
            assert job.attempts == 0
            assert job.claimed_at <= datetime.now(timezone.utc)
            assert job.finished_at == None
        elif i == 1:
            assert job.queue == "tasks"
            assert job.payload == 1
            break

    # Make sure the job is not in the queue
    assert not simple.claim("default")


@pytest.mark.asyncio
async def test_basic_subscribe_async(asynchronous: AsyncRaquel):
    await asynchronous.enqueue("default", {"foo": "bar"})
    await asynchronous.enqueue("tasks", 1)

    i = 0
    async for job in asynchronous.subscribe("default", "tasks"):
        if i == 0:
            assert job.queue == "default"
            assert job.payload == {"foo": "bar"}
            assert job.status == asynchronous.CLAIMED
            assert job.max_age == None
            assert job.max_retry_count == None
            assert job.enqueued_at <= datetime.now(timezone.utc)
            assert job.enqueued_at == job.scheduled_at
            assert job.attempts == 0
            assert job.claimed_at <= datetime.now(timezone.utc)
            assert job.finished_at == None
        elif i == 1:
            assert job.queue == "tasks"
            assert job.payload == 1
            break
        i += 1

    # Make sure the job is not in the queue
    assert not await asynchronous.claim("default")


def test_reject_subscribe(simple: Raquel):
    # Perform ENQUEUE
    enqueued_job = simple.enqueue("default", {"foo": "bar"})

    # Perform SUBSCRIBE
    for job in simple.subscribe("default"):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == simple.CLAIMED
        assert job.attempts == 0

        job.reject()
        break

    assert simple.count("default") == 1
    assert simple.count("default", simple.QUEUED) == 1

    updated_job = simple.get(enqueued_job.id)
    assert updated_job.attempts == 0
    assert updated_job.status == simple.QUEUED
    assert updated_job.claimed_at == None


@pytest.mark.asyncio
async def test_reject_subscribe_async(asynchronous: AsyncRaquel):
    # Perform ENQUEUE
    enqueued_job = await asynchronous.enqueue("default", {"foo": "bar"})

    # Perform SUBSCRIBE
    async for job in asynchronous.subscribe("default"):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == asynchronous.CLAIMED
        assert job.attempts == 0

        job.reject()
        break

    assert await asynchronous.count("default") == 1
    assert await asynchronous.count("default", asynchronous.QUEUED) == 1

    updated_job = await asynchronous.get(enqueued_job.id)
    assert updated_job.attempts == 0
    assert updated_job.status == asynchronous.QUEUED
    assert updated_job.claimed_at == None


def test_fail_subscribe(simple: Raquel):
    # Perform ENQUEUE
    enqueued_job = simple.enqueue("default", {"foo": "bar"})

    # Perform SUBSCRIBE
    for job in simple.subscribe("default"):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == simple.CLAIMED
        assert job.attempts == 0

        job.fail("Won't do")
        break

    assert simple.count("default") == 1
    assert simple.count("default", simple.FAILED) == 1

    updated_job = simple.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == simple.FAILED
    assert updated_job.error == "Won't do"


def test_catch_exception_and_fail_subscribe(simple: Raquel):
    # Perform ENQUEUE
    enqueued_job = simple.enqueue("default", {"foo": "bar"})

    # Perform SUBSCRIBE
    for job in simple.subscribe("default"):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == simple.CLAIMED
        assert job.attempts == 0

        try:
            raise ValueError("Won't do")
        except ValueError as e:
            job.fail(e)
        break

    assert simple.count("default") == 1
    assert simple.count("default", simple.FAILED) == 1

    updated_job = simple.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == simple.FAILED
    assert updated_job.error == "Won't do"
    assert updated_job.error_trace is not None


@pytest.mark.asyncio
async def test_fail_subscribe_async(asynchronous: AsyncRaquel):
    # Perform ENQUEUE
    enqueued_job = await asynchronous.enqueue("default", {"foo": "bar"})

    # Perform SUBSCRIBE
    async for job in asynchronous.subscribe("default"):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == asynchronous.CLAIMED
        assert job.attempts == 0

        job.fail("Won't do")
        break

    assert await asynchronous.count("default") == 1
    assert await asynchronous.count("default", asynchronous.FAILED) == 1

    updated_job = await asynchronous.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == asynchronous.FAILED
    assert updated_job.error == "Won't do"


def test_reschedule_subscribe(simple: Raquel):
    # Perform ENQUEUE
    enqueued_job = simple.enqueue("default", {"foo": "bar"})

    # Perform SUBSCRIBE
    for i, job in enumerate(simple.subscribe("default")):
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == simple.CLAIMED
        assert job.attempts == 0

        if i == 0:
            job.reschedule(delay=1)
        elif i == 1:
            break

    assert simple.count("default") == 1
    assert simple.count("default", simple.SUCCESS) == 1

    completed_job = simple.get(enqueued_job.id)
    assert completed_job.attempts == 1
    assert completed_job.status == simple.SUCCESS
    assert completed_job.claimed_at != None
    assert completed_job.scheduled_at < datetime.now(timezone.utc)
