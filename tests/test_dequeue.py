import asyncio
import time
from datetime import datetime, timezone

import pytest

from raquel import Raquel, AsyncRaquel
from .fixtures import simple, asynchronous


def test_basic_dequeue(simple: Raquel):
    # Perform ENQUEUE
    simple.enqueue({"foo": "bar"})

    # Perform DEQUEUE
    with simple.dequeue() as job:
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

    # Make sure the job is not in the queue
    assert not simple.claim("default")


@pytest.mark.asyncio
async def test_basic_dequeue_async(asynchronous: AsyncRaquel):
    # Perform ENQUEUE
    await asynchronous.enqueue({"foo": "bar"})

    # Perform DEQUEUE
    async with asynchronous.dequeue() as job:
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

    # Make sure the job is not in the queue
    assert not await asynchronous.claim("default")


def test_no_job_dequeue(simple: Raquel):
    # Attempt to immediately dequeue the job
    with simple.dequeue(queue="default") as job:
        assert job is None


@pytest.mark.asyncio
async def test_no_job_dequeue_async(asynchronous: AsyncRaquel):
    # Attempt to immediately dequeue the job
    async with asynchronous.dequeue(queue="default") as job:
        assert job is None


def test_max_age_in_time_dequeue(simple: Raquel):
    # Perform ENQUEUE
    simple.enqueue({"foo": "bar"}, queue="default", max_age=1000)

    # Attempt to immediately dequeue the job
    with simple.dequeue("default") as job:
        assert job.payload == {"foo": "bar"}
        assert job.status == simple.CLAIMED


@pytest.mark.asyncio
async def test_max_age_in_time_dequeue_async(asynchronous: AsyncRaquel):
    # Perform ENQUEUE
    await asynchronous.enqueue({"foo": "bar"}, queue="default", max_age=1000)

    # Attempt to immediately dequeue the job
    async with asynchronous.dequeue("default") as job:
        assert job.payload == {"foo": "bar"}
        assert job.status == asynchronous.CLAIMED


def test_reject(simple: Raquel):
    # Perform ENQUEUE
    enqueued_job = simple.enqueue({"foo": "bar"}, queue="default")

    # Perform DEQUEUE
    with simple.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == simple.CLAIMED
        assert job.attempts == 0

        job.reject()

    assert simple.count("default") == 1
    assert simple.count("default", simple.QUEUED) == 1

    updated_job = simple.get(enqueued_job.id)
    assert updated_job.attempts == 0
    assert updated_job.status == simple.QUEUED
    assert updated_job.claimed_at == None


@pytest.mark.asyncio
async def test_reject_async(asynchronous: AsyncRaquel):
    # Perform ENQUEUE
    enqueued_job = await asynchronous.enqueue({"foo": "bar"}, queue="default")

    # Perform DEQUEUE
    async with asynchronous.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == asynchronous.CLAIMED
        assert job.attempts == 0

        job.reject()

    assert await asynchronous.count("default") == 1
    assert await asynchronous.count("default", asynchronous.QUEUED) == 1

    updated_job = await asynchronous.get(enqueued_job.id)
    assert updated_job.attempts == 0
    assert updated_job.status == asynchronous.QUEUED
    assert updated_job.claimed_at == None


def test_fail(simple: Raquel):
    # Perform ENQUEUE
    enqueued_job = simple.enqueue({"foo": "bar"}, queue="default")

    # Perform DEQUEUE
    with simple.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == simple.CLAIMED
        assert job.attempts == 0

        job.fail("Won't do")

    assert simple.count("default") == 1
    assert simple.count("default", simple.FAILED) == 1

    updated_job = simple.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == simple.FAILED
    assert updated_job.error == "Won't do"


@pytest.mark.asyncio
async def test_fail_async(asynchronous: AsyncRaquel):
    # Perform ENQUEUE
    enqueued_job = await asynchronous.enqueue({"foo": "bar"}, queue="default")

    # Perform DEQUEUE
    async with asynchronous.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == asynchronous.CLAIMED
        assert job.attempts == 0

        job.fail("Won't do")

    assert await asynchronous.count("default") == 1
    assert await asynchronous.count("default", asynchronous.FAILED) == 1

    updated_job = await asynchronous.get(enqueued_job.id)
    assert updated_job.attempts == 1
    assert updated_job.status == asynchronous.FAILED
    assert updated_job.error == "Won't do"
