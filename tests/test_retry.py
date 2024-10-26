import asyncio
import time
from datetime import timedelta

import pytest

from raquel import Raquel, AsyncRaquel
from .fixtures import simple, asynchronous


def test_min_retry_delay_int_dequeue(simple: Raquel):
    # Perform ENQUEUE
    simple.enqueue(payload={"foo": "bar"}, min_retry_delay=100)

    # Perform DEQUEUE
    with simple.dequeue() as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == simple.CLAIMED
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not simple.claim()

    # Wait for the job to be ready for processing
    time.sleep(0.2)

    # Make sure the job is back in the queue
    with simple.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == simple.CLAIMED
        assert job.attempts == 1


@pytest.mark.asyncio
async def test_min_retry_delay_int_dequeue_async(asynchronous: AsyncRaquel):
    # Perform ENQUEUE
    await asynchronous.enqueue(payload={"foo": "bar"}, min_retry_delay=100)

    # Perform DEQUEUE
    async with asynchronous.dequeue() as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == asynchronous.CLAIMED
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not await asynchronous.claim()

    # Wait for the job to be ready for processing
    await asyncio.sleep(0.2)

    # Make sure the job is back in the queue
    async with asynchronous.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == asynchronous.CLAIMED
        assert job.attempts == 1


def test_max_retry_delay_int_dequeue(simple: Raquel):
    # Perform ENQUEUE
    simple.enqueue(
        payload={"foo": "bar"},
        min_retry_delay=100,
        max_retry_delay=100,
    )

    # Perform DEQUEUE
    with simple.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == simple.CLAIMED
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not simple.claim("default")

    # Wait for the job to be ready for processing
    time.sleep(0.2)

    # Make sure the job is back in the queue
    with simple.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == simple.CLAIMED
        assert job.attempts == 1


@pytest.mark.asyncio
async def test_max_retry_delay_int_dequeue_async(asynchronous: AsyncRaquel):
    # Perform ENQUEUE
    await asynchronous.enqueue(
        payload={"foo": "bar"},
        min_retry_delay=100,
        max_retry_delay=100,
    )

    # Perform DEQUEUE
    async with asynchronous.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == asynchronous.CLAIMED
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not await asynchronous.claim("default")

    # Wait for the job to be ready for processing
    await asyncio.sleep(0.2)

    # Make sure the job is back in the queue
    async with asynchronous.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == asynchronous.CLAIMED
        assert job.attempts == 1


def test_min_max_retry_delay_timedelta_dequeue(simple: Raquel):
    # Perform ENQUEUE
    simple.enqueue(
        payload={"foo": "bar"},
        min_retry_delay=timedelta(milliseconds=100),
        max_retry_delay=timedelta(milliseconds=100),
    )

    # Perform DEQUEUE
    with simple.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == simple.CLAIMED
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not simple.claim("default")

    # Wait for the job to be ready for processing
    time.sleep(0.2)

    # Make sure the job is back in the queue
    with simple.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == simple.CLAIMED
        assert job.attempts == 1


@pytest.mark.asyncio
async def test_min_max_retry_delay_timedelta_dequeue_async(asynchronous: AsyncRaquel):
    # Perform ENQUEUE
    await asynchronous.enqueue(
        payload={"foo": "bar"},
        min_retry_delay=timedelta(milliseconds=100),
        max_retry_delay=timedelta(milliseconds=100),
    )

    # Perform DEQUEUE
    async with asynchronous.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == asynchronous.CLAIMED
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not await asynchronous.claim("default")

    # Wait for the job to be ready for processing
    await asyncio.sleep(0.2)

    # Make sure the job is back in the queue
    async with asynchronous.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == asynchronous.CLAIMED
        assert job.attempts == 1
