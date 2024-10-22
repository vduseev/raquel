import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

from raquel import Raquel, AsyncRaquel, Job
from .fixtures import simple, asynchronous


def test_basic_enqueue(simple: Raquel):
    # Make sure the queue doesn't exist
    assert not simple.queues()

    # Perform ENQUEUE
    simple.enqueue({"foo": "bar"}, queue="foo")

    # Make sure the queue exists
    assert "foo" in simple.queues()
    assert "default" not in simple.queues()

    # Make sure the job exists
    jobs = simple.jobs("foo")
    assert len(jobs) == 1
    job = jobs[0]
    assert job.queue == "foo"
    assert job.payload == {"foo": "bar"}
    assert job.status == "queued"
    assert job.max_age == None
    assert job.max_retry_count == None
    assert job.enqueued_at <= datetime.now(timezone.utc)
    assert job.enqueued_at == job.scheduled_at
    assert job.attempts == 0
    assert job.claimed_at == None
    assert job.finished_at == None


@pytest.mark.asyncio
async def test_basic_enqueue_async(asynchronous: AsyncRaquel):
    # Make sure the queue doesn't exist
    assert not await asynchronous.queues()

    # Perform ENQUEUE
    await asynchronous.enqueue({"foo": "bar"}, queue="foo")

    # Make sure the queue exists
    assert "foo" in await asynchronous.queues()
    assert "default" not in await asynchronous.queues()

    # Make sure the job exists
    jobs = await asynchronous.jobs("foo")
    assert len(jobs) == 1
    job = jobs[0]
    assert job.queue == "foo"
    assert job.payload == {"foo": "bar"}
    assert job.status == "queued"
    assert job.max_age == None
    assert job.max_retry_count == None
    assert job.enqueued_at <= datetime.now(timezone.utc)
    assert job.enqueued_at == job.scheduled_at
    assert job.attempts == 0
    assert job.claimed_at == None
    assert job.finished_at == None


def test_at_datetime_enqueue(simple: Raquel):
    delayed_by = 100  # milliseconds
    scheduled_at = datetime.now(timezone.utc) + timedelta(milliseconds=delayed_by)
    # Perform ENQUEUE
    simple.enqueue({"foo": "bar"}, at=scheduled_at)

    # Attempt to immediately dequeue the job
    assert not simple.claim()

    # Wait for the job to be ready for processing
    time.sleep(delayed_by / 1000)

    job = simple.claim()
    assert job.queue == "default"
    assert job.status == simple.CLAIMED


@pytest.mark.asyncio
async def test_at_datetime_enqueue_async(asynchronous: AsyncRaquel):
    delayed_by = 100
    scheduled_at = datetime.now(timezone.utc) + timedelta(milliseconds=delayed_by)
    # Perform ENQUEUE
    await asynchronous.enqueue({"foo": "bar"}, at=scheduled_at)

    # Attempt to immediately dequeue the job
    assert not await asynchronous.claim()

    # Wait for the job to be ready for processing
    await asyncio.sleep(delayed_by / 1000)

    job = await asynchronous.claim()
    assert job.queue == "default"
    assert job.status == asynchronous.CLAIMED


def test_at_int_enqueue(simple: Raquel):
    delayed_by = 100  # milliseconds
    at = int(datetime.now(timezone.utc).timestamp() * 1000) + delayed_by
    # Perform ENQUEUE
    simple.enqueue({"foo": "bar"}, at=at)

    # Attempt to immediately dequeue the job
    assert not simple.claim()

    # Wait for the job to be ready for processing
    time.sleep(delayed_by / 1000)

    job = simple.claim("default")
    assert job.queue == "default"
    assert job.status == simple.CLAIMED


@pytest.mark.asyncio
async def test_at_int_enqueue_async(asynchronous: AsyncRaquel):
    delayed_by = 100
    at = int(datetime.now(timezone.utc).timestamp() * 1000) + delayed_by
    # Perform ENQUEUE
    await asynchronous.enqueue({"foo": "bar"}, at=at)

    # Attempt to immediately dequeue the job
    assert not await asynchronous.claim()

    # Wait for the job to be ready for processing
    await asyncio.sleep(delayed_by / 1000)

    job = await asynchronous.claim("default")
    assert job.queue == "default"
    assert job.status == asynchronous.CLAIMED


def test_delayed_int_enqueue(simple: Raquel):
    delayed_by = 100  # milliseconds
    # Perform ENQUEUE
    simple.enqueue({"foo": "bar"}, queue="default", delay=delayed_by)

    # Attempt to immediately dequeue the job
    assert not simple.claim("default")

    # Wait for the job to be ready for processing
    time.sleep(delayed_by / 1000)

    job = simple.claim("default")
    assert job.queue == "default"
    assert job.status == simple.CLAIMED


@pytest.mark.asyncio
async def test_delayed_int_enqueue_async(asynchronous: AsyncRaquel):
    delayed_by = 100
    # Perform ENQUEUE
    await asynchronous.enqueue({"foo": "bar"}, queue="default", delay=delayed_by)

    # Attempt to immediately dequeue the job
    assert not await asynchronous.claim("default")

    # Wait for the job to be ready for processing
    await asyncio.sleep(delayed_by / 1000)

    job = await asynchronous.claim("default")
    assert job.queue == "default"
    assert job.status == asynchronous.CLAIMED


def test_delayed_timedelta_enqueue(simple: Raquel):
    delayed_by = timedelta(milliseconds=100)
    # Perform ENQUEUE
    simple.enqueue({"foo": "bar"}, delay=delayed_by)

    # Attempt to immediately dequeue the job
    assert not simple.claim("default")

    # Wait for the job to be ready for processing
    time.sleep(0.1)

    job = simple.claim("default")
    assert job.queue == "default"
    assert job.status == simple.CLAIMED


@pytest.mark.asyncio
async def test_delayed_timedelta_enqueue_async(asynchronous: AsyncRaquel):
    delayed_by = timedelta(milliseconds=100)
    # Perform ENQUEUE
    await asynchronous.enqueue({"foo": "bar"}, delay=delayed_by)

    # Attempt to immediately dequeue the job
    assert not await asynchronous.claim("default")

    # Wait for the job to be ready for processing
    await asyncio.sleep(0.1)

    job = await asynchronous.claim("default")
    assert job.queue == "default"
    assert job.status == asynchronous.CLAIMED


def test_enqueue_in_the_past(simple: Raquel):
    scheduled_at = datetime.now(timezone.utc) - timedelta(hours=2)
    before = datetime.now(timezone.utc) - timedelta(hours=1)
    # Perform ENQUEUE
    job1 = simple.enqueue(1)
    job2 = simple.enqueue(2, at=scheduled_at)  # should be picked up first

    # Attempt to immediately dequeue the job
    job = simple.claim("default", before=before)
    assert job.queue == "default"
    assert job.status == simple.CLAIMED


@pytest.mark.asyncio
async def test_enqueue_in_the_past_async(asynchronous: AsyncRaquel):
    scheduled_at = datetime.now(timezone.utc) - timedelta(hours=2)
    before = datetime.now(timezone.utc) - timedelta(hours=1)
    # Perform ENQUEUE
    job1 = await asynchronous.enqueue(1)
    job2 = await asynchronous.enqueue(2, at=scheduled_at)

    # Attempt to immediately dequeue the job
    job = await asynchronous.claim("default", before=before)
    assert job.queue == "default"
    assert job.status == asynchronous.CLAIMED


def test_no_payload_enqueue(simple: Raquel):
    # Perform ENQUEUE
    simple.enqueue()

    job = simple.claim("default")
    assert job.queue == "default"
    assert job.status == simple.CLAIMED
    assert job.payload is None


@pytest.mark.asyncio
async def test_no_payload_enqueue_async(asynchronous: AsyncRaquel):
    # Perform ENQUEUE
    await asynchronous.enqueue()

    job = await asynchronous.claim("default")
    assert job.queue == "default"
    assert job.status == asynchronous.CLAIMED
    assert job.payload is None


def test_str_payload_enqueue(simple: Raquel):
    # Perform ENQUEUE
    simple.enqueue("foo", "default")

    job = simple.claim("default")
    assert job.queue == "default"
    assert job.status == simple.CLAIMED
    assert job.payload == "foo"


@pytest.mark.asyncio
async def test_str_payload_enqueue_async(asynchronous: AsyncRaquel):
    # Perform ENQUEUE
    await asynchronous.enqueue("foo", "default")

    job = await asynchronous.claim("default")
    assert job.queue == "default"
    assert job.status == asynchronous.CLAIMED
    assert job.payload == "foo"


def test_job_payload_enqueue(simple: Raquel):
    job = Job(
        queue="job_queue",
        payload=1,
    )

    # Perform ENQUEUE
    job = simple.enqueue(job)
    assert job.queue == "job_queue"
    assert job.status == "queued"
    assert job.payload == 1

    acquired_job = simple.claim("job_queue")
    assert acquired_job.queue == "job_queue"
    assert acquired_job.status == simple.CLAIMED
    assert acquired_job.payload == 1


@pytest.mark.asyncio
async def test_job_payload_enqueue_async(asynchronous: AsyncRaquel):
    job = Job(
        queue="job_queue",
        payload=1,
    )

    # Perform ENQUEUE
    job = await asynchronous.enqueue(job)
    assert job.queue == "job_queue"
    assert job.status == "queued"
    assert job.payload == 1

    acquired_job = await asynchronous.claim("job_queue")
    assert acquired_job.queue == "job_queue"
    assert acquired_job.status == asynchronous.CLAIMED
    assert acquired_job.payload == 1


def test_max_age_timedelta_enqueue(simple: Raquel):
    max_age = timedelta(milliseconds=50)
    # Perform ENQUEUE
    simple.enqueue({"foo": "bar"}, max_age=max_age)

    # Wait for the job to expire
    time.sleep(0.1)

    # Queue is empty
    assert not simple.claim("default")


@pytest.mark.asyncio
async def test_max_age_timedelta_enqueue_async(asynchronous: AsyncRaquel):
    max_age = timedelta(milliseconds=50)
    # Perform ENQUEUE
    await asynchronous.enqueue({"foo": "bar"}, max_age=max_age)

    # Wait for the job to expire
    await asyncio.sleep(0.1)

    # Queue is empty
    assert not await asynchronous.claim("default")


def test_max_age_int_enqueue(simple: Raquel):
    max_age = 50
    # Perform ENQUEUE
    simple.enqueue({"foo": "bar"}, max_age=max_age)

    # Wait for the job to expire
    time.sleep(0.1)

    # Queue is empty
    assert not simple.claim("default")


@pytest.mark.asyncio
async def test_max_age_int_enqueue_async(asynchronous: AsyncRaquel):
    max_age = 50
    # Perform ENQUEUE
    await asynchronous.enqueue({"foo": "bar"}, max_age=max_age)

    # Wait for the job to expire
    await asyncio.sleep(0.1)

    # Queue is empty
    assert not await asynchronous.claim("default")
