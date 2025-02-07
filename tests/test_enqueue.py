import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

from raquel import Raquel, AsyncRaquel, Job
from .fixtures import rq_sqlite, rq_asyncpg


def test_basic_enqueue(rq_sqlite: Raquel):
    # Make sure the queue doesn't exist
    assert not rq_sqlite.queues()

    rq_sqlite.enqueue("foo", {"foo": "bar"})

    # Make sure the queue exists
    assert "foo" in rq_sqlite.queues()
    assert "default" not in rq_sqlite.queues()

    # Make sure the job exists
    jobs = rq_sqlite.jobs("foo")
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
async def test_basic_enqueue_async(rq_asyncpg: AsyncRaquel):
    # Make sure the queue doesn't exist
    assert not await rq_asyncpg.queues()

    await rq_asyncpg.enqueue("foo", {"foo": "bar"})

    # Make sure the queue exists
    assert "foo" in await rq_asyncpg.queues()
    assert "default" not in await rq_asyncpg.queues()

    # Make sure the job exists
    jobs = await rq_asyncpg.jobs("foo")
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


def test_at_datetime_enqueue(rq_sqlite: Raquel):
    delayed_by = 100  # milliseconds
    scheduled_at = datetime.now(timezone.utc) + timedelta(milliseconds=delayed_by)
    rq_sqlite.enqueue(payload={"foo": "bar"}, at=scheduled_at)

    # Attempt to immediately dequeue the job
    assert not rq_sqlite.claim()

    # Wait for the job to be ready for processing
    time.sleep(delayed_by / 1000)

    job = rq_sqlite.claim()
    assert job.queue == "default"
    assert job.status == rq_sqlite.CLAIMED


@pytest.mark.asyncio
async def test_at_datetime_enqueue_async(rq_asyncpg: AsyncRaquel):
    delayed_by = 100
    scheduled_at = datetime.now(timezone.utc) + timedelta(milliseconds=delayed_by)
    await rq_asyncpg.enqueue(payload={"foo": "bar"}, at=scheduled_at)

    # Attempt to immediately dequeue the job
    assert not await rq_asyncpg.claim()

    # Wait for the job to be ready for processing
    await asyncio.sleep(delayed_by / 1000)

    job = await rq_asyncpg.claim()
    assert job.queue == "default"
    assert job.status == rq_asyncpg.CLAIMED


def test_at_int_enqueue(rq_sqlite: Raquel):
    delayed_by = 100  # milliseconds
    at = int(datetime.now(timezone.utc).timestamp() * 1000) + delayed_by
    rq_sqlite.enqueue("default", {"foo": "bar"}, at=at)

    # Attempt to immediately dequeue the job
    assert not rq_sqlite.claim()

    # Wait for the job to be ready for processing
    time.sleep(delayed_by / 1000)

    job = rq_sqlite.claim("default")
    assert job.queue == "default"
    assert job.status == rq_sqlite.CLAIMED


@pytest.mark.asyncio
async def test_at_int_enqueue_async(rq_asyncpg: AsyncRaquel):
    delayed_by = 100
    at = int(datetime.now(timezone.utc).timestamp() * 1000) + delayed_by
    await rq_asyncpg.enqueue("default", {"foo": "bar"}, at=at)

    # Attempt to immediately dequeue the job
    assert not await rq_asyncpg.claim()

    # Wait for the job to be ready for processing
    await asyncio.sleep(delayed_by / 1000)

    job = await rq_asyncpg.claim("default")
    assert job.queue == "default"
    assert job.status == rq_asyncpg.CLAIMED


def test_delayed_int_enqueue(rq_sqlite: Raquel):
    delayed_by = 100  # milliseconds
    rq_sqlite.enqueue("default", {"foo": "bar"}, delay=delayed_by)

    # Attempt to immediately dequeue the job
    assert not rq_sqlite.claim("default")

    # Wait for the job to be ready for processing
    time.sleep(delayed_by / 1000)

    job = rq_sqlite.claim("default")
    assert job.queue == "default"
    assert job.status == rq_sqlite.CLAIMED


@pytest.mark.asyncio
async def test_delayed_int_enqueue_async(rq_asyncpg: AsyncRaquel):
    delayed_by = 100
    await rq_asyncpg.enqueue("default", {"foo": "bar"}, delay=delayed_by)

    # Attempt to immediately dequeue the job
    assert not await rq_asyncpg.claim("default")

    # Wait for the job to be ready for processing
    await asyncio.sleep(delayed_by / 1000)

    job = await rq_asyncpg.claim("default")
    assert job.queue == "default"
    assert job.status == rq_asyncpg.CLAIMED


def test_delayed_timedelta_enqueue(rq_sqlite: Raquel):
    delayed_by = timedelta(milliseconds=100)
    rq_sqlite.enqueue("default", {"foo": "bar"}, delay=delayed_by)

    # Attempt to immediately dequeue the job
    assert not rq_sqlite.claim("default")

    # Wait for the job to be ready for processing
    time.sleep(0.1)

    job = rq_sqlite.claim("default")
    assert job.queue == "default"
    assert job.status == rq_sqlite.CLAIMED


@pytest.mark.asyncio
async def test_delayed_timedelta_enqueue_async(rq_asyncpg: AsyncRaquel):
    delayed_by = timedelta(milliseconds=100)
    await rq_asyncpg.enqueue("default", {"foo": "bar"}, delay=delayed_by)

    # Attempt to immediately dequeue the job
    assert not await rq_asyncpg.claim("default")

    # Wait for the job to be ready for processing
    await asyncio.sleep(0.1)

    job = await rq_asyncpg.claim("default")
    assert job.queue == "default"
    assert job.status == rq_asyncpg.CLAIMED


def test_enqueue_in_the_past(rq_sqlite: Raquel):
    scheduled_2_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
    scheduled_3_hours_ago = datetime.now(timezone.utc) - timedelta(hours=3)
    
    # Despite being the first job enqueued, this job should be picked up last,
    # because the other two jobs have earlier scheduled times.
    job1 = rq_sqlite.enqueue(payload=1)

    # The second job should be picked up first, because it has the earliest
    # scheduled time.
    job2 = rq_sqlite.enqueue(payload=2, at=scheduled_3_hours_ago)

    # The third job should be picked up second, because it has the second
    # earliest scheduled time.
    job3 = rq_sqlite.enqueue(payload=3, at=scheduled_2_hours_ago)

    # Attempt to immediately dequeue the job. Should get Job 2.
    job_2 = rq_sqlite.claim("default")
    assert job_2.queue == job2.queue
    assert job_2.status == rq_sqlite.CLAIMED
    assert job_2.payload == job2.payload

    # Next job to be picked up should be Job 3.
    job_3 = rq_sqlite.claim("default")
    assert job_3.queue == job3.queue
    assert job_3.status == rq_sqlite.CLAIMED
    assert job_3.payload == job3.payload

    # Last job to be picked up should be Job 1.
    job_1 = rq_sqlite.claim("default")
    assert job_1.queue == job1.queue
    assert job_1.status == rq_sqlite.CLAIMED
    assert job_1.payload == job1.payload

    
@pytest.mark.asyncio
async def test_enqueue_in_the_past_async(rq_asyncpg: AsyncRaquel):
    scheduled_2_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
    scheduled_3_hours_ago = datetime.now(timezone.utc) - timedelta(hours=3)
    
    # Despite being the first job enqueued, this job should be picked up last,
    # because the other two jobs have earlier scheduled times.
    job1 = await rq_asyncpg.enqueue(payload=1)

    # The second job should be picked up first, because it has the earliest
    # scheduled time.
    job2 = await rq_asyncpg.enqueue(payload=2, at=scheduled_3_hours_ago)

    # The third job should be picked up second, because it has the second
    # earliest scheduled time.
    job3 = await rq_asyncpg.enqueue(payload=3, at=scheduled_2_hours_ago)

    # Attempt to immediately dequeue the job. Should get Job 2.
    job_2 = await rq_asyncpg.claim("default")
    assert job_2.queue == job2.queue
    assert job_2.status == rq_asyncpg.CLAIMED
    assert job_2.payload == job2.payload

    # Next job to be picked up should be Job 3.
    job_3 = await rq_asyncpg.claim("default")
    assert job_3.queue == job3.queue
    assert job_3.status == rq_asyncpg.CLAIMED
    assert job_3.payload == job3.payload

    # Last job to be picked up should be Job 1.
    job_1 = await rq_asyncpg.claim("default")
    assert job_1.queue == job1.queue
    assert job_1.status == rq_asyncpg.CLAIMED
    assert job_1.payload == job1.payload


def test_enqueue_before(rq_sqlite: Raquel):
    scheduled_2_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)

    job1 = rq_sqlite.enqueue(payload=1)
    # should be picked up first even though it is scheduled later
    job2 = rq_sqlite.enqueue(payload=2, at=scheduled_2_hours_ago) 

    hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
    job_2 = rq_sqlite.claim("default", before=hour_ago)
    assert job_2.queue == job2.queue
    assert job_2.status == rq_sqlite.CLAIMED
    assert job_2.payload == job2.payload

    job_1 = rq_sqlite.claim("default")
    assert job_1.queue == job1.queue
    assert job_1.status == rq_sqlite.CLAIMED
    assert job_1.payload == job1.payload


@pytest.mark.asyncio
async def test_enqueue_before_async(rq_asyncpg: AsyncRaquel):
    scheduled_2_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)

    job1 = await rq_asyncpg.enqueue(payload=1)
    # should be picked up first even though it is scheduled later
    job2 = await rq_asyncpg.enqueue(payload=2, at=scheduled_2_hours_ago)

    hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
    job_2 = await rq_asyncpg.claim("default", before=hour_ago)
    assert job_2.queue == job2.queue
    assert job_2.status == rq_asyncpg.CLAIMED
    assert job_2.payload == job2.payload

    job_1 = await rq_asyncpg.claim("default")
    assert job_1.queue == job1.queue
    assert job_1.status == rq_asyncpg.CLAIMED
    assert job_1.payload == job1.payload


def test_no_payload_enqueue(rq_sqlite: Raquel):
    rq_sqlite.enqueue()

    job = rq_sqlite.claim("default")
    assert job.queue == "default"
    assert job.status == rq_sqlite.CLAIMED
    assert job.payload is None


@pytest.mark.asyncio
async def test_no_payload_enqueue_async(rq_asyncpg: AsyncRaquel):
    await rq_asyncpg.enqueue()

    job = await rq_asyncpg.claim("default")
    assert job.queue == "default"
    assert job.status == rq_asyncpg.CLAIMED
    assert job.payload is None


def test_str_payload_enqueue(rq_sqlite: Raquel):
    rq_sqlite.enqueue("default", "foo")

    job = rq_sqlite.claim("default")
    assert job.queue == "default"
    assert job.status == rq_sqlite.CLAIMED
    assert job.payload == "foo"


@pytest.mark.asyncio
async def test_str_payload_enqueue_async(rq_asyncpg: AsyncRaquel):
    await rq_asyncpg.enqueue("default", "foo")

    job = await rq_asyncpg.claim("default")
    assert job.queue == "default"
    assert job.status == rq_asyncpg.CLAIMED
    assert job.payload == "foo"


def test_job_object_payload_enqueue(rq_sqlite: Raquel):
    job = Job(
        queue="job_queue",
        payload=1,
    )

    job = rq_sqlite.enqueue(payload=job)
    assert job.queue == "job_queue"
    assert job.status == "queued"
    assert job.payload == 1

    acquired_job = rq_sqlite.claim("job_queue")
    assert acquired_job.queue == "job_queue"
    assert acquired_job.status == rq_sqlite.CLAIMED
    assert acquired_job.payload == 1


@pytest.mark.asyncio
async def test_job_object_payload_enqueue_async(rq_asyncpg: AsyncRaquel):
    job = Job(
        queue="job_queue",
        payload=1,
    )

    job = await rq_asyncpg.enqueue(payload=job)
    assert job.queue == "job_queue"
    assert job.status == "queued"
    assert job.payload == 1

    acquired_job = await rq_asyncpg.claim("job_queue")
    assert acquired_job.queue == "job_queue"
    assert acquired_job.status == rq_asyncpg.CLAIMED
    assert acquired_job.payload == 1


def test_max_age_timedelta_enqueue(rq_sqlite: Raquel):
    max_age = timedelta(milliseconds=50)
    rq_sqlite.enqueue(payload={"foo": "bar"}, max_age=max_age)

    # Wait for the job to expire
    time.sleep(0.1)

    # Queue is empty
    assert not rq_sqlite.claim("default")


@pytest.mark.asyncio
async def test_max_age_timedelta_enqueue_async(rq_asyncpg: AsyncRaquel):
    max_age = timedelta(milliseconds=50)
    await rq_asyncpg.enqueue(payload={"foo": "bar"}, max_age=max_age)

    # Wait for the job to expire
    await asyncio.sleep(0.1)

    # Queue is empty
    assert not await rq_asyncpg.claim("default")


def test_max_age_int_enqueue(rq_sqlite: Raquel):
    max_age = 50
    rq_sqlite.enqueue(payload={"foo": "bar"}, max_age=max_age)

    # Wait for the job to expire
    time.sleep(0.1)

    # Queue is empty
    assert not rq_sqlite.claim("default")


@pytest.mark.asyncio
async def test_max_age_int_enqueue_async(rq_asyncpg: AsyncRaquel):
    max_age = 50
    await rq_asyncpg.enqueue(payload={"foo": "bar"}, max_age=max_age)

    # Wait for the job to expire
    await asyncio.sleep(0.1)

    # Queue is empty
    assert not await rq_asyncpg.claim("default")
