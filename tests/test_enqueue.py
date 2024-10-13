import time
from datetime import datetime, timedelta, timezone

from raquel import Raquel, Job
from .fixtures import sqlite


def test_basic_enqueue(sqlite: Raquel):
    # Make sure the queue doesn't exist
    assert not sqlite.list_queues()

    # Perform ENQUEUE
    sqlite.enqueue({"foo": "bar"}, queue="foo")

    # Make sure the queue exists
    assert "foo" in sqlite.list_queues().keys()
    assert "default" not in sqlite.list_queues().keys()

    # Make sure the job exists
    jobs = sqlite.list_jobs("foo")
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
    assert job.locked_at == None
    assert job.finished_at == None


def test_at_datetime_enqueue(sqlite: Raquel):
    delayed_by = 100  # milliseconds
    scheduled_at = datetime.now(timezone.utc) + timedelta(milliseconds=delayed_by)
    # Perform ENQUEUE
    sqlite.enqueue({"foo": "bar"}, at=scheduled_at)

    # Attempt to immediately dequeue the job
    assert not sqlite.acquire_job()

    # Wait for the job to be ready for processing
    time.sleep(delayed_by / 1000)

    job = sqlite.acquire_job()
    assert job.queue == "default"
    assert job.status == "locked"


def test_at_int_enqueue(sqlite: Raquel):
    delayed_by = 100  # milliseconds
    at = int(datetime.now(timezone.utc).timestamp() * 1000) + delayed_by
    # Perform ENQUEUE
    sqlite.enqueue({"foo": "bar"}, at=at)

    # Attempt to immediately dequeue the job
    assert not sqlite.acquire_job()

    # Wait for the job to be ready for processing
    time.sleep(delayed_by / 1000)

    job = sqlite.acquire_job("default")
    assert job.queue == "default"
    assert job.status == "locked"


def test_delayed_int_enqueue(sqlite: Raquel):
    delayed_by = 100  # milliseconds
    # Perform ENQUEUE
    sqlite.enqueue({"foo": "bar"}, queue="default", delay=delayed_by)

    # Attempt to immediately dequeue the job
    assert not sqlite.acquire_job("default")

    # Wait for the job to be ready for processing
    time.sleep(delayed_by / 1000)

    job = sqlite.acquire_job("default")
    assert job.queue == "default"
    assert job.status == "locked"


def test_delayed_timedelta_enqueue(sqlite: Raquel):
    delayed_by = timedelta(milliseconds=100)
    # Perform ENQUEUE
    sqlite.enqueue({"foo": "bar"}, delay=delayed_by)

    # Attempt to immediately dequeue the job
    assert not sqlite.acquire_job("default")

    # Wait for the job to be ready for processing
    time.sleep(0.1)

    job = sqlite.acquire_job("default")
    assert job.queue == "default"
    assert job.status == "locked"


def test_no_payload_enqueue(sqlite: Raquel):
    # Perform ENQUEUE
    sqlite.enqueue()

    job = sqlite.acquire_job("default")
    assert job.queue == "default"
    assert job.status == "locked"
    assert job.payload is None


def test_str_payload_enqueue(sqlite: Raquel):
    # Perform ENQUEUE
    sqlite.enqueue("foo", "default")

    job = sqlite.acquire_job("default")
    assert job.queue == "default"
    assert job.status == "locked"
    assert job.payload == "foo"


def test_job_payload_enqueue(sqlite: Raquel):
    job = Job(
        queue="job_queue",
        payload=1,
    )

    # Perform ENQUEUE
    job = sqlite.enqueue(job)
    assert job.queue == "job_queue"
    assert job.status == "queued"
    assert job.payload == 1

    acquired_job = sqlite.acquire_job("job_queue")
    assert acquired_job.queue == "job_queue"
    assert acquired_job.status == "locked"
    assert acquired_job.payload == 1


def test_max_age_timedelta_enqueue(sqlite: Raquel):
    max_age = timedelta(milliseconds=50)
    # Perform ENQUEUE
    sqlite.enqueue({"foo": "bar"}, max_age=max_age)

    # Wait for the job to expire
    time.sleep(0.1)

    # Queue is empty
    assert not sqlite.acquire_job("default")


def test_max_age_int_enqueue(sqlite: Raquel):
    max_age = 50
    # Perform ENQUEUE
    sqlite.enqueue({"foo": "bar"}, max_age=max_age)

    # Wait for the job to expire
    time.sleep(0.1)

    # Queue is empty
    assert not sqlite.acquire_job("default")
