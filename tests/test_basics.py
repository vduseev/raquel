from datetime import datetime, timezone

from raquel import Raquel
from .fixtures import rq


def test_basic_enqueue(rq: Raquel):
    # Make sure the queue doesn't exist
    assert not rq.list_queues()

    # Perform ENQUEUE
    rq.enqueue("default", {"foo": "bar"})

    # Make sure the queue exists
    assert "default" in rq.list_queues().keys()

    # Make sure the job exists
    jobs = rq.list_jobs("default")
    assert len(jobs) == 1
    job = jobs[0]
    assert job.queue == "default"
    assert job.payload == {"foo": "bar"}
    assert job.status == "queued"
    assert job.max_age == None
    assert job.max_retry_count == None
    assert job.enqueued_at <= datetime.now(timezone.utc)
    assert job.enqueued_at == job.scheduled_at
    assert job.attempts == 0
    assert job.locked_at == None
    assert job.finished_at == None


def test_basic_dequeue(rq: Raquel):
    # Perform ENQUEUE
    rq.enqueue("default", {"foo": "bar"})

    # Perform DEQUEUE
    with rq.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == "locked"
        assert job.max_age == None
        assert job.max_retry_count == None
        assert job.enqueued_at <= datetime.now(timezone.utc)
        assert job.enqueued_at == job.scheduled_at
        assert job.attempts == 0
        assert job.locked_at <= datetime.now(timezone.utc)
        assert job.finished_at == None

    # Make sure the job is not in the queue
    assert not rq.acquire_job("default")


def test_get_job_exists(rq: Raquel):
    # Perform ENQUEUE
    rq.enqueue("default", {"foo": "bar"})

    # Get the job
    job = rq.get_job(1)
    assert job.queue == "default"
    assert job.payload == {"foo": "bar"}
    assert job.status == "queued"
    assert job.max_age == None
    assert job.max_retry_count == None
    assert job.enqueued_at <= datetime.now(timezone.utc)
    assert job.enqueued_at == job.scheduled_at
    assert job.attempts == 0
    assert job.locked_at == None
    assert job.finished_at == None


def test_get_job_does_not_exist(rq: Raquel):
    # Get the job
    job = rq.get_job(1)
    assert job is None
