from datetime import datetime, timezone

from raquel import Raquel
from .fixtures import sqlite


def test_get_job_exists(sqlite: Raquel):
    # Perform ENQUEUE
    sqlite.enqueue({"foo": "bar"})

    # Get the job
    job = sqlite.get_job(1)
    assert job.queue == "default"
    assert job.payload == {"foo": "bar"}
    assert job.status == sqlite.QUEUED
    assert job.max_age == None
    assert job.max_retry_count == None
    assert job.enqueued_at <= datetime.now(timezone.utc)
    assert job.enqueued_at == job.scheduled_at
    assert job.attempts == 0
    assert job.locked_at == None
    assert job.finished_at == None


def test_get_job_does_not_exist(sqlite: Raquel):
    # Get the job
    job = sqlite.get_job(1)
    assert job is None


def test_job_acquire_order(sqlite: Raquel):
    # Enqueue multiple jobs
    sqlite.enqueue({"foo": 1})
    sqlite.enqueue({"foo": 2}, queue="default")
    assert sqlite.count_jobs() == 2
    assert sqlite.count_jobs("default", sqlite.QUEUED) == 2

    # Acquire the jobs
    job1 = sqlite.acquire_job("default")
    assert job1.status == sqlite.LOCKED
    assert job1.payload == {"foo": 1}
    assert sqlite.count_jobs("default") == 2
    assert sqlite.count_jobs("default", sqlite.QUEUED) == 1
    assert sqlite.count_jobs("default", sqlite.LOCKED) == 1

    job2 = sqlite.acquire_job()
    assert job2.status == sqlite.LOCKED
    assert job2.payload == {"foo": 2}
    assert sqlite.count_jobs("default") == 2
    assert sqlite.count_jobs("default", sqlite.QUEUED) == 0
    assert sqlite.count_jobs("default", sqlite.LOCKED) == 2


def test_job_dequeue_order(sqlite: Raquel):
    # Enqueue multiple jobs
    sqlite.enqueue({"foo": 1})
    sqlite.enqueue({"foo": 2})
    assert sqlite.count_jobs("default") == 2
    assert sqlite.count_jobs("default", sqlite.QUEUED) == 2

    # Dequeue the jobs
    with sqlite.dequeue("default") as job1:
        assert job1.status == sqlite.LOCKED
        assert job1.payload == {"foo": 1}
        assert sqlite.count_jobs("default") == 2
        assert sqlite.count_jobs("default", sqlite.QUEUED) == 1
        assert sqlite.count_jobs("default", sqlite.LOCKED) == 1

    with sqlite.dequeue("default") as job2:
        assert job2.status == sqlite.LOCKED
        assert job2.payload == {"foo": 2}
        assert sqlite.count_jobs("default") == 2
        assert sqlite.count_jobs("default", sqlite.QUEUED) == 0
        assert sqlite.count_jobs("default", sqlite.LOCKED) == 1
        assert sqlite.count_jobs("default", sqlite.SUCCESS) == 1

    assert sqlite.count_jobs("default") == 2
    assert sqlite.count_jobs("default", sqlite.QUEUED) == 0
    assert sqlite.count_jobs("default", sqlite.LOCKED) == 0
    assert sqlite.count_jobs("default", sqlite.SUCCESS) == 2
