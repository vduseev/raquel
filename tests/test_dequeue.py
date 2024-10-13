import time
from datetime import datetime, timezone

from raquel import Raquel
from .fixtures import sqlite


def test_basic_dequeue(sqlite: Raquel):
    # Perform ENQUEUE
    sqlite.enqueue({"foo": "bar"})

    # Perform DEQUEUE
    with sqlite.dequeue() as job:
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
    assert not sqlite.acquire_job("default")


def test_no_job_dequeue(sqlite: Raquel):
    # Attempt to immediately dequeue the job
    with sqlite.dequeue(queue="default") as job:
        assert job is None


def test_max_age_in_time_dequeue(sqlite: Raquel):
    # Perform ENQUEUE
    sqlite.enqueue({"foo": "bar"}, queue="default", max_age=1000)

    # Attempt to immediately dequeue the job
    with sqlite.dequeue("default") as job:
        assert job.payload == {"foo": "bar"}
        assert job.status == "locked"
