import time
from datetime import timedelta

from raquel import Raquel
from .fixtures import sqlite


def test_min_retry_delay_int_dequeue(sqlite: Raquel):
    # Perform ENQUEUE
    sqlite.enqueue({"foo": "bar"}, queue="default", min_retry_delay=100)

    # Perform DEQUEUE
    with sqlite.dequeue() as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == "locked"
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not sqlite.acquire_job()

    # Wait for the job to be ready for processing
    time.sleep(0.2)

    # Make sure the job is back in the queue
    with sqlite.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == "locked"
        assert job.attempts == 1


def test_max_retry_delay_int_dequeue(sqlite: Raquel):
    # Perform ENQUEUE
    sqlite.enqueue(
        {"foo": "bar"},
        queue="default",
        min_retry_delay=100,
        max_retry_delay=100,
    )

    # Perform DEQUEUE
    with sqlite.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == "locked"
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not sqlite.acquire_job("default")

    # Wait for the job to be ready for processing
    time.sleep(0.2)

    # Make sure the job is back in the queue
    with sqlite.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == "locked"
        assert job.attempts == 1


def test_min_max_retry_delay_timedelta_dequeue(sqlite: Raquel):
    # Perform ENQUEUE
    sqlite.enqueue(
        {"foo": "bar"},
        queue="default",
        min_retry_delay=timedelta(milliseconds=100),
        max_retry_delay=timedelta(milliseconds=100),
    )

    # Perform DEQUEUE
    with sqlite.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == "locked"
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not sqlite.acquire_job("default")

    # Wait for the job to be ready for processing
    time.sleep(0.2)

    # Make sure the job is back in the queue
    with sqlite.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == "locked"
        assert job.attempts == 1
