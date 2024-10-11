import time

from raquel import Raquel
from .fixtures import rq


def test_min_retry_delay_dequeue(rq: Raquel):
    # Perform ENQUEUE
    rq.enqueue("default", {"foo": "bar"}, min_retry_delay=100)

    # Perform DEQUEUE
    with rq.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == "locked"
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not rq.acquire_job("default")

    # Wait for the job to be ready for processing
    time.sleep(0.2)

    # Make sure the job is back in the queue
    with rq.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == "locked"
        assert job.attempts == 1


def test_max_retry_delay_dequeue(rq: Raquel):
    # Perform ENQUEUE
    rq.enqueue(
        "default",
        {"foo": "bar"},
        min_retry_delay=100,
        max_retry_delay=100,
    )

    # Perform DEQUEUE
    with rq.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == "locked"
        assert job.attempts == 0

        # Simulate a job failure
        raise RuntimeError("Something went wrong")

    # Make sure the job is not immediately in the queue
    assert not rq.acquire_job("default")

    # Wait for the job to be ready for processing
    time.sleep(0.2)

    # Make sure the job is back in the queue
    with rq.dequeue("default") as job:
        assert job.queue == "default"
        assert job.payload == {"foo": "bar"}
        assert job.status == "locked"
        assert job.attempts == 1
