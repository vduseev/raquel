import time
from datetime import datetime, timedelta, timezone

from raquel import Raquel
from .fixtures import rq


def test_at_datetime_enqueue(rq: Raquel):
    delayed_by = 100  # milliseconds
    scheduled_at = datetime.now(timezone.utc) + timedelta(milliseconds=delayed_by)
    # Perform ENQUEUE
    rq.enqueue("default", {"foo": "bar"}, at=scheduled_at)

    # Attempt to immediately dequeue the job
    assert not rq.acquire_job("default")

    # Wait for the job to be ready for processing
    time.sleep(delayed_by / 1000)

    job = rq.acquire_job("default")
    assert job.queue == "default"
    assert job.status == "locked"


def test_at_int_enqueue(rq: Raquel):
    delayed_by = 100  # milliseconds
    at = int(datetime.now(timezone.utc).timestamp() * 1000) + delayed_by
    # Perform ENQUEUE
    rq.enqueue("default", {"foo": "bar"}, at=at)

    # Attempt to immediately dequeue the job
    assert not rq.acquire_job("default")

    # Wait for the job to be ready for processing
    time.sleep(delayed_by / 1000)

    job = rq.acquire_job("default")
    assert job.queue == "default"
    assert job.status == "locked"


def test_delayed_int_enqueue(rq: Raquel):
    delayed_by = 100  # milliseconds
    # Perform ENQUEUE
    rq.enqueue("default", {"foo": "bar"}, delay=delayed_by)

    # Attempt to immediately dequeue the job
    assert not rq.acquire_job("default")

    # Wait for the job to be ready for processing
    time.sleep(delayed_by / 1000)

    job = rq.acquire_job("default")
    assert job.queue == "default"
    assert job.status == "locked"


def test_delayed_timedelta_enqueue(rq: Raquel):
    delayed_by = timedelta(milliseconds=100)
    # Perform ENQUEUE
    rq.enqueue("default", {"foo": "bar"}, delay=delayed_by)

    # Attempt to immediately dequeue the job
    assert not rq.acquire_job("default")

    # Wait for the job to be ready for processing
    time.sleep(0.1)

    job = rq.acquire_job("default")
    assert job.queue == "default"
    assert job.status == "locked"


def test_no_payload_enqueue(rq: Raquel):
    # Perform ENQUEUE
    rq.enqueue("default")

    job = rq.acquire_job("default")
    assert job.queue == "default"
    assert job.status == "locked"
    assert job.payload is None


def test_str_payload_enqueue(rq: Raquel):
    # Perform ENQUEUE
    rq.enqueue("default", "foo")

    job = rq.acquire_job("default")
    assert job.queue == "default"
    assert job.status == "locked"
    assert job.payload == "foo"


def test_max_age_timedelta_enqueue(rq: Raquel):
    max_age = timedelta(milliseconds=50)
    # Perform ENQUEUE
    rq.enqueue("default", {"foo": "bar"}, max_age=max_age)

    # Wait for the job to expire
    time.sleep(0.1)

    # Queue is empty
    assert not rq.acquire_job("default")


def test_max_age_int_enqueue(rq: Raquel):
    max_age = 50
    # Perform ENQUEUE
    rq.enqueue("default", {"foo": "bar"}, max_age=max_age)

    # Wait for the job to expire
    time.sleep(0.1)

    # Queue is empty
    assert not rq.acquire_job("default")
