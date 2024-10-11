import time

from raquel import Raquel
from .fixtures import rq


def test_no_job_dequeue(rq: Raquel):
    # Attempt to immediately dequeue the job
    with rq.dequeue("default") as job:
        assert job is None


def test_max_age_in_time_dequeue(rq: Raquel):
    # Perform ENQUEUE
    rq.enqueue("default", {"foo": "bar"}, max_age=1000)

    # Attempt to immediately dequeue the job
    with rq.dequeue("default") as job:
        assert job.payload == {"foo": "bar"}
        assert job.status == "locked"
