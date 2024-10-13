import threading
import time

from raquel import Raquel
from .fixtures import postgres, postgres_pool


def test_concurrent_job_dequeue_in_loop(postgres_pool: Raquel):
    def enqueue_jobs(limit: int):
        for i in range(limit):
            postgres_pool.enqueue({"data": i}, queue="concurrent_loop")
            print(f"Enqueued job {i}")
            time.sleep(0.05)

    def dequeue_jobs(limit: int):
        for i in range(limit):
            print(f"Dequeueing job {i}")
            with postgres_pool.dequeue("concurrent_loop") as job:
                print(f"Dequeued job {i}: {job}")
                assert job.status == "locked"
                assert job.payload == {"data": i}
            time.sleep(0.05)

    num_jobs = 10
    enqueue_thread = threading.Thread(target=enqueue_jobs, args=(num_jobs,))
    dequeue_thread = threading.Thread(target=dequeue_jobs, args=(num_jobs,))

    # First, start the enqueue thread
    enqueue_thread.start()
    # Wait a bit
    time.sleep(0.1)
    dequeue_thread.start()
    enqueue_thread.join()
    dequeue_thread.join()

    assert postgres_pool.count_jobs("concurrent_loop") == num_jobs
    assert postgres_pool.count_jobs("concurrent_loop", "queued") == 0
    assert postgres_pool.count_jobs("concurrent_loop", "locked") == 0
    assert postgres_pool.count_jobs("concurrent_loop", "success") == num_jobs


def test_continuous_dequeue_for_1_sec(postgres_pool: Raquel):
    def enqueue_jobs(limit: int):
        for i in range(limit):
            postgres_pool.enqueue({"data": i}, "continuous_1_sec")
            print(f"Enqueued job {i}")
            time.sleep(0.05)

    def dequeue_jobs():
        start_time = time.time()
        job_counter = 0
        while time.time() - start_time < 1:
            with postgres_pool.dequeue("continuous_1_sec") as job:
                if job is None:
                    continue
                print(f"Dequeued job: {job}")
                assert job.status == "locked"
                assert job.payload == {"data": job_counter}
            job_counter += 1
            time.sleep(0.1)

    # You can dequeue 10 jobs in 1 second with 0.1 sleep time
    # if each job takes close to nothing to process.

    num_jobs = 7
    enqueue_thread = threading.Thread(target=enqueue_jobs, args=(num_jobs,))
    dequeue_thread = threading.Thread(target=dequeue_jobs)

    # This time, start the dequeue thread first. Let it run.
    dequeue_thread.start()
    time.sleep(0.1)
    enqueue_thread.start()
    enqueue_thread.join()
    dequeue_thread.join()

    assert postgres_pool.count_jobs("continuous_1_sec") == num_jobs
    assert postgres_pool.count_jobs("continuous_1_sec", "queued") == 0
    assert postgres_pool.count_jobs("continuous_1_sec", "locked") == 0
    assert postgres_pool.count_jobs("continuous_1_sec", "success") == num_jobs
