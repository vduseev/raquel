import asyncio
import threading
import time

import pytest

from raquel import Raquel, AsyncRaquel
from .fixtures import normal, asynchronous


def test_concurrent_job_dequeue_in_loop(normal: Raquel):
    def enqueue_jobs(limit: int):
        for i in range(limit):
            normal.enqueue("concurrent_loop", {"data": i})
            print(f"Enqueued job {i}")
            time.sleep(0.05)

    def dequeue_jobs(limit: int):
        for i in range(limit):
            print(f"Dequeueing job {i}")
            with normal.dequeue("concurrent_loop") as job:
                print(f"Dequeued job {i}: {job}")
                assert job.status == normal.CLAIMED
                assert job.payload == {"data": i}
            time.sleep(0.05)

    num_jobs = 5
    enqueue_thread = threading.Thread(target=enqueue_jobs, args=(num_jobs,))
    dequeue_thread = threading.Thread(target=dequeue_jobs, args=(num_jobs,))

    # First, start the enqueue thread
    enqueue_thread.start()
    # Wait a bit
    time.sleep(0.1)
    dequeue_thread.start()
    enqueue_thread.join()
    dequeue_thread.join()

    assert normal.count("concurrent_loop") == num_jobs
    assert normal.count("concurrent_loop", normal.QUEUED) == 0
    assert normal.count("concurrent_loop", normal.CLAIMED) == 0
    assert normal.count("concurrent_loop", normal.SUCCESS) == num_jobs


@pytest.mark.asyncio
async def test_concurrent_job_dequeue_in_loop_async(asynchronous: AsyncRaquel):
    async def enqueue_jobs(limit: int):
        for i in range(limit):
            await asynchronous.enqueue("concurrent_loop", {"data": i})
            print(f"Enqueued job {i}")
            await asyncio.sleep(0.05)

    async def dequeue_jobs(limit: int):
        for i in range(limit):
            print(f"Dequeueing job {i}")
            async with asynchronous.dequeue("concurrent_loop") as job:
                print(f"Dequeued job {i}: {job}")
                assert job.status == asynchronous.CLAIMED
                assert job.payload == {"data": i}
            await asyncio.sleep(0.05)

    num_jobs = 5
    enqueue_task = asyncio.create_task(enqueue_jobs(num_jobs))
    dequeue_task = asyncio.create_task(dequeue_jobs(num_jobs))

    await asyncio.gather(enqueue_task, dequeue_task)

    assert await asynchronous.count("concurrent_loop") == num_jobs
    assert await asynchronous.count("concurrent_loop", asynchronous.QUEUED) == 0
    assert await asynchronous.count("concurrent_loop", asynchronous.CLAIMED) == 0
    assert await asynchronous.count("concurrent_loop", asynchronous.SUCCESS) == num_jobs


def test_continuous_dequeue_for_1_sec(normal: Raquel):
    def enqueue_jobs(limit: int):
        for i in range(limit):
            normal.enqueue("continuous_1_sec", {"data": i})
            print(f"Enqueued job {i}")
            time.sleep(0.05)

    def dequeue_jobs(limit: int):
        start_time = time.time()
        job_counter = 0
        while time.time() - start_time < 10 and job_counter < limit:
            with normal.dequeue("continuous_1_sec") as job:
                if job is None:
                    continue
                print(f"Dequeued job: {job}")
                assert job.status == normal.CLAIMED
                assert job.payload == {"data": job_counter}
            job_counter += 1
            time.sleep(0.1)

    # You can dequeue 10 jobs in 1 second with 0.1 sleep time
    # if each job takes close to nothing to process.

    num_jobs = 7
    enqueue_thread = threading.Thread(target=enqueue_jobs, args=(num_jobs,))
    dequeue_thread = threading.Thread(target=dequeue_jobs, args=(num_jobs,))

    # This time, start the dequeue thread first. Let it run.
    dequeue_thread.start()
    time.sleep(0.1)
    enqueue_thread.start()
    enqueue_thread.join()
    dequeue_thread.join()

    assert normal.count("continuous_1_sec") == num_jobs
    assert normal.count("continuous_1_sec", normal.QUEUED) == 0
    assert normal.count("continuous_1_sec", normal.CLAIMED) == 0
    assert normal.count("continuous_1_sec", normal.SUCCESS) == num_jobs


@pytest.mark.asyncio
async def test_continuous_dequeue_for_1_sec_async(asynchronous: AsyncRaquel):
    async def enqueue_jobs(limit: int):
        for i in range(limit):
            await asynchronous.enqueue("continuous_1_sec", {"data": i})
            print(f"Enqueued job {i}")
            await asyncio.sleep(0.05)

    async def dequeue_jobs(limit: int):
        start_time = time.time()
        job_counter = 0
        while time.time() - start_time < 10 and job_counter < limit:
            async with asynchronous.dequeue("continuous_1_sec") as job:
                if job is None:
                    continue
                print(f"Dequeued job: {job}")
                assert job.status == asynchronous.CLAIMED
                assert job.payload == {"data": job_counter}
            job_counter += 1
            await asyncio.sleep(0.1)

    num_jobs = 7

    # This time, start the dequeue task first. Let it run.
    dequeue_task = asyncio.create_task(dequeue_jobs(num_jobs))
    await asyncio.sleep(0.1)
    enqueue_task = asyncio.create_task(enqueue_jobs(num_jobs))
    
    await asyncio.gather(enqueue_task, dequeue_task)

    assert await asynchronous.count("continuous_1_sec") == num_jobs
    assert await asynchronous.count("continuous_1_sec", asynchronous.QUEUED) == 0
    assert await asynchronous.count("continuous_1_sec", asynchronous.CLAIMED) == 0
    assert await asynchronous.count("continuous_1_sec", asynchronous.SUCCESS) == num_jobs
