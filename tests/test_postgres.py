import asyncio
import threading
import time

import pytest

from raquel import Raquel, AsyncRaquel
from .fixtures import rq_psycopg2, rq_asyncpg


def test_concurrent_job_dequeue_in_loop(rq_psycopg2: Raquel):
    # Thread that will continuously enqueue jobs
    def enqueue_jobs(limit: int):
        for i in range(limit):
            rq_psycopg2.enqueue("concurrent_loop", {"data": i})
            time.sleep(0.05)

    # Thread that will simultaneously continuously dequeue jobs
    def dequeue_jobs(limit: int):
        for i in range(limit):
            with rq_psycopg2.dequeue("concurrent_loop") as job:
                assert job.status == rq_psycopg2.CLAIMED
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

    assert rq_psycopg2.count("concurrent_loop") == num_jobs
    assert rq_psycopg2.count("concurrent_loop", rq_psycopg2.QUEUED) == 0
    assert rq_psycopg2.count("concurrent_loop", rq_psycopg2.CLAIMED) == 0
    assert rq_psycopg2.count("concurrent_loop", rq_psycopg2.SUCCESS) == num_jobs


@pytest.mark.asyncio
async def test_concurrent_job_dequeue_in_loop_async(rq_asyncpg: AsyncRaquel):
    # Task that will continuously enqueue jobs
    async def enqueue_jobs(limit: int):
        for i in range(limit):
            await rq_asyncpg.enqueue("concurrent_loop", {"data": i})
            await asyncio.sleep(0.05)

    # Task that will simultaneously continuously dequeue jobs
    async def dequeue_jobs(limit: int):
        for i in range(limit):
            async with rq_asyncpg.dequeue("concurrent_loop") as job:
                assert job.status == rq_asyncpg.CLAIMED
                assert job.payload == {"data": i}
            await asyncio.sleep(0.05)

    num_jobs = 5
    enqueue_task = asyncio.create_task(enqueue_jobs(num_jobs))
    dequeue_task = asyncio.create_task(dequeue_jobs(num_jobs))

    await asyncio.gather(enqueue_task, dequeue_task)

    assert await rq_asyncpg.count("concurrent_loop") == num_jobs
    assert await rq_asyncpg.count("concurrent_loop", rq_asyncpg.QUEUED) == 0
    assert await rq_asyncpg.count("concurrent_loop", rq_asyncpg.CLAIMED) == 0
    assert await rq_asyncpg.count("concurrent_loop", rq_asyncpg.SUCCESS) == num_jobs


def test_continuous_dequeue_for_1_sec(rq_psycopg2: Raquel):
    def enqueue_jobs(limit: int):
        for i in range(limit):
            rq_psycopg2.enqueue("continuous_1_sec", {"data": i})
            time.sleep(0.05)

    def dequeue_jobs(limit: int):
        start_time = time.time()
        job_counter = 0
        # We are prepared to dequeue for 10 seconds but we will stop
        # if we reach the correct number of jobs.
        while time.time() - start_time < 10 and job_counter < limit:
            with rq_psycopg2.dequeue("continuous_1_sec") as job:
                if job is None:
                    continue
                assert job.status == rq_psycopg2.CLAIMED
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

    assert rq_psycopg2.count("continuous_1_sec") == num_jobs
    assert rq_psycopg2.count("continuous_1_sec", rq_psycopg2.QUEUED) == 0
    assert rq_psycopg2.count("continuous_1_sec", rq_psycopg2.CLAIMED) == 0
    assert rq_psycopg2.count("continuous_1_sec", rq_psycopg2.SUCCESS) == num_jobs


@pytest.mark.asyncio
async def test_continuous_dequeue_for_1_sec_async(rq_asyncpg: AsyncRaquel):
    async def enqueue_jobs(limit: int):
        for i in range(limit):
            await rq_asyncpg.enqueue("continuous_1_sec", {"data": i})
            await asyncio.sleep(0.05)

    async def dequeue_jobs(limit: int):
        start_time = time.time()
        job_counter = 0
        # We are prepared to dequeue for 10 seconds but we will stop
        # if we reach the correct number of jobs.
        while time.time() - start_time < 10 and job_counter < limit:
            async with rq_asyncpg.dequeue("continuous_1_sec") as job:
                if job is None:
                    continue
                assert job.status == rq_asyncpg.CLAIMED
                assert job.payload == {"data": job_counter}
            job_counter += 1
            await asyncio.sleep(0.1)

    num_jobs = 7

    # This time, start the dequeue task first. Let it run.
    dequeue_task = asyncio.create_task(dequeue_jobs(num_jobs))
    await asyncio.sleep(0.1)
    enqueue_task = asyncio.create_task(enqueue_jobs(num_jobs))
    
    await asyncio.gather(enqueue_task, dequeue_task)

    assert await rq_asyncpg.count("continuous_1_sec") == num_jobs
    assert await rq_asyncpg.count("continuous_1_sec", rq_asyncpg.QUEUED) == 0
    assert await rq_asyncpg.count("continuous_1_sec", rq_asyncpg.CLAIMED) == 0
    assert await rq_asyncpg.count("continuous_1_sec", rq_asyncpg.SUCCESS) == num_jobs
