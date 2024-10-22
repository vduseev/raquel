import asyncio
import threading
import time

import pytest

from raquel import Raquel, AsyncRaquel
from .fixtures import normal, asynchronous


def test_conflict_cancel(normal: Raquel):
    job = normal.enqueue(1, queue="conflict_cancel")

    def dequeue_job():
        print(f"Dequeueing job...")
        with normal.dequeue("conflict_cancel") as job:
            print(f"Dequeued job: {job}")
            assert job.status == normal.CLAIMED
            assert job.payload == 1
            time.sleep(0.1)
            print("Job processed")

    # Cancel will not do anything, since the job is already being processed.
    def cancel_job():
        print(f"Cancelling job...")
        assert normal.cancel(job.id) is False
        print("Job cancelled")

    dequeue_thread = threading.Thread(target=dequeue_job)
    cancel_thread = threading.Thread(target=cancel_job)

    # First, start the dequeue thread
    dequeue_thread.start()
    # Wait a bit
    time.sleep(0.05)
    cancel_thread.start()
    dequeue_thread.join()
    cancel_thread.join()

    updated_job = normal.get(job.id)
    assert updated_job.status == normal.SUCCESS


@pytest.mark.asyncio
async def test_conflict_cancel_async(asynchronous: AsyncRaquel):
    job = await asynchronous.enqueue(1, queue="conflict_cancel")

    async def dequeue_job():
        print(f"Dequeueing job...")
        async with asynchronous.dequeue("conflict_cancel") as job:
            print(f"Dequeued job: {job}")
            assert job.status == asynchronous.CLAIMED
            assert job.payload == 1
            await asyncio.sleep(0.1)
            print("Job processed")

    # Cancel will not do anything, since the job is already being processed.
    async def cancel_job():
        print(f"Cancelling job...")
        assert await asynchronous.cancel(job.id) is False
        print("Job cancelled")

    dequeue_task = asyncio.create_task(dequeue_job())
    cancel_task = asyncio.create_task(cancel_job())
    await asyncio.gather(dequeue_task, cancel_task)

    updated_job = await asynchronous.get(job.id)
    assert updated_job.status == asynchronous.SUCCESS


def test_nested_cancel(normal: Raquel):
    job = normal.enqueue(1, queue="nested_cancel")

    print(f"Dequeueing job...")
    with normal.dequeue("nested_cancel") as job:
        print(f"Dequeued job: {job}")
        assert job.status == normal.CLAIMED
        assert job.payload == 1
        assert normal.cancel(job.id) is False
        print("Job processed")

    updated_job = normal.get(job.id)
    assert updated_job.status == normal.SUCCESS


@pytest.mark.asyncio
async def test_nested_cancel_async(asynchronous: AsyncRaquel):
    job = await asynchronous.enqueue(1, queue="nested_cancel")

    print(f"Dequeueing job...")
    async with asynchronous.dequeue("nested_cancel") as job:
        print(f"Dequeued job: {job}")
        assert job.status == asynchronous.CLAIMED
        assert job.payload == 1
        assert await asynchronous.cancel(job.id) is False
        print("Job processed")

    updated_job = await asynchronous.get(job.id)
    assert updated_job.status == asynchronous.SUCCESS
