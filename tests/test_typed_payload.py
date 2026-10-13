import logging
from dataclasses import dataclass
from typing import TypedDict
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from raquel import Raquel, AsyncRaquel, StopSubscription, Job
from raquel.core.common import payload_type_of
from raquel.models.payload import load_payload
from .fixtures import rq_sqlite, rq_psycopg2, rq_asyncpg


class PydanticPayload(BaseModel):
    name: str
    tags: list[str] = []


@dataclass
class DataclassPayload:
    name: str
    count: int = 0


class ClassPayload:
    def __init__(self, name: str, count: int = 0) -> None:
        self.name = name
        self.count = count


class TypedDictPayload(TypedDict):
    name: str


def test_enqueue_objects(rq_sqlite: Raquel):
    job = rq_sqlite.enqueue("q", PydanticPayload(name="a", tags=["x"]))
    assert rq_sqlite.get(job.id).payload == {"name": "a", "tags": ["x"]}

    job = rq_sqlite.enqueue("q", DataclassPayload(name="b", count=2))
    assert rq_sqlite.get(job.id).payload == {"name": "b", "count": 2}

    job = rq_sqlite.enqueue("q", ClassPayload(name="c", count=3))
    assert rq_sqlite.get(job.id).payload == {"name": "c", "count": 3}

    # Objects nested inside regular JSON values are serialized too
    job = rq_sqlite.enqueue("q", [DataclassPayload(name="d")])
    assert rq_sqlite.get(job.id).payload == [{"name": "d", "count": 0}]


def test_enqueue_unserializable(rq_sqlite: Raquel):
    with pytest.raises(TypeError):
        rq_sqlite.enqueue("q", {1, 2})


def test_payload_type_of():
    def untyped(job): ...
    def bare(job: Job): ...
    def pydantic(job: Job[PydanticPayload]): ...
    def generic(job: Job[list[int]]): ...
    def union(job: Job[DataclassPayload | None]): ...

    assert payload_type_of(untyped) is None
    assert payload_type_of(bare) is None
    assert payload_type_of(pydantic) is PydanticPayload
    assert payload_type_of(generic) is list
    with pytest.raises(TypeError):
        payload_type_of(union)


def test_payload_type_of_unresolved_annotation(caplog):
    def worker(job: "Job[Missing]"): ...  # noqa: F821

    with caplog.at_level(logging.WARNING):
        assert payload_type_of(worker) is None
    assert "Missing" in caplog.text


def test_load_payload():
    payload = load_payload({"name": "a"}, PydanticPayload)
    assert payload == PydanticPayload(name="a")

    payload = load_payload({"name": "b"}, DataclassPayload)
    assert payload == DataclassPayload(name="b")

    payload = load_payload({"name": "c", "count": 1}, ClassPayload)
    assert isinstance(payload, ClassPayload)
    assert (payload.name, payload.count) == ("c", 1)

    # Values that are not JSON objects are passed as a single argument
    job_id = uuid4()
    assert load_payload(str(job_id), UUID) == job_id

    # Values that already have the right type are left as is
    data = {"name": "d"}
    assert load_payload(data, dict) is data
    assert load_payload(data, TypedDictPayload) is data

    with pytest.raises(ValueError):
        load_payload(None, DataclassPayload)


def test_subscribe(rq_psycopg2: Raquel):
    rq_psycopg2.enqueue("pydantic", PydanticPayload(name="a", tags=["x"]))
    rq_psycopg2.enqueue("dataclass", DataclassPayload(name="b", count=2))
    rq_psycopg2.enqueue("class", ClassPayload(name="c", count=3))
    rq_psycopg2.enqueue("untyped", {"name": "d"})
    processed = []

    @rq_psycopg2.subscribe("pydantic")
    def pydantic_worker(job: Job[PydanticPayload]):
        assert job.payload == PydanticPayload(name="a", tags=["x"])
        processed.append(job.queue)
        raise StopSubscription

    @rq_psycopg2.subscribe("dataclass")
    def dataclass_worker(job: Job[DataclassPayload]):
        assert job.payload == DataclassPayload(name="b", count=2)
        processed.append(job.queue)
        raise StopSubscription

    @rq_psycopg2.subscribe("class")
    def class_worker(job: Job[ClassPayload]):
        assert isinstance(job.payload, ClassPayload)
        assert (job.payload.name, job.payload.count) == ("c", 3)
        processed.append(job.queue)
        raise StopSubscription

    @rq_psycopg2.subscribe("untyped")
    def untyped_worker(job: Job):
        assert job.payload == {"name": "d"}
        processed.append(job.queue)
        raise StopSubscription

    rq_psycopg2.run_subscriptions()

    assert sorted(processed) == ["class", "dataclass", "pydantic", "untyped"]
    assert rq_psycopg2.count("pydantic", rq_psycopg2.SUCCESS) == 1
    assert rq_psycopg2.count("dataclass", rq_psycopg2.SUCCESS) == 1
    assert rq_psycopg2.count("class", rq_psycopg2.SUCCESS) == 1
    assert rq_psycopg2.count("untyped", rq_psycopg2.SUCCESS) == 1


@pytest.mark.asyncio
async def test_subscribe_async(rq_asyncpg: AsyncRaquel):
    await rq_asyncpg.enqueue("q", PydanticPayload(name="a", tags=["x"]))

    @rq_asyncpg.subscribe("q")
    async def worker(job: Job[PydanticPayload]):
        assert job.payload == PydanticPayload(name="a", tags=["x"])
        raise StopSubscription

    await rq_asyncpg.run_subscriptions()

    assert await rq_asyncpg.count("q", rq_asyncpg.SUCCESS) == 1


def test_subscribe_invalid_payload(rq_psycopg2: Raquel):
    enqueued_job = rq_psycopg2.enqueue("q", {"title": "No name"})

    @rq_psycopg2.subscribe("q", raise_stop_on_unhandled_exc=True)
    def worker(job: Job[PydanticPayload]):
        pytest.fail("Job with an invalid payload must not be processed")

    rq_psycopg2.run_subscriptions()

    updated_job = rq_psycopg2.get(enqueued_job.id)
    assert updated_job.status == rq_psycopg2.FAILED
    assert updated_job.attempts == 1
    assert "validation error" in updated_job.error


@pytest.mark.asyncio
async def test_subscribe_invalid_payload_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("q", {"title": "No name"})

    @rq_asyncpg.subscribe("q", raise_stop_on_unhandled_exc=True)
    async def worker(job: Job[PydanticPayload]):
        pytest.fail("Job with an invalid payload must not be processed")

    await rq_asyncpg.run_subscriptions()

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.status == rq_asyncpg.FAILED
    assert updated_job.attempts == 1
    assert "validation error" in updated_job.error


def test_dequeue(rq_psycopg2: Raquel):
    rq_psycopg2.enqueue("q", DataclassPayload(name="a", count=1))

    with rq_psycopg2.dequeue("q", payload_type=DataclassPayload) as job:
        assert job.payload == DataclassPayload(name="a", count=1)

    assert rq_psycopg2.count("q", rq_psycopg2.SUCCESS) == 1


@pytest.mark.asyncio
async def test_dequeue_async(rq_asyncpg: AsyncRaquel):
    await rq_asyncpg.enqueue("q", DataclassPayload(name="a", count=1))

    async with rq_asyncpg.dequeue("q", payload_type=DataclassPayload) as job:
        assert job.payload == DataclassPayload(name="a", count=1)

    assert await rq_asyncpg.count("q", rq_asyncpg.SUCCESS) == 1


def test_dequeue_invalid_payload(rq_psycopg2: Raquel):
    enqueued_job = rq_psycopg2.enqueue("q", {"title": "No name"})

    with rq_psycopg2.dequeue("q", payload_type=DataclassPayload) as job:
        assert job is None

    updated_job = rq_psycopg2.get(enqueued_job.id)
    assert updated_job.status == rq_psycopg2.FAILED
    assert updated_job.attempts == 1
    assert "title" in updated_job.error


@pytest.mark.asyncio
async def test_dequeue_invalid_payload_async(rq_asyncpg: AsyncRaquel):
    enqueued_job = await rq_asyncpg.enqueue("q", {"title": "No name"})

    async with rq_asyncpg.dequeue("q", payload_type=DataclassPayload) as job:
        assert job is None

    updated_job = await rq_asyncpg.get(enqueued_job.id)
    assert updated_job.status == rq_asyncpg.FAILED
    assert updated_job.attempts == 1
    assert "title" in updated_job.error
