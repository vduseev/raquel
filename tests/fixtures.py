import logging

import pytest
import pytest_asyncio

from raquel import Raquel, AsyncRaquel


@pytest.fixture
def simple():
    logging.getLogger("raquel").setLevel(logging.DEBUG)

    instance = Raquel("sqlite://")
    instance.create_all()
    return instance


@pytest.fixture
def normal():
    logging.getLogger("raquel").setLevel(logging.DEBUG)

    instance = Raquel("postgresql+psycopg2://postgres:postgres@localhost:6432/postgres")
    try:
        instance.create_all()
        yield instance
    finally:
        instance.drop_all()


@pytest_asyncio.fixture
async def asynchronous():
    logging.getLogger("raquel").setLevel(logging.DEBUG)

    instance = AsyncRaquel("postgresql+asyncpg://postgres:postgres@localhost:6432/postgres")
    try:
        await instance.create_all()
        yield instance
    finally:
        await instance.drop_all()
