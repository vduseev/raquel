import logging

import pytest
import pytest_asyncio

from raquel import Raquel, AsyncRaquel
from sqlalchemy import create_engine


@pytest.fixture
def rq_sqlite():
    logging.getLogger("raquel").setLevel(logging.DEBUG)

    engine = create_engine("sqlite:///:memory:")
    instance = Raquel(engine)
    instance.create_all()
    return instance


@pytest.fixture
def rq_psycopg2(postgres_dsn_sync):
    logging.getLogger("raquel").setLevel(logging.DEBUG)

    instance = Raquel(postgres_dsn_sync)
    try:
        instance.create_all()
        yield instance
    finally:
        instance.drop_all()


@pytest_asyncio.fixture
async def rq_asyncpg(postgres_dsn_async):
    logging.getLogger("raquel").setLevel(logging.DEBUG)

    instance = AsyncRaquel(postgres_dsn_async)
    try:
        await instance.create_all()
        yield instance
    finally:
        await instance.drop_all()
