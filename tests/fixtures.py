import logging
import sqlite3

import psycopg2
import psycopg2.pool
import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

from raquel import Raquel


@pytest.fixture
def sqlite():
    logging.getLogger("raquel").setLevel(logging.DEBUG)

    connection = sqlite3.connect(":memory:")
    instance = Raquel(connection)
    instance.setup()
    return instance


@pytest.fixture
def postgres():
    logging.getLogger("raquel").setLevel(logging.DEBUG)

    connection = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="postgres",
    )

    instance = Raquel(connection)
    instance.setup()
    yield instance
    instance.teardown()


@pytest.fixture(scope="session")
def postgres_pool():
    logging.getLogger("raquel").setLevel(logging.DEBUG)

    pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=2,
        host="localhost",
        user="postgres",
        password="postgres",
    )

    instance = Raquel(pool)
    instance.setup()
    yield instance
    instance.teardown()


@pytest.fixture(scope="session")
def sqlalchemy_sync():
    logging.getLogger("raquel").setLevel(logging.DEBUG)

    engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost/postgres")

    instance = Raquel(engine)
    instance.setup()
    yield instance
    instance.teardown()


@pytest.fixture(scope="session")
def sqlalchemy_async():
    logging.getLogger("raquel").setLevel(logging.DEBUG)

    engine = create_async_engine("postgresql+asyncpg://postgres:postgres@localhost/postgres")

    instance = Raquel(engine)
    instance.setup()
    yield instance
    instance.teardown()
