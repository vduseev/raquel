import logging
import sqlite3

import pytest

from raquel import Raquel


@pytest.fixture
def rq():
    logging.getLogger("raquel").setLevel(logging.DEBUG)

    connection = sqlite3.connect(":memory:")
    instance = Raquel(connection)
    instance.setup()
    return instance
