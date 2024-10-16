from contextlib import asynccontextmanager
from types import TracebackType
from typing import (
    Any,
    Coroutine,
    Iterable,
    Iterator,
    AsyncIterator,
    AsyncIterable,
    AsyncGenerator,
    Literal,
    Mapping,
    Self,
    TypeAlias,
    final,
)


_InputData: TypeAlias = str | int | float | None
_AdaptedInputData: TypeAlias = _InputData | Any
_Parameters: TypeAlias = Mapping[str, _AdaptedInputData]


@final
class _Statement: ...


class Cursor(Iterator[Any]):
    arraysize: int
    @property
    def description(self) -> tuple[tuple[str, None, None, None, None, None, None], ...] | Any: ...
    @property
    def lastrowid(self) -> int | None: ...
    @property
    def rowcount(self) -> int: ...
    def close(self) -> None: ...
    def execute(self, sql: str, parameters: _Parameters = (), /) -> Self: ...
    def executemany(self, sql: str, seq_of_parameters: Iterable[_Parameters], /) -> Self: ...
    def executescript(self, sql_script: str, /) -> Self: ...
    def fetchall(self) -> list[Any]: ...
    def fetchmany(self, size: int | None = 1) -> list[Any]: ...
    def fetchone(self) -> Any: ...
    def __iter__(self) -> Self: ...
    def __next__(self) -> Any: ...


class AsyncCursor(AsyncIterator[Any]):
    arraysize: int
    @property
    async def description(self) -> tuple[tuple[str, None, None, None, None, None, None], ...] | Any: ...
    @property
    async def lastrowid(self) -> int | None: ...
    @property
    async def rowcount(self) -> int: ...
    async def close(self) -> None: ...
    async def execute(self, sql: str, parameters: _Parameters = (), /) -> Self: ...
    async def executemany(self, sql: str, seq_of_parameters: Iterable[_Parameters], /) -> Self: ...
    async def executescript(self, sql_script: str, /) -> Self: ...
    async def fetchall(self) -> list[Any]: ...
    async def fetchmany(self, size: int | None = 1) -> list[Any]: ...
    async def fetchone(self) -> Any: ...
    async def __aiter__(self) -> Self: ...
    async def __anext__(self) -> Any: ...


class Connection:
    def close(self) -> None: ...
    def commit(self) -> None: ...
    def cursor(self, factory: None = None) -> Cursor: ...
    def execute(self, sql: str, parameters: _Parameters = ..., /) -> Cursor: ...
    def executemany(self, sql: str, parameters: Iterable[_Parameters], /) -> Cursor: ...
    def executescript(self, sql_script: str, /) -> Cursor: ...
    def rollback(self) -> None: ...

    def __call__(self, sql: str, /) -> _Statement: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self, type: type[BaseException] | None, value: BaseException | None, traceback: TracebackType | None, /
    ) -> Literal[False]: ...


class AsyncConnection:
    async def close(self) -> None: ...
    async def commit(self) -> None: ...
    async def cursor(self, factory: None = None) -> AsyncCursor: ...
    async def execute(self, sql: str, parameters: _Parameters = ..., /) -> AsyncCursor: ...
    async def executemany(self, sql: str, parameters: Iterable[_Parameters], /) -> AsyncCursor: ...
    async def executescript(self, sql_script: str, /) -> AsyncCursor: ...
    async def rollback(self) -> None: ...
    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None, None]: ...
    async def prepare(self, sql: str, /) -> _Statement: ...

    async def __call__(self, sql: str, /) -> _Statement: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self, type: type[BaseException] | None, value: BaseException | None, traceback: TracebackType | None, /
    ) -> Literal[False]: ...


class ConnectionPool:
    # SQLAlchemy (whole engine is passed)
    def connect(self) -> Connection: ...
    # mysql-connector-python
    def get_connection(self) -> Connection: ...
    # psycopg2
    def getconn(self) -> Connection: ...
    def putconn(self, connection: Connection, /) -> None: ...
    # Common
    def close(self) -> None: ...


class AsyncConnectionPool:
    # SQLAlchemy (whole engine is passed)
    @asynccontextmanager
    async def connect(self) -> AsyncGenerator[AsyncConnection, None]: ...
    # asyncpg
    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[AsyncConnection, None]: ...
