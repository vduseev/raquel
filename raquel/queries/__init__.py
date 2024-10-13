import enum

from .sqlite import SQLiteQueries
from .postgres import PostgresQueries


class Dialect(str, enum.Enum):
    SQLITE = "sqlite"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    ORACLE = "oracle"
    MSSQL = "mssql"
    DB2 = "db2"


__all__ = [
    "SQLiteQueries",
    "PostgresQueries",
    "Dialect",
]
