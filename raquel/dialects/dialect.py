import enum


class Dialect(str, enum.Enum):
    SQLITE3 = "sqlite3"
    PSYCOPG2 = "psycopg2"
    ASYNCPG = "asyncpg"
    ORACLEDB = "oracledb"
