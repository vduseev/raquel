import pytest


def pytest_addoption(parser):
    """Add command-line options for configuring database connections."""
    parser.addoption(
        "--postgres-host",
        action="store",
        default="localhost",
        help="PostgreSQL host (default: localhost)"
    )
    parser.addoption(
        "--postgres-port",
        action="store",
        default="5432",
        help="PostgreSQL port (default: 5432)"
    )
    parser.addoption(
        "--postgres-user",
        action="store",
        default="postgres",
        help="PostgreSQL username (default: postgres)"
    )
    parser.addoption(
        "--postgres-password",
        action="store",
        default="postgres",
        help="PostgreSQL password (default: postgres)"
    )
    parser.addoption(
        "--postgres-database",
        action="store",
        default="postgres",
        help="PostgreSQL database name (default: postgres)"
    )


@pytest.fixture(scope="session")
def postgres_config(request) -> dict[str, str]:
    """Provide PostgreSQL connection configuration from command-line options."""
    return {
        "host": request.config.getoption("--postgres-host"),
        "port": request.config.getoption("--postgres-port"),
        "user": request.config.getoption("--postgres-user"),
        "password": request.config.getoption("--postgres-password"),
        "database": request.config.getoption("--postgres-database"),
    } 


@pytest.fixture(scope="session")
def postgres_dsn_sync(postgres_config) -> str:
    """Generate PostgreSQL DSN for synchronous connections (psycopg2)."""
    return (
        f"postgresql+psycopg2://{postgres_config['user']}:{postgres_config['password']}"
        f"@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
    )


@pytest.fixture(scope="session")
def postgres_dsn_async(postgres_config) -> str:
    """Generate PostgreSQL DSN for asynchronous connections (asyncpg)."""
    return (
        f"postgresql+asyncpg://{postgres_config['user']}:{postgres_config['password']}"
        f"@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
    )
