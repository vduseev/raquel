# Contributing

Table of contents:

- [Opening issues](#opening-issues)
  - [Bug reports](#bug-reports)
  - [Feature requests](#feature-requests)
- [Accepting new changes](#accepting-new-changes)
- [New releases](#new-releases)
- [Development environment](#development-environment)
  - [Prepare](#prepare)
  - [Install everything with uv](#install-everything-with-uv)
  - [Using Nix](#using-nix)
- [Testing](#testing)
  - [Prerequisites](#prerequisites)
  - [Running tests](#running-tests)
  - [Configure PostgreSQL connection](#configure-postgresql-connection)

## Opening issues

### Bug reports

- Use the [issue template](https://github.com/vduseev/raquel/issues/new/choose) when creating an issue.
- Provide a minimal reproducible example.
- Provide the full traceback.
- Provide the output of `uv venv --python` and `uv pip freeze`.

### Feature requests

- Describe the exact use case.
- Describe the input and output for the potential test case.
- Optionally, describe how you envision the feature to be implemented.

## Accepting new changes

### Pull requests

1. Fork the repository.
1. Make your changes in your fork.
1. Run all tests and make sure they pass.
1. Push your commits to your fork.
1. Create a pull request from your fork to the original repository. Choose the
   `main` branch as the target branch or a specific branch you want to merge
   your changes into.
1. Get an approval from the maintainer.
1. Your changes will be merged into the original repository.
1. Once the maintainer collects all the changes, they will release a new version
   of the library.

### Code style

We use [ruff](https://github.com/astral-sh/ruff) for code style and linting.

## New releases

Here are the steps to publish a new version of the library:

1. Make sure everything you want to release is merged into the `main` branch.
1. Sync the dependencies with `uv sync --all-extras`.
1. Run all tests and make sure they pass.
1. Bump the version using `uv run bump-my-version` and make sure the bump
   reflects the scope of changes:
   - `patch` for small changes, might be unnoticeable to users
   - `minor` for new features, might be noticeable to users
   - `major` for breaking changes or complete overhauls
1. Push new commit and tag created by `bump-my-version` to GitHub.
1. Build the package with `uv run build`.
1. Publish the package to PyPI using `uv run publish`.

## Development environment

### Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)
- [PostgreSQL](https://www.postgresql.org/download/) (standalone or container)

### Install everything with uv

```bash
uv sync --all-extras
```

### Using Nix

If you are using Nix, you can use the provided `shell.nix` file to enter a
shell with all the dependencies installed. That includes uv, PostgreSQL, and
will automatically set up the virtual environment.

```bash
nix-shell
```

## Testing

### Prepare

- You need to have an active PostgreSQL instance running on `localhost:5432`.
- Make sure to install everything in optional dependencies as well as extras.
  `uv sync --all-extras` does this for you.

When tests are running, some of the are using a temporary SQLite database.
But around half of the tests interact with the real PostgreSQL instance.

There is a ready to use Docker Compose file in the `tests` directory.
It will start a PostgreSQL instance on `localhost:5432`:

```bash
$ docker compose -f tests/docker-compose.yaml up -d
[+] Running 2/2
 ✔ Network tests_default      Created  0.0s
 ✔ Container tests-postgres-1 Started  0.0s
```

Or by manually creating a container using docker or podman:

```bash
$ docker run -d --name raquel-postgres -p 5432:5432 \
-e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
-e POSTGRES_DB=postgres postgres:latest
```

After the tests are done, you can bring the PostgreSQL container down with:

```bash
$ docker compose -f tests/docker-compose.yaml down
[+] Running 2/2
 ✔ Container tests-postgres-1 Stopped  0.0s
 ✔ Network tests_default      Removed  0.0s
```

If you start the container manually, remove it with:

```bash
$ docker rm -f raquel-postgres
raquel-postgres
```

### Running tests

*⚠️ Note: If you have not activated the virtual environment, you need to
activate it first or run the tests with `uv run pytest -x`.*

```bash
$ pytest -x
============================ test session starts ===========================
platform darwin -- Python 3.13.2, pytest-8.3.5, pluggy-1.6.0
rootdir: /Users/vduseev/Projects/vduseev/raquel
configfile: pyproject.toml
plugins: asyncio-0.24.0, cov-5.0.0
asyncio: mode=Mode.AUTO, default_loop_scope=function
collected 78 items         

tests/test_basics.py ..........                                       [ 12%]
tests/test_conflict.py ....                                           [ 17%]
tests/test_dequeue.py ..................                              [ 41%]
tests/test_enqueue.py ........................                        [ 71%]
tests/test_postgres.py ....                                           [ 76%]
tests/test_retry.py ......                                            [ 84%]
tests/test_subscribe.py ............                                  [100%]

============================ 78 passed in 9.37s ============================
```

### Configure PostgreSQL connection

If your PostgreSQL instance is running on a different host, port, or with different credentials, you can configure the connection using command-line options:

```bash
# Run tests with a custom PostgreSQL port
pytest --postgres-port=5433

# Run tests with custom host and credentials
pytest --postgres-host=db.example.com --postgres-user=testuser --postgres-password=testpass

# Run tests with a custom database
pytest --postgres-database=test_db
```

Available PostgreSQL configuration options:

- `--postgres-host` (default: localhost)
- `--postgres-port` (default: 5432)
- `--postgres-user` (default: postgres)
- `--postgres-password` (default: postgres)
- `--postgres-database` (default: postgres)
