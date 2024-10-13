# raquel

*Simple and elegant Job Queues for Python using SQL.*

Tired of complex job queues for distributed computing or event based systems?
Do you want full visibility and complete reliability of your job queue?

* **Simple**: Use existing or standalone SQL database. **Single** table.
* **Freedom**: Enqueue and dequeue whatever you want however you want. No frameworks, no restrictions.
* **Reliable**: Uses SQL transactions and handles exceptions, retries, and "at least once" execution.
* **Transparent**: Full visibility into which jobs are runnning, which failed and why, which are pending, etc.

## Installation

*Raquel has 0 dependencies. It's light as a feather, simple, pure Python.*

```bash
pip install raquel
```

## Usage

### Basic example with Postgres

On client side, schedule jobs:

```python
import psycopg2.pool
from raquel import Raquel

pool = psycopg2.pool.ThreadedConnectionPool(1, 10, host='localhost', user='postgres', password='postgres')
rq = Raquel(pool)

rq.enqueue('Hello, World!')
```

On worker side, run jobs:

```python
import time
import psycopg2.pool
from raquel import Raquel

pool = psycopg2.pool.ThreadedConnectionPool(1, 10, host='localhost', user='postgres', password='postgres')
rq = Raquel(pool)

while True:
    job = rq.dequeue()
    if job is None:
        time.sleep(1)
    print(job)
```

## Other

* [How to get DBAPI connection in SQLAlchemy](https://docs.sqlalchemy.org/en/20/faq/connections.html)
