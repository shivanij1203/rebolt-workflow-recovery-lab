import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

BUSY_TIMEOUT_SECONDS = 5.0


def connect(path: str | Path, schema: str) -> sqlite3.Connection:
    # isolation_level=None turns off the sqlite3 module's implicit transactions,
    # so every write path below states its own transaction boundary.
    connection = sqlite3.connect(
        str(path), timeout=BUSY_TIMEOUT_SECONDS, isolation_level=None
    )
    connection.execute("PRAGMA journal_mode = WAL")
    connection.executescript(schema)
    return connection


@contextmanager
def immediate_transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Take the write lock up front so a read-check-write sequence is atomic."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield connection
    except BaseException:
        connection.execute("ROLLBACK")
        raise
    connection.execute("COMMIT")
