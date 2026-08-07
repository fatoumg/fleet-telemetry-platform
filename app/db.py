"""Connection pooling for the OLTP database.

One pool for the whole process, opened at startup and closed at shutdown.

Why a pool at all: opening a Postgres connection costs a TCP handshake, authentication and a
backend process fork -- a few milliseconds. Irrelevant once, ruinous at a thousand requests a
second. The pool keeps a handful of connections open and lends them out.

Why it matters for the *data*, not just for speed: the simulator posts pings in batches from
many concurrent workers. Without a bounded pool, a burst would open a connection per request
and hit Postgres's `max_connections` (100 by default), at which point requests fail with a
confusing error that looks like a database outage rather than a client-side capacity problem.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from fleet_telemetry import config

_pool: ConnectionPool | None = None


def open_pool() -> ConnectionPool:
    """Create the process-wide pool. Called once, from the app's lifespan startup."""
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            conninfo=config.oltp().dsn(),
            min_size=1,
            max_size=10,
            # Fail fast rather than hanging forever if the database is down. A request that
            # blocks indefinitely is far harder to diagnose than one that errors in 5 seconds.
            timeout=5.0,
            # Validate a connection before lending it out.
            #
            # A pooled connection is a TCP socket held open for minutes. If the database
            # restarts -- which it does every time the container is recreated -- every socket
            # in the pool is dead, but the pool does not know that until something tries to use
            # one. Without this check the next request fails with a confusing error against a
            # database that is demonstrably up and healthy.
            #
            # That happened during phase 1: the OLTP volume was rebuilt, and /health reported
            # "unreachable" while `psql` connected fine one second later. The cost is one cheap
            # round trip per checkout, which is worth paying to avoid that hour.
            check=ConnectionPool.check_connection,
            open=True,
        )
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


@contextmanager
def connection() -> Iterator[Connection]:
    """Borrow a connection, returning dict rows.

    psycopg commits on clean exit of the context manager and rolls back on an exception, so
    every request is one transaction and a failed request leaves nothing half-written.
    """
    if _pool is None:
        raise RuntimeError("connection pool is not open; call open_pool() first")
    with _pool.connection() as conn:
        conn.row_factory = dict_row
        yield conn
