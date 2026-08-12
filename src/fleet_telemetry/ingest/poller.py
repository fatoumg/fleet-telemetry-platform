"""The naive batch poller: `SELECT ... WHERE updated_at > watermark`, on a loop.

This is phase 2 step 1 and it is meant to be replaced. It works, and building it first is what
makes the CDC configuration in step 2 mean anything -- every key in that connector answers a
failure reproduced here (design spec section 13, lines 370-376).

Three failures to find by experiment, not by reading:

  1. It never sees a DELETE. A deleted row has no updated_at to exceed the watermark, and no
     query over current state can return a row that is not there.
  2. It misses a row changed twice between two polls. It reads state, not changes, so two
     commits collapse into one observation.
  3. It competes with the application for the OLTP database, on the same connection budget.

WATERMARK COLUMNS DIFFER PER TABLE, AND THAT IS NOT A DETAIL.
`pings` has no updated_at (docker/oltp/init.sql:214-224) -- it is append-only, so there is
nothing to update -- and watermarks on `server_ts` instead. The mutable tables carry updated_at
maintained by a database TRIGGER rather than by application code, deliberately: if the app
forgot to set it on one path the poller would silently skip those rows, which is a data-loss
bug that looks like nothing (docker/oltp/init.sql:28-34).
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from psycopg import Connection, connect
from psycopg.rows import dict_row

from fleet_telemetry import config
from fleet_telemetry.load import schema

# Table -> the column a watermark can be built from. depots is included even though it changes
# almost never: "changes rarely" and "never changes" differ, and only one of them is safe to
# leave out.
POLLED_TABLES: dict[str, str] = {
    "depots": "updated_at",
    "drivers": "updated_at",
    "vehicles": "updated_at",
    "jobs": "updated_at",
    "job_events": "created_at",
    "pings": "server_ts",
}


def next_watermark(rows: Sequence[dict[str, Any]], column: str, current: datetime) -> datetime:
    """The highest value seen, never lower than where we already were.

    Two guards, both earned:

    An empty result returns `current` rather than `now()`. Advancing on an empty poll is the
    classic watermark leak -- a transaction that committed with an earlier timestamp but became
    visible afterwards is then permanently behind the mark and never read.

    The result is clamped at `current` because the clock can go backwards. The profiler measured
    the container stepping back roughly 2.7 seconds every 30 under WSL2
    (docs/source-system-reference.md, section 8). With a strict `>` comparison a retreating
    watermark would skip every row committed in between.
    """
    highest = current
    for row in rows:
        value = row[column]
        if value is not None and value > highest:
            highest = value
    return highest


# Far enough back to select everything on the first run. Not datetime.min, because Postgres
# accepts it but the timestamptz range checks make the intent unreadable.
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Rows per statement. Small enough that a poll does not hold a long-running snapshot open on
# the OLTP -- which is failure 3, and the reason this whole approach loses.
#
# It bounds the STATEMENT, not the poll. See poll_once: a poll drains its tables, issuing as
# many statements as it takes. Letting the batch size bound the poll instead would quietly
# redefine what a poll means -- "everything since the watermark" would become "up to 1000
# rows", and with 172,800 pings in the baseline the poller would be permanently catching up
# while every poll still reported success. That mistake was made here first and caught by
# test_a_second_poll_with_no_changes_lands_nothing, which asserts the property the wrong
# version quietly broke.
BATCH = 1000


def _watermarks(wh_conn: Connection) -> dict[str, datetime]:
    """Where each table's poll last reached. Missing means "never polled"."""
    with wh_conn.cursor() as cur:
        cur.execute("select source_table, watermark_value from bronze.poll_watermarks")
        stored = dict(cur.fetchall())
    return {table: stored.get(table, EPOCH) for table in POLLED_TABLES}


def poll_once(oltp_conn: Connection, wh_conn: Connection) -> dict[str, int]:
    """One pass over every polled table, draining each. Returns rows landed per table.

    A poll means "everything committed since the watermark", so each table is read repeatedly
    until a statement returns less than a full batch. BATCH bounds how much is in flight at
    once; it deliberately does not bound the poll. See the comment on BATCH for the bug that
    distinction fixes.

    Strictly `>` rather than `>=`, so a row is not re-read on every poll forever. The cost is
    that two rows sharing a timestamp to the microsecond, straddling a batch boundary, lose the
    second one -- a real hazard the CDC path does not have, and worth stating rather than
    hiding behind `>=` and a deduplication step.

    Each batch commits its rows and its advanced watermark together. An interrupted poll
    therefore resumes where it stopped rather than starting the table again: the alternative,
    one transaction for the whole drain, would re-read 172,800 pings after any interruption and
    hold a snapshot open on the OLTP for the duration -- which is failure 3, made worse.
    """
    schema.apply(wh_conn)
    marks = _watermarks(wh_conn)
    landed: dict[str, int] = {}

    for table, column in POLLED_TABLES.items():
        mark = marks[table]
        total = 0

        while True:
            with oltp_conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"select * from {table} where {column} > %s order by {column} limit %s",
                    (mark, BATCH),
                )
                rows = cur.fetchall()
            # The read is finished; release the OLTP snapshot before touching the warehouse.
            oltp_conn.rollback()

            if rows:
                with wh_conn.cursor() as cur:
                    cur.executemany(
                        "insert into bronze.poll_rows "
                        "(source_table, row_image, watermark_value) values (%s, %s, %s)",
                        [(table, json.dumps(row, default=str), row[column]) for row in rows],
                    )

            advanced = next_watermark(rows, column, mark)
            with wh_conn.cursor() as cur:
                cur.execute(
                    "insert into bronze.poll_watermarks "
                    "(source_table, watermark_column, watermark_value) values (%s, %s, %s) "
                    "on conflict (source_table) do update "
                    "set watermark_value = excluded.watermark_value, updated_at = now()",
                    (table, column, advanced),
                )
            wh_conn.commit()
            total += len(rows)

            # A watermark that did not move cannot make progress, so stopping here is what
            # prevents an infinite loop when a batch is full of rows sharing one timestamp.
            # It also means those rows are the ones the strict `>` drops, above.
            if len(rows) < BATCH or advanced == mark:
                break
            mark = advanced

        landed[table] = total

    return landed


def run(interval: float, cycles: int | None) -> None:
    """Poll forever, or a fixed number of times. `cycles=None` means forever."""
    with connect(config.oltp().dsn()) as oltp_conn, connect(config.warehouse().dsn()) as wh_conn:
        done = 0
        while cycles is None or done < cycles:
            landed = poll_once(oltp_conn, wh_conn)
            total = sum(landed.values())
            detail = ", ".join(f"{t}={n}" for t, n in landed.items() if n)
            print(f"poll {done + 1}: {total} rows" + (f" ({detail})" if detail else ""))
            done += 1
            if cycles is None or done < cycles:
                time.sleep(interval)


def main() -> int:
    parser = argparse.ArgumentParser(description="The naive batch poller. Phase 2, step 1.")
    parser.add_argument("--interval", type=float, default=10.0, help="seconds between polls")
    parser.add_argument("--cycles", type=int, default=None, help="stop after N polls")
    args = parser.parse_args()
    run(args.interval, args.cycles)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
