"""Insert decoded messages into bronze. The only thing that writes bronze.raw_*.

Not all of bronze: ingest/poller.py writes bronze.poll_rows and bronze.poll_watermarks directly,
because a polled row has no Kafka coordinate and so cannot be a BronzeRow. Routing it through here
would mean inventing the one field this module deduplicates on -- the same argument that keeps `op`
and `before` out of poll_rows.

No parsing, no typing, no deduplication of business keys -- all of that is dbt's
(src/fleet_telemetry/load/__init__.py:3-4). What happens here is one INSERT per bronze table
per batch, and a count of how many rows the unique index refused.

THAT COUNT IS THE INTERESTING OUTPUT. The consumer commits its Kafka offsets only after the
Postgres transaction commits, so a crash in between replays messages that already landed. The
unique index on the Kafka coordinate absorbs them, and `suppressed` is how many. Zero forever
means nothing has crashed yet; a jump after a restart is the delivery guarantee doing exactly
what it says.

`executemany` rather than COPY: COPY is faster and cannot do ON CONFLICT, and correctness under
replay is worth more here than throughput at 8 events a second
(docs/source-system-reference.md, continuous mode). If the load ever justifies it, the shape is
COPY into an UNLOGGED staging table then INSERT ... SELECT ... ON CONFLICT -- measure first.
"""

from __future__ import annotations

from collections import defaultdict
from typing import NamedTuple

from psycopg import Connection


class BronzeRow(NamedTuple):
    """One message, already decoded, addressed to one bronze table."""

    table: str
    topic: str
    partition: int
    offset: int
    timestamp: int | None
    payload: str | None
    raw_payload: str | None
    parse_error: str | None


# raw_cdc_entities carries four topics, so its uniqueness includes the topic. The other two
# carry one each, where (partition, offset) is already unique and the narrower index is cheaper
# on a table headed for ~22M rows.
_CONFLICT_TARGET = {
    "raw_ping_events": "(_kafka_partition, _kafka_offset)",
    "raw_job_events": "(_kafka_partition, _kafka_offset)",
    "raw_cdc_entities": "(_topic, _kafka_partition, _kafka_offset)",
}


def write(conn: Connection, rows: list[BronzeRow]) -> tuple[int, int]:
    """Insert a heterogeneous batch. Returns (inserted, suppressed).

    Does NOT commit. The caller owns the transaction boundary, because the whole restart
    guarantee depends on the Kafka offset commit happening strictly after the database one.
    """
    if not rows:
        return 0, 0

    by_table: dict[str, list[BronzeRow]] = defaultdict(list)
    for row in rows:
        by_table[row.table].append(row)

    inserted = 0
    with conn.cursor() as cur:
        for table, batch in by_table.items():
            cur.executemany(
                f"insert into bronze.{table} "
                "(_topic, _kafka_partition, _kafka_offset, _kafka_timestamp, "
                " payload, raw_payload, parse_error) "
                "values (%s, %s, %s, %s, %s, %s, %s) "
                f"on conflict {_CONFLICT_TARGET[table]} do nothing",
                [
                    (
                        row.topic,
                        row.partition,
                        row.offset,
                        row.timestamp,
                        row.payload,
                        row.raw_payload,
                        row.parse_error,
                    )
                    for row in batch
                ],
            )
            # psycopg 3 accumulates rowcount across an executemany, and ON CONFLICT DO NOTHING
            # reports only the rows that actually landed.
            inserted += cur.rowcount

    return inserted, len(rows) - inserted
