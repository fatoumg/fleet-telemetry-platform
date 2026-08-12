"""Read the change topics and land every message in bronze. Append-only, restart-safe.

THE ORDER OF THE TWO COMMITS IS THE ENTIRE DESIGN.

    1. poll a batch
    2. INSERT ... ON CONFLICT DO NOTHING
    3. Postgres COMMIT
    4. consumer.commit()

Nothing is skipped, because the Kafka offset only advances after the rows are durable: crash
before step 3 and the same messages are redelivered. Nothing is duplicated, because a redelivery
carries the same (partition, offset) and the unique index refuses it.

Reversing 3 and 4 -- or leaving enable.auto.commit at its default of true, which effectively
does reverse them -- produces silent data loss. The loader reports success, the offsets are
past the data, and nothing anywhere says a batch went missing. That is why auto-commit is
turned off explicitly rather than left alone.

WHY THE DEDUPLICATION KEY IS THE KAFKA COORDINATE AND NOT ping_id.
The application already removed device retries with ON CONFLICT (ping_id) DO NOTHING
(docker/oltp/init.sql:189-191), so the OLTP holds one row per ping and Debezium emits one
message for it. A duplicate arriving here can therefore only be a broker redelivery, which the
coordinate identifies exactly. ping_id would be wrong twice over: it is null for deletes, and
a re-snapshot (op='r' after an earlier op='c') is a genuinely new observation that ping_id
would collapse into the create it re-reads.

    python -m fleet_telemetry.ingest.consumer
    python -m fleet_telemetry.ingest.consumer --max-batches 5
"""

from __future__ import annotations

import argparse
import signal
from collections.abc import Sequence
from typing import Any

from psycopg import connect

from fleet_telemetry import config
from fleet_telemetry.ingest import envelope
from fleet_telemetry.load import schema, writer

# Messages per poll. Large enough that one transaction covers many rows; small enough that a
# crash replays little and the suppressed count stays readable.
BATCH_SIZE = 500

# Seconds to wait for a full batch before taking what has arrived. Bounds how long a message
# sits unlanded when the stream is quiet.
POLL_TIMEOUT = 2.0

# Consecutive empty polls that end a BOUNDED run (--max-batches). An unbounded run is a daemon
# and waits forever, which is correct: a quiet stream is not a finished one.
#
# Without this, a bounded run against a caught-up topic never returns. Empty polls do not count
# as batches -- rightly, since nothing was landed -- so the loop spins on `continue` and the
# batch counter never reaches its limit. It looks exactly like a hung consumer.
IDLE_POLLS_BEFORE_STOP = 3


def to_rows(messages: Sequence[Any]) -> tuple[list[writer.BronzeRow], list[str]]:
    """Map messages to bronze rows. Returns (rows, topics that routed nowhere).

    Unrouted topics are returned rather than logged-and-forgotten because the caller has to
    decide: a topic nobody planned for means table.include.list changed, and quietly skipping
    it while advancing the offset past it is unrecoverable once retention expires.
    """
    rows: list[writer.BronzeRow] = []
    unrouted: list[str] = []

    for message in messages:
        topic = message.topic()
        table = envelope.table_for_topic(topic)
        if table is None:
            unrouted.append(topic)
            continue

        payload, raw_payload, parse_error = envelope.decode(message.value())
        # (TIMESTAMP_NOT_AVAILABLE, -1) when the producer set none.
        kind, value = message.timestamp()
        rows.append(
            writer.BronzeRow(
                table=table,
                topic=topic,
                partition=message.partition(),
                offset=message.offset(),
                timestamp=value if kind else None,
                payload=payload,
                raw_payload=raw_payload,
                parse_error=parse_error,
            )
        )

    return rows, unrouted


def run(max_batches: int | None = None) -> dict[str, int]:
    """Consume until interrupted, or for a fixed number of batches.

    Returns the run totals -- inserted, suppressed, malformed, batches. Returned rather than
    only printed so a test can assert on them: "bronze did not grow after a replay" is also
    true when the replay never happened, and the suppressed count is what tells the two apart.
    """
    from confluent_kafka import Consumer

    broker = config.kafka()
    client = Consumer(
        {
            "bootstrap.servers": broker.bootstrap_servers,
            "group.id": broker.consumer_group,
            # The whole point. See the module docstring.
            "enable.auto.commit": False,
            # A new group starts from the beginning of every topic, so the Debezium snapshot
            # is not skipped. The default, "latest", would silently discard it.
            "auto.offset.reset": "earliest",
        }
    )
    client.subscribe(sorted(envelope.TOPIC_TABLES))

    stopping = False

    def _stop(*_: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    totals = {"inserted": 0, "suppressed": 0, "malformed": 0}
    batches = 0
    idle_polls = 0

    with connect(config.warehouse().dsn()) as conn:
        schema.apply(conn)
        try:
            while not stopping and (max_batches is None or batches < max_batches):
                messages = client.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT)
                messages = [m for m in messages if m.error() is None]
                if not messages:
                    idle_polls += 1
                    if max_batches is not None and idle_polls >= IDLE_POLLS_BEFORE_STOP:
                        break
                    continue
                idle_polls = 0

                rows, unrouted = to_rows(messages)
                for topic in sorted(set(unrouted)):
                    print(f"WARNING: no bronze table for topic {topic}; not committing past it")
                if unrouted:
                    # Refusing to advance is deliberate. Skipping an unrouted topic loses it
                    # permanently once retention expires, and the offsets would show no gap.
                    raise RuntimeError(f"unrouted topics: {sorted(set(unrouted))}")

                inserted, suppressed = writer.write(conn, rows)
                conn.commit()  # (3) rows are durable
                client.commit(asynchronous=False)  # (4) only now may the offset move

                malformed = sum(1 for row in rows if row.parse_error)
                totals["inserted"] += inserted
                totals["suppressed"] += suppressed
                totals["malformed"] += malformed
                batches += 1
                print(
                    f"batch {batches}: {inserted} inserted, {suppressed} suppressed, "
                    f"{malformed} malformed"
                )
        finally:
            client.close()

    print(
        f"stopped after {batches} batches: {totals['inserted']} inserted, "
        f"{totals['suppressed']} suppressed by the dedup index, "
        f"{totals['malformed']} stored with a parse error"
    )
    return {**totals, "batches": batches}


def main() -> int:
    parser = argparse.ArgumentParser(description="Land Debezium change events in bronze.*")
    parser.add_argument("--max-batches", type=int, default=None)
    args = parser.parse_args()
    run(args.max_batches)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
