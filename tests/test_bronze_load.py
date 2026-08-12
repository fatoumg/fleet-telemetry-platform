"""Bronze DDL and the loader, against the real warehouse.

Marked `integration` because these need the warehouse container:

    docker compose -f docker/docker-compose.yml up -d warehouse
    pytest -m integration

Not mocked, for the same reason tests/test_app.py is not: the behaviour under test is the
database's -- GENERATED ALWAYS columns, ON CONFLICT DO NOTHING, and whether a malformed
payload survives an INSERT. A mocked connection would assert we call psycopg correctly,
which is not the same thing.
"""

from __future__ import annotations

import json

import pytest

from fleet_telemetry import config
from fleet_telemetry.load import schema, writer

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def conn():
    psycopg = pytest.importorskip("psycopg")
    try:
        connection = psycopg.connect(config.warehouse().dsn(), connect_timeout=3)
    except Exception as exc:
        pytest.skip(f"warehouse not reachable ({type(exc).__name__}); start docker compose")
    with connection:
        schema.apply(connection)
        yield connection


def test_apply_is_idempotent(conn):
    """Every startup path calls apply(); running it twice must not error or drop data."""
    schema.apply(conn)
    schema.apply(conn)
    with conn.cursor() as cur:
        cur.execute(
            "select table_name from information_schema.tables where table_schema = 'bronze'"
        )
        present = {row[0] for row in cur.fetchall()}
    assert set(schema.BRONZE_TABLES) <= present


def test_generated_columns_project_the_payload(conn):
    """Python inserts the envelope and nothing else; the columns Silver reads are SQL."""
    envelope = {
        "op": "u",
        "before": {"vehicle_id": 7, "current_driver_id": 3},
        "after": {"vehicle_id": 7, "current_driver_id": 9},
        "source": {"table": "vehicles", "ts_ms": 1754568000000},
    }
    with conn.cursor() as cur:
        cur.execute(
            "insert into bronze.raw_cdc_entities "
            "(_topic, _kafka_partition, _kafka_offset, payload) "
            "values (%s, %s, %s, %s) "
            'returning source_table, op, "before", "after", source_ts_ms',
            ("fleet.public.vehicles", 0, 1_000_001, json.dumps(envelope)),
        )
        source_table, op, before, after, source_ts_ms = cur.fetchone()
    conn.rollback()

    assert source_table == "vehicles"
    assert op == "u"
    assert before["current_driver_id"] == 3
    assert after["current_driver_id"] == 9
    # text, not bigint: see the module docstring on why no generated column may cast.
    assert source_ts_ms == "1754568000000"


def test_a_malformed_payload_is_stored_rather_than_rejected(conn):
    """Bronze keeps the evidence. A row that cannot be parsed still lands, with the error."""
    with conn.cursor() as cur:
        cur.execute(
            "insert into bronze.raw_ping_events "
            "(_topic, _kafka_partition, _kafka_offset, payload, raw_payload, parse_error) "
            "values (%s, %s, %s, %s, %s, %s) returning ping_id, raw_payload, parse_error",
            ("fleet.public.pings", 0, 2_000_001, None, "{not json", "Expecting property name"),
        )
        ping_id, raw_payload, parse_error = cur.fetchone()
    conn.rollback()

    assert ping_id is None
    assert raw_payload == "{not json"
    assert parse_error


def test_a_ping_id_that_is_not_a_uuid_still_lands(conn):
    """The reason every scalar generated column is text.

    A cast inside GENERATED ALWAYS raises on INSERT, which would make Bronze reject exactly
    the malformed evidence it exists to preserve. Postgres 16 has no TRY_CAST.
    """
    envelope = {"op": "c", "after": {"ping_id": "banana", "sequence_no": "not-a-number"}}
    with conn.cursor() as cur:
        cur.execute(
            "insert into bronze.raw_ping_events "
            "(_topic, _kafka_partition, _kafka_offset, payload) values (%s, %s, %s, %s) "
            "returning ping_id, sequence_no",
            ("fleet.public.pings", 0, 2_000_002, json.dumps(envelope)),
        )
        ping_id, sequence_no = cur.fetchone()
    conn.rollback()

    assert ping_id == "banana"
    assert sequence_no == "not-a-number"


def test_the_same_kafka_offset_cannot_land_twice(conn):
    """At-least-once delivery means a replayed message must be absorbed, not duplicated."""
    row = ("fleet.public.pings", 0, 3_000_001, json.dumps({"op": "c", "after": {}}))
    with conn.cursor() as cur:
        for _ in range(2):
            cur.execute(
                "insert into bronze.raw_ping_events "
                "(_topic, _kafka_partition, _kafka_offset, payload) values (%s, %s, %s, %s) "
                "on conflict (_kafka_partition, _kafka_offset) do nothing",
                row,
            )
        cur.execute(
            "select count(*) from bronze.raw_ping_events where _kafka_offset = %s",
            (3_000_001,),
        )
        (n,) = cur.fetchone()
    conn.rollback()

    assert n == 1


def _row(offset: int, table: str = "raw_ping_events", **kwargs) -> writer.BronzeRow:
    defaults = {
        "table": table,
        "topic": "fleet.public.pings",
        "partition": 0,
        "offset": offset,
        "timestamp": 1_754_568_000_000,
        "payload": json.dumps({"op": "c", "after": {"ping_id": f"id-{offset}"}}),
        "raw_payload": None,
        "parse_error": None,
    }
    return writer.BronzeRow(**{**defaults, **kwargs})


def test_write_reports_inserted_and_suppressed(conn):
    """The suppressed count is the point. At-least-once delivery is a slogan until it is a
    number you can watch go up after a restart."""
    rows = [_row(4_000_001), _row(4_000_002)]
    inserted, suppressed = writer.write(conn, rows)
    assert (inserted, suppressed) == (2, 0)

    replayed, suppressed = writer.write(conn, [*rows, _row(4_000_003)])
    assert (replayed, suppressed) == (1, 2)
    conn.rollback()


def test_a_batch_spanning_two_tables_writes_to_both(conn):
    """One poll returns messages from every subscribed topic, so a batch is heterogeneous."""
    rows = [
        _row(5_000_001, table="raw_ping_events"),
        _row(
            5_000_001,
            table="raw_cdc_entities",
            topic="fleet.public.vehicles",
            payload=json.dumps({"op": "u", "source": {"table": "vehicles"}}),
        ),
    ]
    inserted, suppressed = writer.write(conn, rows)
    assert (inserted, suppressed) == (2, 0)
    conn.rollback()


def test_a_malformed_row_does_not_take_the_batch_with_it(conn):
    """The failure mode that would defeat the whole design: one bad message rolling back the
    good ones, so the offsets advance past data that never landed."""
    rows = [
        _row(6_000_001),
        _row(6_000_002, payload=None, raw_payload="{not json", parse_error="Expecting"),
        _row(6_000_003),
    ]
    inserted, suppressed = writer.write(conn, rows)
    assert (inserted, suppressed) == (3, 0)
    conn.rollback()


def test_a_rewound_consumer_group_does_not_duplicate_bronze(conn):
    """The restart guarantee, end to end and against a real broker.

    Rewinding the group is the strongest possible version of a restart: it replays messages
    that certainly landed. If bronze grows, the deduplication key is wrong.
    """
    pytest.importorskip("confluent_kafka")
    from confluent_kafka import Consumer, TopicPartition

    from fleet_telemetry.ingest import consumer, envelope

    def bronze_count() -> int:
        with conn.cursor() as cur:
            cur.execute(
                "select (select count(*) from bronze.raw_ping_events) "
                "     + (select count(*) from bronze.raw_cdc_entities) "
                "     + (select count(*) from bronze.raw_job_events)"
            )
            return cur.fetchone()[0]

    conn.rollback()
    before = bronze_count()
    if before == 0:
        pytest.skip("bronze is empty; register the connector and run the consumer first")

    # Rewind by hand -- the same thing `rpk group seek bronze-loader --to start` does.
    broker = config.kafka()
    client = Consumer(
        {
            "bootstrap.servers": broker.bootstrap_servers,
            "group.id": broker.consumer_group,
            "enable.auto.commit": False,
        }
    )
    client.commit(
        offsets=[TopicPartition(topic, 0, 0) for topic in sorted(envelope.TOPIC_TABLES)],
        asynchronous=False,
    )
    client.close()

    totals = consumer.run(max_batches=40)

    conn.rollback()
    # Without this the test passes when the rewind silently did nothing: "bronze did not grow"
    # is trivially true of a replay that never happened.
    assert totals["suppressed"] > 0, "nothing was replayed, so nothing was proven"

    # Bronze grew by exactly the genuinely-new messages and not one row more.
    #
    # Not `== before`, and not `inserted == 0`: this pipeline is watching a live database that
    # the rest of the suite is also writing to, so tests/test_app.py's inserts and deletes turn
    # into real change events that this consumer is right to land. Asserting nothing was
    # inserted made the test fail whenever it ran second, which is a statement about test
    # ordering rather than about deduplication.
    assert bronze_count() == before + totals["inserted"], (
        "bronze grew by more than the new messages; the dedup key is not doing its job"
    )
