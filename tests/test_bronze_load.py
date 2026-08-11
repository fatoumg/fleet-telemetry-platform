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
from fleet_telemetry.load import schema

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
