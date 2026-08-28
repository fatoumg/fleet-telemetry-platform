"""The truth schema, applied against a real warehouse.

Integration because the properties worth asserting here are properties of Postgres, not of Python:
that the DDL is idempotent, that the columns really are typed, and that a round trip preserves every
field exactly. A mock would assert that the code calls the functions the code calls.

THE TYPE ASSERTIONS ARE NOT DECORATION. Bronze is deliberately all-text (load/schema.py:20-27) and
this table is deliberately not, so the obvious "consistency" fix is to align them -- which would
silently move every cast failure from the simulator, where it is a bug report, into the diff, where
it is a missing row. Asserting the types from the database is what makes that fix fail loudly.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from fleet_telemetry import config, truth

pytestmark = pytest.mark.integration

# Same convention as tests/test_app.py:35-37. Anything this suite writes must be identifiable and
# removable, because a leaked row here is a row the ground-truth diff will look for in Silver and
# never find -- a permanent false failure in the one test the project rests on.
TEST_RUN_ID = "00000000-0000-4000-8000-00000000dead"
TEST_SEQ_BASE = 900_000


@pytest.fixture(scope="module")
def warehouse():
    psycopg = pytest.importorskip("psycopg")
    target = config.warehouse()
    try:
        conn = psycopg.connect(target.dsn(), connect_timeout=3)
    except psycopg.OperationalError as exc:
        pytest.skip(f"warehouse unreachable at {target.safe_dsn()}: {exc}")
    with conn:
        truth.apply(conn)
        yield conn
        with conn.cursor() as cur:
            cur.execute(
                f"delete from {truth.SCHEMA}.intended_pings where run_id = %s", (TEST_RUN_ID,)
            )
        conn.commit()


def test_apply_is_idempotent(warehouse):
    """Called on every simulator start, so a second call must be a no-op rather than an error."""
    truth.apply(warehouse)
    truth.apply(warehouse)
    with warehouse.cursor() as cur:
        cur.execute(
            "select count(*) from information_schema.tables "
            "where table_schema = %s and table_name = %s",
            (truth.SCHEMA, "intended_pings"),
        )
        assert cur.fetchone()[0] == 1


def test_columns_are_typed_not_text(warehouse):
    """The inversion of Bronze's rule, asserted from the database so nobody can 'fix' it quietly."""
    with warehouse.cursor() as cur:
        cur.execute(
            "select column_name, data_type from information_schema.columns "
            "where table_schema = %s and table_name = 'intended_pings'",
            (truth.SCHEMA,),
        )
        types = dict(cur.fetchall())

    assert types["ping_id"] == "uuid"
    assert types["run_id"] == "uuid"
    assert types["sequence_no"] == "bigint"
    assert types["vehicle_id"] == "integer"
    assert types["intended_device_ts"] == "timestamp with time zone"
    assert types["truth_ts"] == "timestamp with time zone"
    assert types["latitude"] == "double precision"
    assert types["longitude"] == "double precision"
    assert types["emitted"] == "boolean"
    # The one column that IS text, because a pathology name is a label rather than a measurement.
    assert types["pathology"] == "text"


def test_the_grain_is_enforced_by_the_database(warehouse):
    """Unlike pings, which withholds this constraint on purpose (docker/oltp/init.sql:236-238).

    The difference is who wrote the row. A duplicate ping is evidence about a device and must be
    kept; a duplicate truth row could only be our own recorder writing twice, and it would inflate
    the denominator of every ground-truth comparison downstream.
    """
    with warehouse.cursor() as cur:
        cur.execute(
            "select indexdef from pg_indexes "
            "where schemaname = %s and indexname = 'intended_pings_run_vehicle_seq_uk'",
            (truth.SCHEMA,),
        )
        row = cur.fetchone()

    assert row is not None, "the grain index is missing"
    assert "UNIQUE" in row[0]
    assert "run_id" in row[0]
    assert "vehicle_id" in row[0]
    assert "sequence_no" in row[0]


def test_write_round_trips_every_field_exactly(warehouse):
    """Exact, not approximate.

    The diff compares for equality, so a lossy round trip here would surface later as a pipeline
    discrepancy that is really a storage artefact -- and the obvious repair for that is to introduce
    a tolerance, which is how a real reconciliation becomes decorative.
    """
    row = truth.from_reading(
        {
            "ping_id": "44444444-4444-4444-8444-444444444444",
            "vehicle_id": 9101,
            "sequence_no": TEST_SEQ_BASE + 1,
            "device_ts": "2026-08-28T10:00:00+00:00",
            "latitude": 13.454900,
            "longitude": -16.579000,
            "speed_kmh": 45.0,
            "heading_deg": 91.5,
        },
        run_id=TEST_RUN_ID,
        seed=42,
        truth_ts=datetime(2026, 8, 28, 10, 0, tzinfo=UTC),
    )

    assert truth.write(warehouse, [row]) == 1
    # Idempotent on the primary key, so a retried flush lands nothing rather than raising.
    assert truth.write(warehouse, [row]) == 0
    warehouse.commit()

    with warehouse.cursor() as cur:
        cur.execute(
            f"select latitude, longitude, speed_kmh, heading_deg, intended_device_ts, truth_ts, "
            f"emitted, pathology, seed from {truth.SCHEMA}.intended_pings "
            f"where run_id = %s and sequence_no = %s",
            (TEST_RUN_ID, TEST_SEQ_BASE + 1),
        )
        got = cur.fetchone()

    assert got[0] == row.latitude
    assert got[1] == row.longitude
    assert got[2] == row.speed_kmh
    assert got[3] == row.heading_deg
    assert got[4] == row.intended_device_ts
    assert got[5] == row.truth_ts
    assert got[6] is True
    assert got[7] is None
    assert got[8] == 42


def test_write_of_an_empty_batch_touches_nothing(warehouse):
    """The final flush of a run with an empty buffer hits this path on every single run."""
    assert truth.write(warehouse, []) == 0
