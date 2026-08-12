"""The batch poller: its watermark arithmetic, and then its limits.

The arithmetic is hermetic. The limits are integration tests, because a poller failing to see
a delete is a claim about a real database, not about a function.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from fleet_telemetry import config
from fleet_telemetry.ingest import poller
from fleet_telemetry.load import schema

T0 = datetime(2026, 8, 11, 12, 0, 0, tzinfo=UTC)


def test_pings_watermark_on_server_ts_not_updated_at():
    """pings has no updated_at at all (docker/oltp/init.sql:214-224).

    Getting this wrong does not error -- the query simply fails to compile, or worse, someone
    adds the column and the poller starts trusting a value the application does not maintain.
    """
    assert poller.POLLED_TABLES["pings"] == "server_ts"
    assert poller.POLLED_TABLES["vehicles"] == "updated_at"


def test_watermark_advances_to_the_newest_row_seen():
    rows = [
        {"updated_at": T0 + timedelta(seconds=5)},
        {"updated_at": T0 + timedelta(seconds=9)},
        {"updated_at": T0 + timedelta(seconds=1)},
    ]
    assert poller.next_watermark(rows, "updated_at", T0) == T0 + timedelta(seconds=9)


def test_an_empty_poll_leaves_the_watermark_alone():
    """Advancing on an empty result to now() is the classic leak: any row committed with an
    earlier timestamp but a later visibility is then permanently skipped."""
    assert poller.next_watermark([], "updated_at", T0) == T0


def test_the_watermark_never_moves_backwards():
    """docs/source-system-reference.md section 8: the container clock steps backwards ~2.7s
    roughly every 30s under WSL2. A watermark that accepted a lower value would re-poll rows
    forever, or -- with a strict `>` -- skip the ones in between."""
    rows = [{"server_ts": T0 - timedelta(seconds=3)}]
    assert poller.next_watermark(rows, "server_ts", T0) == T0


@pytest.fixture()
def databases():
    """Both connections, and a bronze schema that exists."""
    psycopg = pytest.importorskip("psycopg")
    try:
        oltp = psycopg.connect(config.oltp().dsn(), connect_timeout=3)
        warehouse = psycopg.connect(config.warehouse().dsn(), connect_timeout=3)
    except Exception as exc:
        pytest.skip(f"databases not reachable ({type(exc).__name__}); start docker compose")
    schema.apply(warehouse)
    with warehouse.cursor() as cur:
        cur.execute("delete from bronze.poll_rows")
        cur.execute("delete from bronze.poll_watermarks")
    warehouse.commit()
    with oltp, warehouse:
        yield oltp, warehouse


@pytest.mark.integration
def test_a_second_poll_with_no_changes_lands_nothing(databases):
    """The watermark is doing its job if and only if this is true."""
    oltp, warehouse = databases
    poller.poll_once(oltp, warehouse)
    second = poller.poll_once(oltp, warehouse)
    assert sum(second.values()) == 0


@pytest.mark.integration
def test_a_changed_row_is_picked_up_on_the_next_poll(databases):
    oltp, warehouse = databases
    poller.poll_once(oltp, warehouse)
    with oltp.cursor() as cur:
        cur.execute("update depots set name = name where depot_id = 1")
    oltp.commit()

    landed = poller.poll_once(oltp, warehouse)
    assert landed["depots"] == 1

    with warehouse.cursor() as cur:
        cur.execute(
            "select row_image ->> 'depot_id' from bronze.poll_rows "
            "where source_table = 'depots' order by poll_row_id desc limit 1"
        )
        assert cur.fetchone()[0] == "1"


# Ids above the seeded fleet and above what the simulator touches, so teardown is exact --
# the same discipline as tests/test_app.py:32-38. Test rows left in the OLTP poison every
# profiler measurement.
TEST_VEHICLE_ID = 9401
TEST_DRIVER_ID = 9401


@pytest.mark.integration
def test_a_poller_can_never_see_a_delete(databases):
    """Failure 1, and the cleanest argument for CDC that exists.

    A deleted row has no updated_at to exceed a watermark. No polling frequency helps: the row
    is not there to be selected. DELETE /vehicles/{id} is a hard delete for exactly this
    demonstration (app/main.py:318-323).
    """
    oltp, warehouse = databases
    with oltp.cursor() as cur:
        cur.execute(
            "insert into vehicles (vehicle_id, plate, capacity, home_depot_id) "
            "values (%s, %s, 14, 1)",
            (TEST_VEHICLE_ID, f"TEST-{TEST_VEHICLE_ID}"),
        )
    oltp.commit()

    poller.poll_once(oltp, warehouse)  # sees the insert

    with oltp.cursor() as cur:
        cur.execute("delete from vehicles where vehicle_id = %s", (TEST_VEHICLE_ID,))
    oltp.commit()

    landed = poller.poll_once(oltp, warehouse)
    assert landed["vehicles"] == 0, "a poller reporting a delete would mean the test is wrong"

    with warehouse.cursor() as cur:
        cur.execute(
            "select count(*) from bronze.poll_rows where source_table = 'vehicles' "
            "and row_image ->> 'vehicle_id' = %s",
            (str(TEST_VEHICLE_ID),),
        )
        # One row: the insert. Nothing records that the vehicle ceased to exist.
        assert cur.fetchone()[0] == 1


@pytest.mark.integration
def test_two_changes_between_polls_collapse_into_one(databases):
    """Failure 2. The measured version of this is in docs/source-system-reference.md: the
    simulator made 12 vehicle reassignments and the OLTP shows 8 changed rows. Four committed
    changes are unrecoverable by any poller at any frequency."""
    oltp, warehouse = databases
    with oltp.cursor() as cur:
        cur.execute(
            "insert into drivers (driver_id, full_name, home_depot_id) values (%s, %s, 1)",
            (TEST_DRIVER_ID, "Poller Test Driver"),
        )
    oltp.commit()
    poller.poll_once(oltp, warehouse)

    with oltp.cursor() as cur:
        cur.execute("update drivers set home_depot_id = 5 where driver_id = %s", (TEST_DRIVER_ID,))
        oltp.commit()
        cur.execute("update drivers set home_depot_id = 1 where driver_id = %s", (TEST_DRIVER_ID,))
        oltp.commit()

    landed = poller.poll_once(oltp, warehouse)
    assert landed["drivers"] == 1, "two commits, one observation"

    with warehouse.cursor() as cur:
        cur.execute(
            "select row_image ->> 'home_depot_id' from bronze.poll_rows "
            "where source_table = 'drivers' and row_image ->> 'driver_id' = %s "
            "order by poll_row_id desc limit 1",
            (str(TEST_DRIVER_ID),),
        )
        # Back to 1. Depot 5 was real, was committed, and is now unrecoverable.
        assert cur.fetchone()[0] == "1"

    with oltp.cursor() as cur:
        cur.execute("delete from drivers where driver_id = %s", (TEST_DRIVER_ID,))
    oltp.commit()
