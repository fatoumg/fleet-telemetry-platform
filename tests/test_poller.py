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
