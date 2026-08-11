"""The batch poller: its watermark arithmetic, and then its limits.

The arithmetic is hermetic. The limits are integration tests, because a poller failing to see
a delete is a claim about a real database, not about a function.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fleet_telemetry.ingest import poller

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
