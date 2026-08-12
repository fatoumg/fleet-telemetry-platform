"""What each ingestion path captured from the same run. Phase 2's deliverable is this diff.

Every number here comes from a query, the same rule the phase 1 profiler follows: nothing is
asserted from the schema or from what a tool is documented to do. A profile that disagrees with
the simulator's own counts means the measurement is wrong, not the data
(src/fleet_telemetry/profile_source.py:16-18).

The poller measurements land first (phase 2 step 1). The CDC side is added once the consumer
exists, so the two can be compared over one window rather than two.
"""

from __future__ import annotations

from typing import Any

from psycopg import Connection
from psycopg.rows import dict_row


def poller_blind_spots(oltp_conn: Connection, wh_conn: Connection) -> dict[str, Any]:
    """Rows the OLTP holds that bronze.poll_rows does not, and the reverse.

    The reverse direction matters as much: a row in poll_rows whose id no longer exists in the
    OLTP is a delete the poller recorded the *existence* of and not the *removal* of -- bronze
    says the vehicle is active, and nothing in bronze will ever say otherwise.
    """
    out: dict[str, Any] = {}
    with oltp_conn.cursor(row_factory=dict_row) as cur:
        cur.execute("select vehicle_id from vehicles")
        live_vehicles = {row["vehicle_id"] for row in cur.fetchall()}

    with wh_conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "select distinct (row_image ->> 'vehicle_id')::int as vehicle_id "
            "from bronze.poll_rows where source_table = 'vehicles'"
        )
        polled_vehicles = {row["vehicle_id"] for row in cur.fetchall()}

        # Distinct row images per table. A poller that observed the same table ten times with
        # nothing changing would show ten rows and one distinct image, which is the shape of
        # wasted work; a table where the two numbers track each other is one where every poll
        # found something new.
        cur.execute(
            "select source_table, count(*) as rows, "
            "count(distinct row_image::text) as distinct_images "
            "from bronze.poll_rows group by source_table order by source_table"
        )
        out["poll_rows_by_table"] = cur.fetchall()

    out["vehicles_live"] = len(live_vehicles)
    out["vehicles_in_poll_rows"] = len(polled_vehicles)
    # Vehicles bronze believes exist, that the OLTP has deleted. The poller cannot ever
    # shrink this set.
    out["deleted_but_still_present_in_bronze"] = sorted(polled_vehicles - live_vehicles)
    return out
