"""What each ingestion path captured from the same run. Phase 2's deliverable is this diff.

Every number here comes from a query, the same rule the phase 1 profiler follows: nothing is
asserted from the schema or from what a tool is documented to do. A profile that disagrees with
the simulator's own counts means the measurement is wrong, not the data
(src/fleet_telemetry/profile_source.py:16-18).

The poller measurements land first (phase 2 step 1). The CDC side is added once the consumer
exists, so the two can be compared over one window rather than two.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from psycopg import Connection, connect
from psycopg.rows import dict_row

from fleet_telemetry import config


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


def cdc_vs_poller(oltp_conn: Connection, wh_conn: Connection) -> dict[str, Any]:
    """What each path captured over the same window. Every number from a query."""
    out: dict[str, Any] = {}
    with wh_conn.cursor(row_factory=dict_row) as cur:
        # Changes per entity, per path. The poller reports rows it observed; CDC reports
        # committed changes. They are not the same quantity, and the gap is the point.
        cur.execute(
            """
            select source_table,
                   count(*)                         as cdc_events,
                   count(*) filter (where op = 'c') as creates,
                   count(*) filter (where op = 'u') as updates,
                   count(*) filter (where op = 'd') as deletes,
                   count(*) filter (where op = 'r') as snapshot_reads
              from bronze.raw_cdc_entities
             group by source_table
             order by source_table
            """
        )
        out["cdc_by_table"] = cur.fetchall()

        cur.execute(
            "select source_table, count(*) as poll_rows from bronze.poll_rows "
            "group by source_table order by source_table"
        )
        out["poll_by_table"] = cur.fetchall()

        # The headline: deletes exist in one path and cannot exist in the other.
        cur.execute("select count(*) as n from bronze.raw_cdc_entities where op = 'd'")
        out["deletes_seen_by_cdc"] = cur.fetchone()["n"]
        # Structural, not measured. A poller reads current state, so there is no query that
        # could return a different number here -- which is exactly why it is worth stating.
        out["deletes_seen_by_poller"] = 0

        # Changes CDC saw for a key that the poller recorded fewer times. Each is a state the
        # database really held and the poller can never recover.
        cur.execute(
            """
            with cdc as (
                select "after" ->> 'vehicle_id' as vehicle_id, count(*) as changes
                  from bronze.raw_cdc_entities
                 where source_table = 'vehicles' and op in ('c', 'u')
                 group by 1
            ),
            polled as (
                select row_image ->> 'vehicle_id' as vehicle_id, count(*) as observations
                  from bronze.poll_rows where source_table = 'vehicles' group by 1
            )
            select cdc.vehicle_id, cdc.changes, coalesce(polled.observations, 0) as observations
              from cdc left join polled using (vehicle_id)
             where cdc.changes > coalesce(polled.observations, 0)
             order by cdc.changes - coalesce(polled.observations, 0) desc
            """
        )
        out["changes_the_poller_collapsed"] = cur.fetchall()

        # Bronze's own health. Malformed rows are kept, so they are countable rather than
        # invisible -- which is the entire argument for keeping them.
        cur.execute(
            """
            select 'raw_ping_events'  as table_name, count(*) as rows,
                   count(*) filter (where parse_error is not null) as malformed
              from bronze.raw_ping_events
            union all
            select 'raw_cdc_entities', count(*),
                   count(*) filter (where parse_error is not null)
              from bronze.raw_cdc_entities
            union all
            select 'raw_job_events', count(*),
                   count(*) filter (where parse_error is not null)
              from bronze.raw_job_events
            """
        )
        out["bronze_health"] = cur.fetchall()

    out.update(poller_blind_spots(oltp_conn, wh_conn))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Diff what each ingestion path captured.")
    parser.add_argument("--json", type=str, default=None, help="also write the findings to a file")
    args = parser.parse_args()

    with (
        connect(config.oltp().dsn()) as oltp_conn,
        connect(config.warehouse().dsn()) as wh_conn,
    ):
        findings = cdc_vs_poller(oltp_conn, wh_conn)

    rendered = json.dumps(findings, indent=2, default=str)
    print(rendered)
    if args.json:
        Path(args.json).write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
