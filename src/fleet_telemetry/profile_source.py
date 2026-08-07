"""Measure the source system: what is in it, what shape, how much, how late.

This is the phase 1 deliverable. Building the application was the easy half; the point of the
phase is to **characterise** what it produces, the way you would profile a third-party API you
did not write and could not ask questions about.

Doing that on a system you built yourself feels redundant and is not. Two reasons:

  * Source-system analysis is a real skill, and this is the only time you can check your
    findings against the truth -- you know what the simulator intended, so a profile that
    disagrees means your measurement is wrong, not the data.
  * Everything downstream is sized and shaped by these numbers. Row counts decide whether
    partitioning matters. Null rates decide which columns can be joined on. The lateness
    distribution decides the phase 3 watermark. Guessing any of them means rebuilding later.

Every number printed here comes from a query. Nothing is asserted from the schema or from what
the simulator was configured to do -- that distinction is the whole discipline. The predecessor
project only discovered its fatal flaw because someone measured instead of assuming.

    python -m fleet_telemetry.profile_source
    python -m fleet_telemetry.profile_source --json profile.json
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any

from psycopg import connect
from psycopg.rows import dict_row

from fleet_telemetry import config

TABLES = ("depots", "drivers", "vehicles", "jobs", "job_events", "pings")


def _rows(conn, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def _one(conn, sql: str, params: tuple = ()) -> dict[str, Any] | None:
    result = _rows(conn, sql, params)
    return result[0] if result else None


def _rule(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


# --------------------------------------------------------------------------------------
# measurements
# --------------------------------------------------------------------------------------


def table_sizes(conn) -> list[dict]:
    """Row counts and bytes on disk.

    Uses an exact `count(*)` rather than the planner's estimate in `pg_class.reltuples`. The
    estimate is free but only as fresh as the last ANALYZE, and a wrong row count at this stage
    would mislead every capacity decision that follows.

    `pg_total_relation_size` includes indexes and TOAST, not just the heap -- worth knowing,
    because on `pings` the indexes are a large fraction of the total.
    """
    out = []
    for table in TABLES:
        n = _one(conn, f"select count(*) as n from {table}")["n"]
        size = _one(
            conn,
            "select pg_total_relation_size(%s) as total, pg_relation_size(%s) as heap",
            (table, table),
        )
        # Dead tuples: rows deleted or replaced by an UPDATE whose space Postgres has not yet
        # reclaimed. They explain why a table can occupy far more disk than its live row count
        # suggests -- deleting a million rows frees nothing until VACUUM runs. Worth seeing,
        # because "the table is twice the size I expected" is otherwise baffling.
        dead = _one(
            conn,
            "select coalesce(n_dead_tup, 0) as dead, last_vacuum, last_autovacuum "
            "from pg_stat_user_tables where relname = %s",
            (table,),
        ) or {"dead": 0, "last_vacuum": None, "last_autovacuum": None}
        out.append(
            {
                "table": table,
                "rows": n,
                "dead_rows": dead["dead"],
                "total_bytes": size["total"],
                "heap_bytes": size["heap"],
                "index_bytes": size["total"] - size["heap"],
                "bytes_per_row": round(size["total"] / n, 1) if n else None,
                "last_vacuum": dead["last_vacuum"] or dead["last_autovacuum"],
            }
        )
    return out


def ping_window(conn) -> dict | None:
    """The span of time the pings cover, and the resulting rate."""
    return _one(
        conn,
        """
        select min(device_ts) as first_ping,
               max(device_ts) as last_ping,
               count(*)       as pings,
               count(distinct vehicle_id) as vehicles,
               extract(epoch from (max(device_ts) - min(device_ts))) as span_seconds
          from pings
        """,
    )


def lateness(conn) -> dict | None:
    """Distribution of server_ts - device_ts.

    THE most important measurement in phase 1, because it sets the phase 3 lateness bound.

    Percentiles rather than a mean: lateness is heavily right-skewed once devices start
    reconnecting after an outage, and a mean over a long tail describes nobody. The p99 and max
    are what a watermark has to survive.
    """
    return _one(
        conn,
        """
        select count(*) as n,
               min(lag)  as min_s,
               percentile_cont(0.50) within group (order by lag) as p50_s,
               percentile_cont(0.95) within group (order by lag) as p95_s,
               percentile_cont(0.99) within group (order by lag) as p99_s,
               max(lag)  as max_s,
               avg(lag)  as mean_s,
               count(*) filter (where lag < 0) as negative
          from (select extract(epoch from (server_ts - device_ts)) as lag from pings) t
        """,
    )


def null_rates(conn, table: str) -> list[dict]:
    """Measured nullability, column by column.

    Not the same question as "is the column declared NOT NULL". A nullable column that is never
    actually null in practice can still be relied on today and break tomorrow; a column that is
    100% null is dead weight carrying a false promise. Both are only visible by counting.
    """
    columns = _rows(
        conn,
        """
        select column_name, data_type, is_nullable
          from information_schema.columns
         where table_schema = 'public' and table_name = %s
         order by ordinal_position
        """,
        (table,),
    )
    total = _one(conn, f"select count(*) as n from {table}")["n"]
    out = []
    for col in columns:
        nulls = 0
        if total:
            nulls = _one(
                conn,
                f'select count(*) as n from {table} where "{col["column_name"]}" is null',
            )["n"]
        out.append(
            {
                "column": col["column_name"],
                "type": col["data_type"],
                "declared_nullable": col["is_nullable"] == "YES",
                "nulls": nulls,
                "null_pct": round(nulls / total * 100, 1) if total else None,
            }
        )
    return out


def sequence_gaps(conn) -> dict | None:
    """Do any vehicles have holes in their ping sequence?

    In phase 1 the answer must be zero -- this is the clean baseline. Recording that now is
    what makes phase 3's injected gaps provably the injection rather than a pre-existing bug.

    The query compares each ping's sequence_no with the previous one for the same vehicle
    (a window function), and counts the steps larger than one.
    """
    return _one(
        conn,
        """
        with stepped as (
            select vehicle_id,
                   sequence_no - lag(sequence_no) over (
                       partition by vehicle_id order by sequence_no
                   ) as step
              from pings
        )
        select count(*) filter (where step > 1) as gaps,
               coalesce(sum(step - 1) filter (where step > 1), 0) as pings_missing,
               count(*) filter (where step = 0) as repeats
          from stepped
         where step is not null
        """,
    )


def job_status_mix(conn) -> list[dict]:
    return _rows(
        conn,
        """
        select status, count(*) as n,
               round(100.0 * count(*) / sum(count(*)) over (), 1) as pct
          from jobs group by status order by n desc
        """,
    )


def transition_mix(conn) -> list[dict]:
    return _rows(
        conn,
        """
        select coalesce(from_status, '(new)') as from_status, to_status, count(*) as n
          from job_events group by 1, 2 order by n desc
        """,
    )


def entity_churn(conn) -> list[dict]:
    """How many mutable rows have been changed since creation.

    This is the SCD Type 2 workload: every one of these is a history a Type 2 dimension would
    have preserved and the OLTP has already thrown away.
    """
    out = []
    for table, key in (("drivers", "driver_id"), ("vehicles", "vehicle_id")):
        row = _one(
            conn,
            f"""
            select count(*) as total,
                   count(*) filter (where updated_at > created_at) as changed
              from {table}
            """,
        )
        out.append({"table": table, "key": key, **row})
    return out


def ping_rate_per_vehicle_hour(conn) -> dict | None:
    return _one(
        conn,
        """
        select round(avg(n)::numeric, 1) as mean_pings_per_vehicle_hour,
               min(n) as min_n, max(n) as max_n, count(*) as vehicle_hours
          from (
            select vehicle_id, date_trunc('hour', device_ts) as hr, count(*) as n
              from pings group by 1, 2
          ) t
        """,
    )


# --------------------------------------------------------------------------------------
# report
# --------------------------------------------------------------------------------------


def build_profile(conn) -> dict[str, Any]:
    return {
        "measured_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "database": config.oltp().safe_dsn(),
        "tables": table_sizes(conn),
        "ping_window": ping_window(conn),
        "ping_rate": ping_rate_per_vehicle_hour(conn),
        "lateness": lateness(conn),
        "sequence": sequence_gaps(conn),
        "nulls": {t: null_rates(conn, t) for t in ("pings", "jobs", "vehicles", "drivers")},
        "job_status": job_status_mix(conn),
        "transitions": transition_mix(conn),
        "churn": entity_churn(conn),
    }


def _fmt_bytes(n: int | None) -> str:
    if n is None:
        return "-"
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024 or unit == "GB":
            return f"{n:,.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n}"


def print_report(p: dict[str, Any]) -> None:
    print(f"Source system profile   {p['measured_at']}")
    print(f"database: {p['database']}")

    _rule("1. Volume")
    print(
        f"  {'table':<12} {'rows':>10} {'dead':>9} {'total':>10} {'heap':>10} "
        f"{'indexes':>10} {'b/row':>8}"
    )
    for t in p["tables"]:
        print(
            f"  {t['table']:<12} {t['rows']:>10,} {t['dead_rows']:>9,} "
            f"{_fmt_bytes(t['total_bytes']):>10} {_fmt_bytes(t['heap_bytes']):>10} "
            f"{_fmt_bytes(t['index_bytes']):>10} {t['bytes_per_row'] or '-':>8}"
        )
    pings = next(t for t in p["tables"] if t["table"] == "pings")
    if pings["rows"]:
        share = pings["index_bytes"] / pings["total_bytes"] * 100
        print(f"\n  indexes are {share:.0f}% of the pings table -- they are not free")
    bloated = [t for t in p["tables"] if t["dead_rows"] > max(t["rows"] * 0.2, 1000)]
    for t in bloated:
        print(
            f"  {t['table']}: {t['dead_rows']:,} dead rows against {t['rows']:,} live. Deleted "
            f"and updated rows hold their\n    space until VACUUM runs, so size on disk "
            f"overstates the data. Not a leak -- run VACUUM."
        )

    w = p["ping_window"]
    if w and w["pings"]:
        hours = (w["span_seconds"] or 0) / 3600 or 1
        _rule("2. Rate")
        print(f"  window       : {w['first_ping']} -> {w['last_ping']}")
        print(f"  vehicles     : {w['vehicles']}")
        print(f"  pings        : {w['pings']:,} over {hours:.2f} h")
        print(f"  overall      : {w['pings'] / hours:,.0f} pings/hour")
        r = p["ping_rate"]
        print(
            f"  per vehicle  : {r['mean_pings_per_vehicle_hour']} pings/vehicle-hour "
            f"(min {r['min_n']}, max {r['max_n']}, over {r['vehicle_hours']} vehicle-hours)"
        )
        daily = float(r["mean_pings_per_vehicle_hour"]) * 12
        print(f"  implies      : ~{daily:,.0f} rows per vehicle per 12-hour day")
        print(f"               : ~{daily * 100 * 30:,.0f} rows for 100 vehicles over 30 days")

    lat = p["lateness"]
    if lat and lat["n"]:
        _rule("3. Lateness  (server_ts - device_ts, seconds)")
        print(
            f"  min {lat['min_s']:.1f}   p50 {lat['p50_s']:.1f}   p95 {lat['p95_s']:.1f}   "
            f"p99 {lat['p99_s']:.1f}   max {lat['max_s']:.1f}   mean {lat['mean_s']:.1f}"
        )
        if lat["negative"]:
            print(
                f"  {lat['negative']:,} rows have NEGATIVE lateness -- a device clock is ahead "
                f"of the server's. Not an error: it is why device_ts cannot be trusted."
            )
        else:
            print("  no negative values: no device clock is running ahead (clean baseline)")

    s = p["sequence"]
    if s:
        _rule("4. Sequence integrity")
        print(f"  gaps    : {s['gaps']:,}  ({s['pings_missing']:,} pings missing)")
        print(f"  repeats : {s['repeats']:,}")
        if not s["gaps"] and not s["repeats"]:
            print("  clean baseline -- every vehicle's sequence is complete and strictly")
            print("  increasing, so any gap seen later is the phase 3 injection, not a bug")

    _rule("5. Nullability, as measured")
    for table, cols in p["nulls"].items():
        print(f"\n  {table}")
        for c in cols:
            flag = ""
            if c["declared_nullable"] and c["null_pct"] == 0:
                flag = "  <- nullable but never null here; do not rely on it"
            elif c["null_pct"] == 100:
                flag = "  <- always null"
            print(
                f"    {c['column']:<28} {c['type']:<26} "
                f"{'NULL ok' if c['declared_nullable'] else 'NOT NULL':<9} "
                f"{str(c['null_pct']) + '%':>7}{flag}"
            )

    _rule("6. Jobs")
    for row in p["job_status"]:
        print(f"  {row['status']:<12} {row['n']:>6,}  {row['pct']}%")
    print("\n  transitions observed:")
    for row in p["transitions"]:
        print(f"    {row['from_status']:>10} -> {row['to_status']:<12} {row['n']:>6,}")

    _rule("7. Entity churn  (the SCD Type 2 workload)")
    for row in p["churn"]:
        pct = row["changed"] / row["total"] * 100 if row["total"] else 0
        print(
            f"  {row['table']:<10} {row['changed']:>4} of {row['total']:>4} rows changed "
            f"since creation ({pct:.0f}%)"
        )
    print(
        "\n  every one of those is a history the OLTP has already overwritten and cannot\n"
        "  recover. Preserving it is what phase 3's Type 2 dimensions are for."
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", help="also write the raw measurements to this path")
    args = parser.parse_args(argv)

    with connect(config.oltp().dsn()) as conn:
        profile = build_profile(conn)

    print_report(profile)

    if args.json:
        from pathlib import Path

        Path(args.json).write_text(json.dumps(profile, indent=2, default=str), encoding="utf-8")
        print(f"\nraw measurements written to {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
