"""The dbt Silver layer, diffed against the hand-written one it replaces.

    docker compose -f docker/docker-compose.yml up -d
    python -m fleet_telemetry.transform.run
    python -m dbt.cli.main build --project-dir dbt --profiles-dir dbt
    pytest tests/test_dbt_silver.py

This is the check docs/silver-by-hand.md:311-317 promised issue #12 would inherit for free.
`silver_manual` and `silver` hold the same models, built from the same Bronze by two independent
implementations, so where they disagree one of the two is wrong. That is the same shape of
deliverable as phase 2's poller-vs-CDC comparison.

WHY THIS IS PYTEST AND NOT A dbt SINGULAR TEST. A test in dbt/tests/ referencing silver_manual
would make `dbt build` depend on a schema dbt does not own: a fresh clone that had not run
transform.run would fail the build on a missing relation, and the error would name the relation
rather than the missing step. CI happens to run transform.run before dbt (.github/workflows/ci.yml
:108-117), but relying on that couples two independent steps. The diff is a claim about two layers,
not a property of one model.

READ-ONLY. Nothing here writes to bronze, silver or silver_manual -- unlike tests/test_poller.py,
whose fixture issues an unqualified `delete from bronze.poll_watermarks` and re-ingests the entire
source system (docs/known-issues.md, section 2).
"""

from __future__ import annotations

import pytest

from fleet_telemetry import config

pytestmark = pytest.mark.integration

# Model name in `silver` -> table name in `silver_manual`.
#
# Identical for the six stg_* models, deliberately, so the one-liner in the docs works. The reject
# bin is the one exception: CLAUDE.md requires the stg_ prefix in staging, and a naming convention
# that holds everywhere beats the elegance of a one-liner that holds once.
#
# ping_quality and vehicle_day are absent on purpose. They are not ported: the design spec's layer
# table (section 9) defines Silver as deduplication, typing, unit normalisation and geometry
# construction, and those two are percentile aggregates and a three-way join. They stay
# hand-written in silver_manual until the Gold ticket moves them.
LAYER_PAIRS = [
    ("stg_pings", "stg_pings"),
    ("stg_vehicles", "stg_vehicles"),
    ("stg_depots", "stg_depots"),
    ("stg_drivers", "stg_drivers"),
    ("stg_jobs", "stg_jobs"),
    ("stg_job_events", "stg_job_events"),
    ("stg_rejected_rows", "rejected_rows"),
]

# THE TWO LAYERS ARE NOT THE SAME KIND OF THING, and that is what makes this diff subtle.
# silver_manual holds TABLES, materialised at the moment transform.run last executed. silver holds
# VIEWS, evaluated at the moment you query them. So any Bronze row that arrives in between shows up
# on one side only, and the simulator is ingesting continuously.
#
# The first version of this test assumed the entity models were static and only bounded pings and
# job_events. It failed on stg_jobs and stg_rejected_rows the first time the full stack was up,
# because the simulator creates jobs continuously too. The counts were 151 vs 152.
#
# There are two honest strategies, and which one applies is a property of the source, not a
# preference:
#
#   BOUNDED -- for append-only streams. A ping is immutable, so a prefix of the stream bounded by
#              offset is stable, and the diff can be asserted while ingestion continues. This is the
#              closed-window discipline docs/silver-by-hand.md:56-60 describes.
#
#   FRONTIER -- for current-state models. These cannot be rewound. Filtering the view's output to
#              `bronze_offset <= bound` does NOT reconstruct state as of that offset: it DROPS an
#              entity whose latest version is newer, where the snapshot holds that entity's EARLIER
#              version. So there is no bounded query that makes the comparison valid. Instead,
#              compare the frontier -- if Bronze has not advanced for this relation since the
#              snapshot was taken, assert the diff is 0; if it has, skip and say so.
#
# The frontier check is deliberately not "compare row counts and skip if they differ". That would
# mask a genuine port bug that changed the row count as a skip. Comparing the offset frontier means
# a stale snapshot skips, while a stable frontier with differing content still fails.
BOUNDED = "bounded"
FRONTIER = "frontier"

STRATEGY = {
    "stg_pings": BOUNDED,
    "stg_job_events": BOUNDED,
    "stg_vehicles": FRONTIER,
    "stg_depots": FRONTIER,
    "stg_drivers": FRONTIER,
    "stg_jobs": FRONTIER,
    # Rejects are immutable once made -- they derive from append-only Bronze rows -- but they span
    # several topics whose offsets are independent sequences, so a single max() is not a bound.
    # Frontier, grouped by topic, is well-defined across all of them.
    "stg_rejected_rows": FRONTIER,
}

# The frontier expression per relation: what "how far has Bronze been consumed" means here. Grouped
# by topic for the reject bin, because four topics land in raw_cdc_entities and offsets restart from
# zero in every one of them.
FRONTIER_SQL = {
    "stg_rejected_rows": (
        "select coalesce(string_agg(_topic || ':' || mx, ',' order by _topic), 'empty') "
        "from (select _topic, max(_kafka_offset) as mx from {relation} group by _topic) t"
    ),
    "_default": "select coalesce(max(bronze_offset)::text, 'empty') from {relation}",
}


@pytest.fixture(scope="module")
def warehouse():
    """A read-only connection.

    AUTOCOMMIT IS LOAD-BEARING, and it cost a hung test run to learn -- see
    tests/test_transform.py:93-106. psycopg opens a transaction on the first statement and holds it
    until commit, so a plain SELECT here would leave this connection idle-in-transaction holding an
    ACCESS SHARE lock. The next transform.run() then asks for ACCESS EXCLUSIVE to DROP that table
    and waits for a lock this very fixture is holding -- forever, with no error and no timeout.
    """
    psycopg = pytest.importorskip("psycopg")
    try:
        conn = psycopg.connect(config.warehouse().dsn(), connect_timeout=3, autocommit=True)
    except Exception as exc:
        pytest.skip(f"warehouse not reachable ({type(exc).__name__}); start docker compose")

    with conn:
        yield conn


def _scalar(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def _exists(conn, schema, table):
    return _scalar(
        conn,
        "select count(*) from information_schema.tables "
        "where table_schema = %s and table_name = %s",
        (schema, table),
    )


@pytest.mark.parametrize(("dbt_model", "manual_table"), LAYER_PAIRS)
def test_dbt_silver_matches_the_hand_written_layer(warehouse, dbt_model, manual_table):
    """Both directions, because one alone passes if dbt returned a strict subset.

    BOUNDED or FRONTIER per model -- see the STRATEGY table above for why the choice is a property
    of the source rather than a preference. A test that fails for reasons unrelated to correctness
    gets "fixed" by loosening it, which is how a real reconciliation check becomes decorative
    (docs/silver-by-hand.md:56-60), so neither strategy is a weakened assertion: one asserts on a
    stable prefix, the other asserts only when the frontier has not moved.
    """
    if not _exists(warehouse, "silver", dbt_model):
        pytest.skip(f"silver.{dbt_model} missing; run dbt build")
    if not _exists(warehouse, "silver_manual", manual_table):
        pytest.skip(f"silver_manual.{manual_table} missing; run transform.run")

    where = ""
    if STRATEGY[dbt_model] == BOUNDED:
        bound = _scalar(warehouse, f"select max(bronze_offset) from silver_manual.{manual_table}")
        if bound is None:
            pytest.skip(f"silver_manual.{manual_table} is empty; nothing to diff")
        where = f" where bronze_offset <= {bound}"
    else:
        frontier = FRONTIER_SQL.get(dbt_model, FRONTIER_SQL["_default"])
        manual_frontier = _scalar(
            warehouse, frontier.format(relation=f"silver_manual.{manual_table}")
        )
        dbt_frontier = _scalar(warehouse, frontier.format(relation=f"silver.{dbt_model}"))
        if manual_frontier != dbt_frontier:
            pytest.skip(
                f"{dbt_model} is a current-state model and Bronze advanced since silver_manual "
                f"was built (frontier {manual_frontier} vs {dbt_frontier}). It cannot be rewound "
                "to a historical offset, so no valid comparison exists -- re-run "
                "transform.run, ideally with the simulator stopped."
            )

    manual = f"select * from silver_manual.{manual_table}{where}"
    dbt = f"select * from silver.{dbt_model}{where}"

    # A column-list mismatch raises here rather than returning rows, and that is the right
    # behaviour: EXCEPT refuses to compare relations of different shape, so a renamed or reordered
    # column is a hard error instead of a silent zero.
    only_in_manual = _scalar(warehouse, f"select count(*) from ({manual} except {dbt}) d")
    only_in_dbt = _scalar(warehouse, f"select count(*) from ({dbt} except {manual}) d")

    assert (only_in_manual, only_in_dbt) == (0, 0), (
        f"{dbt_model}: {only_in_manual} rows only in silver_manual, "
        f"{only_in_dbt} only in silver -- one of the two implementations is wrong"
    )


def test_the_two_layers_hold_the_same_model_names(warehouse):
    """The naming discipline the diff depends on.

    If someone renames a dbt model, the pair above silently stops being compared -- the test skips
    rather than fails, because the relation is missing rather than different. This asserts the
    pairing itself, so a rename is caught as a rename.
    """
    for dbt_model, manual_table in LAYER_PAIRS:
        assert _exists(warehouse, "silver", dbt_model), (
            f"silver.{dbt_model} does not exist; if it was renamed, update LAYER_PAIRS "
            "and docs/silver-by-hand.md:314, which promises the names match"
        )
        assert _exists(warehouse, "silver_manual", manual_table), (
            f"silver_manual.{manual_table} does not exist; run transform.run"
        )


def test_silver_is_not_silver_silver(warehouse):
    """The schema macro, asserted from the database rather than from the manifest.

    dbt's built-in generate_schema_name concatenates target.schema with +schema, which resolved
    every staging model to `silver_silver` -- measured, before dbt/macros/generate_schema_name.sql
    existed. `dbt build` reports success either way and the `silver` schema simply stays empty, so
    nothing about a green build would tell you. Delete that macro and this is what fails.
    """
    # Named explicitly rather than matched as `silver\_%`. That pattern also matches
    # silver_manual, which is the hand-written layer and is supposed to exist -- the first version
    # of this test asserted on it and failed with "9 relations", which is the count of a healthy
    # silver_manual. These three are the exact names dbt's built-in macro would produce from the
    # three +schema values in dbt_project.yml.
    concatenated = ("silver_silver", "silver_gold", "silver_marts")
    stray = _scalar(
        warehouse,
        "select count(*) from information_schema.schemata where schema_name = any(%s)",
        (list(concatenated),),
    )
    assert stray == 0, (
        f"{stray} of {concatenated} exist; dbt/macros/generate_schema_name.sql is missing or "
        "not being picked up, so models went to a concatenated schema and `silver` is empty"
    )
