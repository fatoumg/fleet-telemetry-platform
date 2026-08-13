"""The hand-written Silver layer: its ordering, and the properties it claims to hold.

    docker compose -f docker/docker-compose.yml up -d
    pytest -m integration

Split the same way tests/test_poller.py is split: the file-ordering logic is hermetic, because
it is a claim about a function. Everything else is a claim about a database and is marked
integration.

THESE TESTS ARE THE THING THE LAYER DOES NOT HAVE. Every assertion below is a property that
sql/silver/*.sql relies on and cannot check for itself -- the grain, the types, that the
deduplication key is right, that nothing was dropped without being recorded. Written by hand
here, once, for one layer. dbt generates the equivalent from a `tests:` block, which is the whole
argument of issue #12; the labour visible in this file is the argument's evidence.

UNLIKE tests/test_poller.py, NOTHING HERE WRITES TO bronze OR DELETES FROM IT. That test's
fixture issues an unqualified `delete from bronze.poll_watermarks`, which resolves the watermark
to EPOCH and re-ingests the entire source system (docs/known-issues.md, section 2). These tests
read bronze and rebuild silver_manual, which is derived and rebuildable by definition -- the
worst case is that a developer's Silver tables are rebuilt, which is what the runner does anyway.
"""

from __future__ import annotations

import pytest

from fleet_telemetry import config
from fleet_telemetry.transform import run as transform

# ------------------------------------------------------------------------------------
# ordering -- hermetic
# ------------------------------------------------------------------------------------


def test_the_target_schema_is_not_silver():
    """The single most important line in this file.

    dbt materialises every staging model as a VIEW in `silver` (dbt/dbt_project.yml:19-23). If
    this ever becomes "silver", dbt drops these tables the first time it runs and the artifact
    this phase produced is gone -- and the dbt-vs-hand diff that issue #12 gets for free goes
    with it. Anyone tidying the schema name into the "right" one trips this.
    """
    assert transform.SCHEMA == "silver_manual"
    assert transform.SCHEMA != "silver"


def test_discovery_is_sorted_by_filename(tmp_path):
    """That sort is the entire dependency graph, so it is worth pinning."""
    for name in ("50_late.sql", "00_first.sql", "10_second.sql"):
        (tmp_path / name).write_text("select 1", encoding="utf-8")
    (tmp_path / "notes.txt").write_text("ignored", encoding="utf-8")

    assert [p.name for p in transform.discover(tmp_path)] == [
        "00_first.sql",
        "10_second.sql",
        "50_late.sql",
    ]


def test_discovery_of_an_empty_directory_is_empty(tmp_path):
    assert transform.discover(tmp_path) == []


def test_the_shipped_scripts_run_in_dependency_order():
    """Pin the real order, because renaming a file silently reorders the build.

    40_ping_quality reads what 10_stg_pings writes, and 50_vehicle_day reads 10, 20 and 21.
    Nothing in the SQL, the runner or Postgres records that -- CREATE TABLE AS registers no
    catalog dependency. The filenames are the only expression of it, so this test is the only
    thing standing between a rename and a layer that builds in the wrong order.
    """
    order = [p.name for p in transform.discover(transform.SQL_DIR)]
    assert order == [
        "00_rejected_rows.sql",
        "10_stg_pings.sql",
        "20_stg_vehicles.sql",
        "21_stg_depots.sql",
        "22_stg_drivers.sql",
        "23_stg_jobs.sql",
        "30_stg_job_events.sql",
        "40_ping_quality.sql",
        "50_vehicle_day.sql",
    ]
    assert order.index("10_stg_pings.sql") < order.index("40_ping_quality.sql")
    assert order.index("20_stg_vehicles.sql") < order.index("50_vehicle_day.sql")


# ------------------------------------------------------------------------------------
# the layer itself -- integration
# ------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def warehouse():
    """A connection, and a Silver layer that has been built at least once.

    AUTOCOMMIT IS LOAD-BEARING, and it cost a hung test run to learn. psycopg opens a
    transaction on the first statement and holds it until commit, so a plain SELECT here leaves
    this connection idle-in-transaction holding an ACCESS SHARE lock on stg_pings. The next
    `transform.run()` then asks for ACCESS EXCLUSIVE to DROP that table and waits for a lock
    this very fixture is holding -- forever, with no error, no timeout and no output. It
    presents as a hung test, not as a deadlock, because Postgres is entirely happy to wait.

    Autocommit means every read releases its lock immediately. The alternative -- committing
    after every query -- works and is one forgotten call away from the same hang.
    """
    psycopg = pytest.importorskip("psycopg")
    try:
        conn = psycopg.connect(config.warehouse().dsn(), connect_timeout=3, autocommit=True)
    except Exception as exc:
        pytest.skip(f"warehouse not reachable ({type(exc).__name__}); start docker compose")

    with conn:
        if transform.run() != 0:
            pytest.fail("the Silver scripts did not run cleanly; see the output above")
        yield conn


def _one(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


@pytest.mark.integration
def test_stg_pings_is_unique_on_ping_id(warehouse):
    """The declared grain. Nothing in 10_stg_pings.sql asserts it.

    Note what this test would NOT have caught at the time it was written: bronze held 350,742
    ping rows and 350,742 distinct ping_ids, so DISTINCT ON removed nothing and the query is
    output-identical whether the deduplication rule is right or wrong. Only after a connector
    re-registration would the two diverge -- which is exactly why the assertion has to exist
    before the divergence, not after.
    """
    rows, distinct = _one(
        warehouse,
        "select count(*), count(distinct ping_id) from silver_manual.stg_pings",
    )
    assert rows == distinct, f"{rows - distinct} duplicate ping_ids survived deduplication"


@pytest.mark.integration
@pytest.mark.parametrize(
    ("table", "grain"),
    [
        ("stg_vehicles", "vehicle_id"),
        ("stg_depots", "depot_id"),
        ("stg_drivers", "driver_id"),
        ("stg_jobs", "job_id"),
        ("stg_job_events", "job_event_id"),
    ],
)
def test_every_staging_table_holds_its_declared_grain(warehouse, table, grain):
    rows, distinct = _one(
        warehouse, f"select count(*), count(distinct {grain}) from silver_manual.{table}"
    )
    assert rows == distinct, f"{table} is not unique on {grain}"


@pytest.mark.integration
@pytest.mark.parametrize("table", ["ping_quality", "vehicle_day"])
def test_the_derived_tables_hold_their_composite_grain(warehouse, table):
    """One row per vehicle per day. A fan-out here would inflate every number silently."""
    rows, distinct = _one(
        warehouse,
        f"select count(*), count(distinct (vehicle_id, event_date)) from silver_manual.{table}",
    )
    assert rows == distinct, f"{table} fanned out: {rows} rows, {distinct} distinct grains"


@pytest.mark.integration
def test_nothing_was_dropped_without_being_recorded(warehouse):
    """Bronze rows in == Silver rows out + rejects, bounded to what Silver actually read.

    Bounded by max(bronze_offset) rather than counting all of bronze, because the simulator is
    usually still running and bronze grows during the test. Comparing unbounded counts would
    fail for a reason that has nothing to do with correctness -- and would then be "fixed" by
    loosening the assertion, which is how a real reconciliation test becomes decorative.
    """
    bronze, silver, rejects = _one(
        warehouse,
        """
        select (select count(*) from bronze.raw_ping_events
                 where _kafka_offset <= bound.m),
               (select count(*) from silver_manual.stg_pings),
               (select count(*) from silver_manual.rejected_rows
                 where source_table = 'raw_ping_events')
          from (select max(bronze_offset) as m from silver_manual.stg_pings) bound
        """,
    )
    # Any shortfall beyond the recorded rejects is a row that vanished with no evidence, which
    # is the one outcome Bronze exists to make impossible.
    assert bronze == silver + rejects, (
        f"bronze {bronze} != silver {silver} + rejects {rejects}; "
        f"{bronze - silver - rejects} rows went missing unrecorded"
    )


@pytest.mark.integration
def test_the_casts_actually_happened(warehouse):
    """Bronze is all text. If Silver is too, the layer did nothing and no query would say so."""
    with warehouse.cursor() as cur:
        cur.execute(
            "select column_name, data_type from information_schema.columns "
            "where table_schema = %s and table_name = 'stg_pings'",
            (transform.SCHEMA,),
        )
        types = dict(cur.fetchall())

    assert types["ping_id"] == "uuid"
    assert types["vehicle_id"] == "integer"
    assert types["sequence_no"] == "bigint"
    assert types["device_ts"] == "timestamp with time zone"
    assert types["server_ts"] == "timestamp with time zone"
    assert types["latitude"] == "double precision"
    # PostGIS types report as USER-DEFINED in information_schema.
    assert types["position"] == "USER-DEFINED"


@pytest.mark.integration
def test_geometry_is_lon_lat_and_the_fleet_is_in_the_gambia(warehouse):
    """The lat/lon swap does not error, and changes total distance by only ~2%.

    Measured: swapping the two arguments to ST_MakePoint moved the fleet's summed distance from
    34,154.8 km to 34,885.9 km -- plausible, unremarkable, and wrong. The latitude and longitude
    COLUMNS stay correct in that case, so the only thing that catches it is asking where the
    geometry says the vehicles are. 13N/16W is The Gambia; the swap puts them at 16S/13E, in the
    South Atlantic.
    """
    lon, lat = _one(
        warehouse,
        "select ST_X(position), ST_Y(position) from silver_manual.stg_pings limit 1",
    )
    assert 13.0 < lat < 14.0, f"latitude {lat} is not in The Gambia -- arguments swapped?"
    assert -17.0 < lon < -13.0, f"longitude {lon} is not in The Gambia -- arguments swapped?"


@pytest.mark.integration
def test_running_it_twice_changes_nothing(warehouse):
    """Idempotent, checked on a closed window so live ingestion cannot muddy the result."""
    query = """
        select count(*), sum(('x' || substr(md5(ping_id::text), 1, 8))::bit(32)::bigint)
          from silver_manual.stg_pings
         where bronze_offset <= %s
    """
    (bound,) = _one(warehouse, "select max(bronze_offset) from silver_manual.stg_pings")
    before = _one(warehouse, query, (bound,))

    assert transform.run() == 0
    assert _one(warehouse, query, (bound,)) == before


@pytest.mark.integration
def test_a_failing_script_stops_the_run_and_reports_a_nonzero_exit(tmp_path, warehouse, capsys):
    """The loud-failure path, which is the runner's most useful output.

    Asserts what the failure report must contain: which scripts committed, which rolled back,
    and which never ran. Without those three lists an operator cannot tell which tables are now
    stale, and "which of my numbers moved?" has no answer.
    """
    (tmp_path / "01_fine.sql").write_text(
        f"create table if not exists {transform.SCHEMA}.pytest_probe (n integer)",
        encoding="utf-8",
    )
    (tmp_path / "02_broken.sql").write_text(
        "select column_that_does_not_exist from bronze.raw_ping_events", encoding="utf-8"
    )
    (tmp_path / "03_never_runs.sql").write_text("select 1", encoding="utf-8")

    try:
        assert transform.run(tmp_path) == 1
        printed = capsys.readouterr().out
        assert "01_fine.sql" in printed
        assert "02_broken.sql" in printed
        assert "03_never_runs.sql" in printed
        assert "never attempted" in printed
    finally:
        # Teardown here rather than after the assertions, so a failing assertion cannot leave
        # the probe table behind -- the mistake tests/test_poller.py makes with driver_id 9401
        # (docs/known-issues.md, section 3).
        with warehouse.cursor() as cur:
            cur.execute(f"drop table if exists {transform.SCHEMA}.pytest_probe")
