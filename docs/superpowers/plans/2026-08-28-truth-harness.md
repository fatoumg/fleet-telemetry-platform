# Ground-Truth Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record every ping the simulator intended to emit into a `truth.intended_pings` table in the warehouse, and assert from dbt that Silver contains all of it — zero discrepancies on a clean run.

**Architecture:** A new top-level module `src/fleet_telemetry/truth.py` owns the schema, the row shape, the batched writer, and a recorder with an injectable sink. The simulator constructs the recorder behind an opt-in `--truth` flag and records each reading as it is taken, flushing to the warehouse alongside each ping POST. dbt declares `truth` as its second source and one singular test does the diff, bounded by a per-run frontier.

**Tech Stack:** Python 3.13, psycopg 3, Postgres 16 (+PostGIS/TimescaleDB), dbt-core 1.12 with the Postgres adapter, pytest.

**Spec:** Issue #14 (`gh issue view 14 --repo fatoumg/fleet-telemetry-platform`), slice A of three. The design context is `docs/superpowers/specs/2026-08-07-telemetry-platform-design.md` §8 (the pathology catalogue) and lines 236-237 (ground truth). The requirements analysis this plan implements was written earlier in this session — **commit it to `docs/superpowers/specs/2026-08-28-dirty-data-analysis.md` as the first step of implementation**, since it holds the reasoning for the three-way slicing and the six open questions and is currently only in the transcript.

## Global Constraints

- Python 3.13. `from __future__ import annotations` at the top of every new module. Type hints on all signatures. `dataclass(frozen=True)` / `NamedTuple` for data shapes.
- Every module opens with a docstring explaining **why**, not what. Match the surrounding comment density — the comments are the deliverable, not decoration.
- SQL embedded in Python is **lowercase**; SQL in DDL string literals is **uppercase keywords**.
- Use `127.0.0.1`, never `localhost`, in anything dbt or psycopg reads.
- Warehouse port is `55432`, oltp is `55433`. Container-side stays `5432`.
- Never read `os.environ` in a test — pass an explicit `env` mapping. CI has no credentials and must stay that way.
- dbt commands run as `python -m dbt.cli.main ...` with both `--project-dir dbt --profiles-dir dbt`. Fusion cannot build this project.
- `ruff check . && pytest` before pushing. Keep `ruff` in step with the `ruff-pre-commit` rev.
- Branch from `main`: `phase-3/truth-harness`.

---

## Context

Phase 3 is the project, and it holds the one hard problem: events arrive late, out of order, and in bursts, and the warehouse must still be correct. That is only *verifiable* because the simulator knows the truth it generated — but today it does not write that truth down anywhere. `simulator/` performs no file or table writes at all; its only outputs are HTTP POSTs and a stdout summary (`simulator/run.py:589-597`).

So there is currently no way to distinguish "the pipeline is correct" from "the pipeline looks plausible." The only ground-truth comparison ever performed was manual, on a closed window, recorded in `docs/silver-by-hand.md:48`.

This slice builds the harness and proves it against the **clean** simulator, where the answer must be exactly zero. Doing it in this order is the point: a diff validated while the expected result is known is the only thing that can later tell an injected pathology from a bug in the harness. Slices B and C (the six pathology flags) depend on this and are out of scope here.

## Approach

**Where the code lives, and why not in `load/`.** `src/fleet_telemetry/load/` is scoped to bronze by its own docstring — `load/schema.py:1-5` states the ownership rule ("Bronze is ours") and `apply()` hardcodes `bronze` as the only schema it creates. `truth` is a different schema with the **opposite** typing rule, so it gets a top-level module `src/fleet_telemetry/truth.py`, sibling to `config.py` and `profile_source.py`. It mirrors `load/schema.py`'s *shape* (module-level DDL f-string, `apply(conn)`, a `main()` that prints what it created) without claiming its *ownership*.

**Typed columns, not text.** Bronze is all `text` because a cast inside `GENERATED ALWAYS` runs on INSERT, so Bronze must never be able to reject the malformed evidence it exists to keep (`load/schema.py:20-27`). The truth table is written by our own code from values we constructed — a cast failure there is a bug we want loudly, immediately, at the write. This inversion must be stated in the module docstring or a future reader will "fix" it to match bronze.

**Recorded at reading time, flushed at POST time.** `take_reading` (`simulator/run.py:209`) is where the intent is formed and where `sequence_no` is consumed. Recording there — rather than in `flush()` — is what lets slice B record a *dropped* gap row that never reaches a batch at all. The `emitted` column exists for exactly that, and is always `true` in this slice.

**Ordering: truth first, then the POST.** A crash between them leaves a truth row with no ping, and the diff correctly reports it — the emission genuinely did not happen. The reverse order would lose the record of a ping that did go out, which is the failure mode that matters. Same reasoning shape as the Kafka commit ordering in `ingest/consumer.py:1-17`.

**Opt-in via `--truth`.** Default-on would make the simulator require a running warehouse, breaking the existing `oltp + api + simulator` workflow. The flag also keeps the phase-1 baseline reproducible.

**The diff bound is a per-run frontier, and the obvious bound is wrong.** The existing reconciliation tests bound every term on `max(bronze_offset)` because the simulator ingests continuously and unbounded counts are two measurements of a moving target (`dbt/tests/assert_ping_rows_are_modelled_or_rejected.sql:48-52`). Truth needs the same discipline but cannot use that column, and the naive substitute fails:

- `max(stg_pings.device_ts)` is taken over **all** of Silver, including ~190k pings from runs that predate the truth table. A backfill run generates `device_ts` from `start = end - span` — i.e. in the past (`simulator/run.py:535-537`) — so its window can end *before* the historical maximum. The bound would then admit every truth row including those still in flight, and the test would fail on a correct pipeline.
- The bound must instead be derived from rows that **actually landed for that run**: `max(device_ts)` over Silver rows whose `ping_id` matches a truth row of that `run_id`. Strict `<` on that frontier, because all pings of one tick share one `device_ts` and a tick can straddle a batch boundary (`PINGS_PER_BATCH = 500` against 40 vehicles means a flush lands mid-tick), so the frontier tick itself must be excluded.

**The reverse direction is not assertable, and the test says so.** "Every Silver ping has a truth row" would fail on all pre-truth historical data, and `--reset` cannot help because it deletes only OLTP `pings` while Bronze is append-only forever (`simulator/run.py:129-135`). The test asserts truth ⊆ Silver with field equality, and documents the gap rather than faking it — the same honesty as that file's own "WHAT THIS PROVES IN CI: nothing" note.

## File Changes

| # | Path | Change |
| --- | --- | --- |
| 1 | `src/fleet_telemetry/truth.py` | **Create.** DDL + `apply()` + `IntendedPing` + `from_reading()` + `write()` + `TruthRecorder` + `main()` |
| 2 | `docker/warehouse/init.sql` | **Modify.** Add `CREATE SCHEMA IF NOT EXISTS truth` after `marts` (~line 41) |
| 3 | `simulator/run.py` | **Modify.** `--truth` flag, a `run_id`, recorder threaded into `simulate` / `simulate_forever` / `take_reading` call sites, banner line |
| 4 | `docker/docker-compose.yml` | **Modify.** `WAREHOUSE_*` env + `depends_on: warehouse` on the `simulator` service (~lines 298-320) |
| 5 | `.github/workflows/ci.yml` | **Modify.** Add `truth` to the psql schema list; add a `python -m fleet_telemetry.truth` step |
| 6 | `dbt/models/staging/_sources.yml` | **Modify.** Second source entry, `truth` / `schema: truth` / `intended_pings` |
| 7 | `dbt/tests/assert_intended_pings_unique_on_run_vehicle_and_sequence.sql` | **Create.** Grain assertion |
| 8 | `dbt/tests/assert_intended_pings_reached_silver.sql` | **Create.** The diff |
| 9 | `tests/test_truth.py` | **Create.** Hermetic — the mapper and the recorder |
| 10 | `tests/test_truth_load.py` | **Create.** Integration — DDL idempotence and a write round-trip |
| 11 | `tests/test_simulator_backfill.py` | **Create.** Hermetic — `simulate()` has zero coverage today |
| 12 | `docs/truth-harness.md`, `README.md`, `CLAUDE.md` | **Modify/create.** Findings doc + the two command lists |

**Reuse, do not reinvent:**

- `config.warehouse()` → `DatabaseConfig` with `.dsn()` / `.safe_dsn()` (`src/fleet_telemetry/config.py:231`). No new config shape; the env vars are already `WAREHOUSE_HOST/PORT/DB/USER/PASSWORD` via `env_var_for` (`config.py:178-193`).
- `load/writer.py:48-94` — the `executemany` + NamedTuple + "does NOT commit, the caller owns the transaction" pattern. Copy it exactly.
- `load/schema.py:141-161` — `apply(conn)` and `main()`. Copy the shape.
- `tests/test_bronze_load.py:28-30` — the integration fixture idiom (`pytest.importorskip`, `connect_timeout=3`, skip when unreachable).
- `tests/test_simulator_forever.py:101-132` — `StubApi(Api)`, which subclasses the real `Api` so signature changes break loudly.
- `dbt/tests/assert_stg_pings_unique_on_ping_id.sql` — the grain-assertion form.

---

## Task 1: The truth schema and table

**Files:**
- Create: `src/fleet_telemetry/truth.py`
- Modify: `docker/warehouse/init.sql` (after the `marts` block, ~line 41)
- Test: `tests/test_truth_load.py`

**Interfaces:**
- Consumes: `fleet_telemetry.config.warehouse()`
- Produces: `SCHEMA = "truth"`, `TABLES = ("intended_pings",)`, `DDL: str`, `apply(conn: Connection) -> None`, `main() -> int`

- [ ] **Step 1: Write the failing integration test**

```python
# tests/test_truth_load.py
"""The truth schema, applied against a real warehouse.

Integration because the properties worth asserting -- that the DDL is idempotent, and that a
round-trip preserves every field exactly -- are properties of Postgres, not of Python. A mock
would assert that the code calls the functions the code calls.
"""

from __future__ import annotations

import pytest

from fleet_telemetry import config, truth

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def warehouse():
    psycopg = pytest.importorskip("psycopg")
    target = config.warehouse()
    try:
        conn = psycopg.connect(target.dsn(), connect_timeout=3)
    except psycopg.OperationalError as exc:
        pytest.skip(f"warehouse unreachable at {target.safe_dsn()}: {exc}")
    with conn:
        yield conn


def test_apply_is_idempotent(warehouse):
    truth.apply(warehouse)
    truth.apply(warehouse)  # second call must not raise
    with warehouse.cursor() as cur:
        cur.execute(
            "select count(*) from information_schema.tables "
            "where table_schema = %s and table_name = %s",
            (truth.SCHEMA, "intended_pings"),
        )
        assert cur.fetchone()[0] == 1


def test_columns_are_typed_not_text(warehouse):
    """The inversion of Bronze's rule, asserted so nobody 'fixes' it to match."""
    truth.apply(warehouse)
    with warehouse.cursor() as cur:
        cur.execute(
            "select column_name, data_type from information_schema.columns "
            "where table_schema = %s and table_name = 'intended_pings'",
            (truth.SCHEMA,),
        )
        types = dict(cur.fetchall())
    assert types["ping_id"] == "uuid"
    assert types["sequence_no"] == "bigint"
    assert types["intended_device_ts"] == "timestamp with time zone"
    assert types["latitude"] == "double precision"
    assert types["emitted"] == "boolean"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `python -m pytest tests/test_truth_load.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'fleet_telemetry.truth'`

- [ ] **Step 3: Write the module**

```python
# src/fleet_telemetry/truth.py
"""What the simulator INTENDED to emit. The reference every correctness claim is checked against.

Phase 3's whole premise is that the simulator knows the truth it generated, so pipeline output
can be diffed against reality rather than merely inspected for plausibility. Without this table
that premise is unexercised: a pipeline nobody can falsify is indistinguishable from one that
happens to look right, and docs/silver-by-hand.md:48 records the only ground-truth comparison
ever made -- by hand, once, on a closed window.

NOT IN load/. That package is scoped to bronze by load/schema.py:1-5 and its apply() creates no
other schema. This is a different layer with the opposite typing rule, so it mirrors that file's
SHAPE without claiming its OWNERSHIP.

EVERY COLUMN HERE IS TYPED, WHICH IS THE EXACT OPPOSITE OF BRONZE'S RULE, DELIBERATELY.
Bronze is all text because a cast inside GENERATED ALWAYS runs on INSERT, so one device sending
a ping_id of "banana" would take the whole batch down -- Bronze rejecting precisely the malformed
evidence it exists to keep (load/schema.py:20-27). Nothing analogous applies here. These values
are constructed by our own code three lines before the insert, so a cast that fails is a bug in
the recorder, and the loudest possible failure at the write is what we want. Do not "fix" this
to match bronze.

Created here rather than only in docker/warehouse/init.sql for the reason transform/run.py:68-72
gives: that file runs only on an empty data directory, so DDL added to it never reaches a volume
that already exists -- which is how four REPLICA IDENTITY FULL statements sat unexecuted for five
days (docs/known-issues.md, section 1). It is added to both: init.sql for a fresh clone, and
apply() for every existing volume.
"""

from __future__ import annotations

import argparse

from psycopg import Connection, connect

from fleet_telemetry import config

SCHEMA = "truth"
TABLES = ("intended_pings",)

DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.intended_pings (
    -- The device-generated UUID, recorded because it is the ONLY join key into silver.stg_pings.
    -- It is an unseeded uuid4 (simulator/run.py:218), so it cannot be recomputed from the seed
    -- and a run that failed to record it is unverifiable afterwards.
    ping_id            uuid        PRIMARY KEY,

    -- Which simulator process produced this row. Needed because ping_id is the only thing that
    -- differs between two same-seed runs: sequence numbers restart from the database
    -- (simulator/run.py:108) and device_ts windows from two backfill runs OVERLAP, since each
    -- walks from now-span to now. Without run_id the diff cannot tell one run's frontier from
    -- another's.
    run_id             uuid        NOT NULL,
    seed               integer     NOT NULL,

    vehicle_id         integer     NOT NULL,
    sequence_no        bigint      NOT NULL,

    -- What the device CLAIMED, i.e. exactly the value put on the wire. Named `intended_` rather
    -- than `device_ts` so a reader is never in doubt about which side of the diff they hold.
    intended_device_ts timestamptz NOT NULL,

    -- The real instant this reading describes. Identical to intended_device_ts in a clean run,
    -- and the two diverge the moment slice B's clock-skew flag exists. Recording both is what
    -- makes skew measurable rather than merely visible.
    truth_ts           timestamptz NOT NULL,

    -- False when the simulator formed the intent and deliberately did not send it -- slice B's
    -- sequence-gap flag. Always true in slice A. The column exists now so slice B needs no
    -- migration and the diff's filter is already written.
    emitted            boolean     NOT NULL,

    -- Which pathology flag caused this row to diverge from clean behaviour; null when clean.
    -- Slice A writes null on every row.
    pathology          text,

    latitude           double precision NOT NULL,
    longitude          double precision NOT NULL,
    speed_kmh          double precision,
    heading_deg        double precision,

    _recorded_at       timestamptz NOT NULL DEFAULT now()
);

-- The declared grain. A duplicate here is a bug in OUR recorder, not evidence about a device,
-- so unlike pings (docker/oltp/init.sql:236-238) this constraint belongs.
CREATE UNIQUE INDEX IF NOT EXISTS intended_pings_run_vehicle_seq_uk
    ON {SCHEMA}.intended_pings (run_id, vehicle_id, sequence_no);

-- The diff joins on ping_id (the primary key, already indexed) and bounds per run on
-- intended_device_ts. This index serves the bound.
CREATE INDEX IF NOT EXISTS intended_pings_run_device_ts_idx
    ON {SCHEMA}.intended_pings (run_id, intended_device_ts);

-- NO HYPERTABLE, deliberately. TimescaleDB is absent in CI (PostGIS image only), so an
-- unguarded create_hypertable would pass locally and fail there. Revisit when the row count
-- justifies it; 190k is not that.
"""


def apply(conn: Connection) -> None:
    """Create anything missing. Safe to call on every startup, and it is."""
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        cur.execute(DDL)
    conn.commit()


def main() -> int:
    """`python -m fleet_telemetry.truth` -- also what CI runs before dbt build."""
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    target = config.warehouse()
    print(f"applying truth schema to {target.safe_dsn()}")
    with connect(target.dsn()) as conn:
        apply(conn)
    print(f"truth tables present: {', '.join(TABLES)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Add the schema to `docker/warehouse/init.sql`**

Insert after the `marts` block (currently ending ~line 41), matching the existing comment style:

```sql
-- What the simulator intended to emit, written by the simulator itself and read only by tests.
-- Not a medallion layer: it is the reference the layers are checked against, which is why it
-- sits outside bronze/silver/gold/marts rather than inside bronze as "another raw feed".
-- Tables are owned by src/fleet_telemetry/truth.py, per the rule at the top of this file.
CREATE SCHEMA IF NOT EXISTS truth;
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `docker compose -f docker/docker-compose.yml up -d warehouse && python -m fleet_telemetry.truth && python -m pytest tests/test_truth_load.py -v`
Expected: PASS, 2 tests. Also expect `truth tables present: intended_pings` on stdout.

- [ ] **Step 6: Commit**

```bash
git add src/fleet_telemetry/truth.py docker/warehouse/init.sql tests/test_truth_load.py
git commit -m "Add the truth schema, typed on purpose against Bronze's rule"
```

---

## Task 2: The row shape and the writer

**Files:**
- Modify: `src/fleet_telemetry/truth.py`
- Test: `tests/test_truth.py` (create), `tests/test_truth_load.py` (extend)

**Interfaces:**
- Consumes: Task 1's `SCHEMA`
- Produces: `IntendedPing` (NamedTuple), `from_reading(reading: dict, *, run_id: str, seed: int, truth_ts: datetime, emitted: bool = True, pathology: str | None = None) -> IntendedPing`, `write(conn: Connection, rows: list[IntendedPing]) -> int`

- [ ] **Step 1: Write the failing hermetic test**

```python
# tests/test_truth.py
"""The mapper and the recorder. No database, no HTTP, no docker."""

from __future__ import annotations

from datetime import UTC, datetime

from fleet_telemetry import truth

READING = {
    "ping_id": "11111111-1111-4111-8111-111111111111",
    "vehicle_id": 7,
    "sequence_no": 42,
    "device_ts": "2026-08-28T10:00:00+00:00",
    "latitude": 13.4549,
    "longitude": -16.5790,
    "speed_kmh": 45.0,
    "heading_deg": 91.5,
}
RUN_ID = "22222222-2222-4222-8222-222222222222"
TRUTH_TS = datetime(2026, 8, 28, 10, 0, tzinfo=UTC)


def test_from_reading_copies_the_wire_values_verbatim():
    """Verbatim matters: the diff compares for equality, so any transformation here is a
    difference the pipeline did not cause."""
    row = truth.from_reading(READING, run_id=RUN_ID, seed=42, truth_ts=TRUTH_TS)

    assert row.ping_id == READING["ping_id"]
    assert row.vehicle_id == 7
    assert row.sequence_no == 42
    assert row.latitude == 13.4549
    assert row.longitude == -16.5790
    assert row.speed_kmh == 45.0
    assert row.heading_deg == 91.5


def test_intended_device_ts_is_the_claim_and_truth_ts_is_the_fact():
    """Identical in a clean run. The columns are separate so slice B's clock skew is measurable
    rather than merely visible."""
    skewed = READING | {"device_ts": "2026-08-28T13:00:00+00:00"}
    row = truth.from_reading(skewed, run_id=RUN_ID, seed=42, truth_ts=TRUTH_TS)

    assert row.intended_device_ts == datetime(2026, 8, 28, 13, 0, tzinfo=UTC)
    assert row.truth_ts == TRUTH_TS


def test_clean_rows_are_emitted_with_no_pathology():
    row = truth.from_reading(READING, run_id=RUN_ID, seed=42, truth_ts=TRUTH_TS)
    assert row.emitted is True
    assert row.pathology is None


def test_a_dropped_row_is_still_recorded():
    """Slice B's sequence gap: the intent was formed, the send was not made. Recording it is the
    only thing that makes the gap provably the injection."""
    row = truth.from_reading(
        READING, run_id=RUN_ID, seed=42, truth_ts=TRUTH_TS, emitted=False, pathology="sequence_gap"
    )
    assert row.emitted is False
    assert row.pathology == "sequence_gap"


def test_insert_columns_match_the_row_shape_exactly():
    """The one failure mode Postgres cannot catch.

    write() builds each parameter tuple by getattr over _COLUMNS, so a mismatch between that list
    and IntendedPing's fields is either a dropped column or an AttributeError. This pins them
    together so the failure is here, in a test that runs in milliseconds, rather than in a
    warehouse full of coordinates with latitude and longitude exchanged -- adjacent columns of the
    same type, which no cast and no constraint would object to.
    """
    assert set(truth._COLUMNS) == set(truth.IntendedPing._fields)
    assert len(truth._COLUMNS) == len(truth.IntendedPing._fields)
```

Reaching into `truth._COLUMNS` is deliberate: a private name is the right home for it (the project's convention is `_`-prefixed private helpers), and the alternative — making it public purely so a test may read it — would widen the module's surface to describe an internal coupling.

- [ ] **Step 2: Run it to verify it fails**

Run: `python -m pytest tests/test_truth.py -v`
Expected: FAIL — `AttributeError: module 'fleet_telemetry.truth' has no attribute 'from_reading'`

- [ ] **Step 3: Add the shape, the mapper and the writer**

Append to `src/fleet_telemetry/truth.py`, and add `from datetime import datetime` plus `from typing import NamedTuple` to the imports:

```python
class IntendedPing(NamedTuple):
    """One reading the simulator formed the intent to emit, whether or not it went out."""

    ping_id: str
    run_id: str
    seed: int
    vehicle_id: int
    sequence_no: int
    intended_device_ts: datetime
    truth_ts: datetime
    emitted: bool
    pathology: str | None
    latitude: float
    longitude: float
    speed_kmh: float | None
    heading_deg: float | None


def from_reading(
    reading: dict,
    *,
    run_id: str,
    seed: int,
    truth_ts: datetime,
    emitted: bool = True,
    pathology: str | None = None,
) -> IntendedPing:
    """Map a take_reading() dict to a truth row, copying wire values VERBATIM.

    Verbatim is the whole contract. The diff asserts equality against silver.stg_pings, so any
    rounding, normalising or re-deriving here would manufacture a difference the pipeline did not
    cause -- and the first instinct on seeing that difference would be to loosen the test.
    simulator/run.py:222-225 already rounds (6dp on coordinates, 1dp on speed and heading); those
    rounded values ARE what the API received, so they are what truth records.
    """
    return IntendedPing(
        ping_id=reading["ping_id"],
        run_id=run_id,
        seed=seed,
        vehicle_id=reading["vehicle_id"],
        sequence_no=reading["sequence_no"],
        intended_device_ts=datetime.fromisoformat(reading["device_ts"]),
        truth_ts=truth_ts,
        emitted=emitted,
        pathology=pathology,
        latitude=reading["latitude"],
        longitude=reading["longitude"],
        speed_kmh=reading.get("speed_kmh"),
        heading_deg=reading.get("heading_deg"),
    )


# The insert's column order, as data rather than as a string, so a test can pin it against
# IntendedPing._fields. THIS GUARD IS NOT PEDANTRY. The rows go in positionally, and `latitude`
# and `longitude` are adjacent columns of the same type -- so reordering the NamedTuple would
# swap them with no error, no type mismatch and no failing cast. That is precisely the bug this
# project already paid for once: ST_MakePoint's arguments swapped passed nine of nine scripts
# with exit 0 and moved fleet total distance by +2.14% (stg_pings.sql:157-162). Postgres cannot
# catch it and neither can review.
_COLUMNS = (
    "ping_id",
    "run_id",
    "seed",
    "vehicle_id",
    "sequence_no",
    "intended_device_ts",
    "truth_ts",
    "emitted",
    "pathology",
    "latitude",
    "longitude",
    "speed_kmh",
    "heading_deg",
)


def write(conn: Connection, rows: list[IntendedPing]) -> int:
    """Insert a batch. Returns the number of rows that landed. Does NOT commit.

    The caller owns the transaction boundary, following load/writer.py:55-58 -- there because the
    Kafka offset commit must strictly follow the database one, here because the simulator decides
    whether a tick's truth and its POST succeed together.

    ON CONFLICT DO NOTHING on the primary key, so a retried flush is idempotent. Note this is a
    different judgement from the pings table, which withholds its (vehicle_id, sequence_no)
    constraint on purpose: a duplicate there is evidence about a device, whereas a duplicate here
    could only be our own recorder writing twice.
    """
    if not rows:
        return 0

    placeholders = ", ".join(["%s"] * len(_COLUMNS))
    with conn.cursor() as cur:
        cur.executemany(
            f"insert into {SCHEMA}.intended_pings ({', '.join(_COLUMNS)}) "
            f"values ({placeholders}) "
            "on conflict (ping_id) do nothing",
            [tuple(getattr(row, column) for column in _COLUMNS) for row in rows],
        )
        return cur.rowcount
```

Building the tuple by `getattr` over `_COLUMNS` rather than passing the NamedTuple directly is what makes the mapping name-based instead of order-based. The test in the next step then pins the two lists together, so a field added to `IntendedPing` and forgotten in `_COLUMNS` fails immediately rather than silently dropping a column.

- [ ] **Step 4: Run the hermetic tests to verify they pass**

Run: `python -m pytest tests/test_truth.py -v`
Expected: PASS, 5 tests.

- [ ] **Step 5: Add the round-trip integration test**

Append to `tests/test_truth_load.py`:

```python
def test_write_round_trips_every_field_exactly(warehouse):
    """Exact, not approximate. The diff compares for equality, so a lossy round trip here would
    surface later as a pipeline discrepancy that is really a storage artefact."""
    from datetime import UTC, datetime

    truth.apply(warehouse)
    run_id = "33333333-3333-4333-8333-333333333333"
    row = truth.from_reading(
        {
            "ping_id": "44444444-4444-4444-8444-444444444444",
            "vehicle_id": 9101,
            "sequence_no": 900_001,
            "device_ts": "2026-08-28T10:00:00+00:00",
            "latitude": 13.454900,
            "longitude": -16.579000,
            "speed_kmh": 45.0,
            "heading_deg": 91.5,
        },
        run_id=run_id,
        seed=42,
        truth_ts=datetime(2026, 8, 28, 10, 0, tzinfo=UTC),
    )
    try:
        assert truth.write(warehouse, [row]) == 1
        # Idempotent: the same row again lands nothing.
        assert truth.write(warehouse, [row]) == 0
        warehouse.commit()

        with warehouse.cursor() as cur:
            cur.execute(
                f"select latitude, longitude, speed_kmh, heading_deg, intended_device_ts, emitted "
                f"from {truth.SCHEMA}.intended_pings where run_id = %s",
                (run_id,),
            )
            got = cur.fetchone()
        assert got[0] == row.latitude
        assert got[1] == row.longitude
        assert got[2] == row.speed_kmh
        assert got[3] == row.heading_deg
        assert got[4] == row.intended_device_ts
        assert got[5] is True
    finally:
        with warehouse.cursor() as cur:
            cur.execute(f"delete from {truth.SCHEMA}.intended_pings where run_id = %s", (run_id,))
        warehouse.commit()
```

Note the `sequence_no` of `900_001`: it stays inside the `TEST_SEQ_BASE = 900_000` convention from `tests/test_app.py:35-37`, and the `finally` teardown is deliberate — `tests/test_app.py:47-54` records a suite that dragged the profiled lateness minimum to -208,270,649 seconds and invented three sequence gaps covering 895,683 phantom pings by not cleaning up.

- [ ] **Step 6: Run both files**

Run: `python -m pytest tests/test_truth.py tests/test_truth_load.py -v`
Expected: PASS, 8 tests — 5 hermetic, 3 integration.

- [ ] **Step 7: Commit**

```bash
git add src/fleet_telemetry/truth.py tests/test_truth.py tests/test_truth_load.py
git commit -m "Record intent verbatim: the truth row shape and its writer"
```

---

## Task 3: The recorder, with an injectable sink

**Files:**
- Modify: `src/fleet_telemetry/truth.py`
- Test: `tests/test_truth.py`

**Interfaces:**
- Consumes: Task 2's `IntendedPing`, `from_reading`, `write`
- Produces: `TruthRecorder(run_id: str, seed: int, sink: Callable[[list[IntendedPing]], int])` with `.record(reading, truth_ts, *, emitted=True, pathology=None) -> None`, `.flush() -> int`, `.recorded: int`; and `postgres_sink(conn: Connection) -> Callable[[list[IntendedPing]], int]`

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_truth.py


def test_recorder_buffers_until_flushed():
    """Buffered because the write must be batched: truth rows are 1:1 with pings, and the last
    measured run was 190k of them."""
    written = []
    rec = truth.TruthRecorder(run_id=RUN_ID, seed=42, sink=lambda rows: written.extend(rows) or len(rows))

    rec.record(READING, TRUTH_TS)
    assert written == []

    assert rec.flush() == 1
    assert len(written) == 1
    assert written[0].ping_id == READING["ping_id"]


def test_flush_is_a_no_op_when_nothing_is_buffered():
    calls = []
    rec = truth.TruthRecorder(run_id=RUN_ID, seed=42, sink=lambda rows: calls.append(rows) or len(rows))
    assert rec.flush() == 0
    assert calls == []


def test_recorder_counts_everything_it_recorded_including_unemitted():
    written = []
    rec = truth.TruthRecorder(run_id=RUN_ID, seed=42, sink=lambda rows: written.extend(rows) or len(rows))

    rec.record(READING, TRUTH_TS)
    rec.record(READING | {"ping_id": "55555555-5555-4555-8555-555555555555"}, TRUTH_TS, emitted=False)
    rec.flush()

    assert rec.recorded == 2
    assert [r.emitted for r in written] == [True, False]
```

- [ ] **Step 2: Run it to verify it fails**

Run: `python -m pytest tests/test_truth.py -v -k recorder`
Expected: FAIL — `AttributeError: module 'fleet_telemetry.truth' has no attribute 'TruthRecorder'`

- [ ] **Step 3: Implement**

Append to `src/fleet_telemetry/truth.py`, adding `from collections.abc import Callable` to the imports:

```python
def postgres_sink(conn: Connection) -> Callable[[list[IntendedPing]], int]:
    """The production sink. Commits, because the simulator has no other transaction to join."""

    def sink(rows: list[IntendedPing]) -> int:
        landed = write(conn, rows)
        conn.commit()
        return landed

    return sink


class TruthRecorder:
    """Accumulates intent and hands it to a sink in batches.

    THE SINK IS INJECTED FOR THE SAME REASON config.py's loaders take an `env` mapping
    (config.py:30-33): without that seam the only way to test the simulator's recording is to
    stand up a warehouse, so the tests that matter most would be the ones least often run.

    Recording happens when the reading is TAKEN, not when the batch is flushed. That is a slice B
    requirement arriving early: a sequence-gap row is one the simulator formed the intent to send
    and then deliberately dropped, so it never reaches a flush and could not be recorded there.
    """

    def __init__(
        self,
        run_id: str,
        seed: int,
        sink: Callable[[list[IntendedPing]], int],
    ) -> None:
        self.run_id = run_id
        self.seed = seed
        self._sink = sink
        self._buffer: list[IntendedPing] = []
        self.recorded = 0
        self.written = 0

    def record(
        self,
        reading: dict,
        truth_ts: datetime,
        *,
        emitted: bool = True,
        pathology: str | None = None,
    ) -> None:
        self._buffer.append(
            from_reading(
                reading,
                run_id=self.run_id,
                seed=self.seed,
                truth_ts=truth_ts,
                emitted=emitted,
                pathology=pathology,
            )
        )
        self.recorded += 1

    def flush(self) -> int:
        if not self._buffer:
            return 0
        landed = self._sink(self._buffer)
        self._buffer = []
        self.written += landed
        return landed
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/test_truth.py -v`
Expected: PASS, 8 tests.

- [ ] **Step 5: Commit**

```bash
git add src/fleet_telemetry/truth.py tests/test_truth.py
git commit -m "A recorder with an injectable sink, so the simulator is testable offline"
```

---

## Task 4: Wire it into the simulator

**Files:**
- Modify: `simulator/run.py` — `take_reading` call sites (`:309`, `:384`), `simulate` (`:271-320`), `simulate_forever` (`:359`), `main` (`:471-599`)
- Test: `tests/test_simulator_backfill.py` (create)

**Interfaces:**
- Consumes: Task 3's `TruthRecorder`, `postgres_sink`
- Produces: `simulate(api, world, start, end, interval_seconds, live, truth=None)` and `simulate_forever(api, world, interval_seconds, stop, truth=None)` — both gaining one trailing keyword argument with a `None` default, so every existing caller and `StubApi` subclass keeps working

`simulate()` has **zero test coverage** today — `tests/test_simulator_forever.py` covers only `next_tick` and `simulate_forever`. This task creates that file, which is worth having regardless of this feature.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_simulator_backfill.py
"""The backfill path. No database, no HTTP, no docker.

simulate() had no test at all before this file, which is notable given what lives in it: the
transmission-delay draw, the server_ts_override logic, and the batch flush whose earlier version
backdated 169,480 rows (simulator/run.py:282-291). Every slice B pathology lands here too.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from fleet_telemetry import truth as truth_module
from simulator import run as run_module
from simulator.world import Depot, World

BANJUL = Depot(1, "Banjul", 13.4549, -16.5790)
BASSE = Depot(2, "Basse", 13.3167, -14.2167)
START = datetime(2026, 8, 28, 6, 0, tzinfo=UTC)


class StubApi:
    """Records batches instead of posting them. Deliberately not subclassing Api: this file needs
    no HTTP client, and tests/test_simulator_forever.py:101-132 already covers the
    signature-drift case by subclassing."""

    def __init__(self) -> None:
        self.batches: list[tuple[list[dict], datetime | None]] = []
        self.pings_sent = 0
        self.pings_inserted = 0

    def post_pings(self, pings, server_ts):
        self.batches.append(([dict(p) for p in pings], server_ts))
        self.pings_sent += len(pings)
        self.pings_inserted += len(pings)

    def create_job(self, *args, **kwargs):
        return 1

    def move_job(self, *args, **kwargs):
        return None

    def patch_vehicle(self, *args, **kwargs):
        return None


@pytest.fixture
def world():
    return World([BANJUL, BASSE], [1, 2, 3], START, seed=42)


def test_every_emitted_ping_has_exactly_one_truth_row(world):
    """The property the whole harness exists for. If this can drift, the diff is measuring the
    recorder rather than the pipeline."""
    api = StubApi()
    written: list = []
    recorder = truth_module.TruthRecorder(
        run_id="66666666-6666-4666-8666-666666666666",
        seed=42,
        sink=lambda rows: written.extend(rows) or len(rows),
    )

    run_module.simulate(
        api, world, START, START + timedelta(minutes=1), 5, live=False, truth=recorder
    )

    posted = [p for batch, _ in api.batches for p in batch]
    assert len(posted) > 0
    assert len(written) == len(posted)
    assert {r.ping_id for r in written} == {p["ping_id"] for p in posted}


def test_truth_records_the_wire_values_not_recomputed_ones(world):
    api = StubApi()
    written: list = []
    recorder = truth_module.TruthRecorder(
        run_id="77777777-7777-4777-8777-777777777777",
        seed=42,
        sink=lambda rows: written.extend(rows) or len(rows),
    )

    run_module.simulate(
        api, world, START, START + timedelta(seconds=10), 5, live=False, truth=recorder
    )

    by_id = {r.ping_id: r for r in written}
    for batch, _ in api.batches:
        for ping in batch:
            row = by_id[ping["ping_id"]]
            assert row.latitude == ping["latitude"]
            assert row.longitude == ping["longitude"]
            assert row.sequence_no == ping["sequence_no"]
            assert row.intended_device_ts.isoformat() == ping["device_ts"]


def test_simulate_runs_unchanged_without_a_recorder(world):
    """The flag is opt-in, so the default path must not require a warehouse."""
    api = StubApi()
    stats = run_module.simulate(api, world, START, START + timedelta(minutes=1), 5, live=False)
    assert stats["pings"] > 0
```

- [ ] **Step 2: Run it to verify it fails**

Run: `python -m pytest tests/test_simulator_backfill.py -v`
Expected: FAIL — `TypeError: simulate() got an unexpected keyword argument 'truth'`

- [ ] **Step 3: Thread the recorder through `simulate`**

In `simulator/run.py`, change the signature (currently `:271-278`) to add one trailing argument, and record inside the per-vehicle loop:

```python
def simulate(
    api: Api,
    world: World,
    start: datetime,
    end: datetime,
    interval_seconds: int,
    live: bool,
    truth: TruthRecorder | None = None,
) -> dict[str, int]:
    """Walk the clock from start to end, emitting pings and job transitions."""
```

Inside `flush()` (currently `:293-305`), record the truth batch **before** the POST:

```python
    def flush() -> None:
        nonlocal buffer, buffer_max_device_ts
        if not buffer:
            return
        server_ts = None
        if not live and buffer_max_device_ts is not None:
            server_ts = buffer_max_device_ts + timedelta(
                seconds=world.rng.randint(*TRANSMISSION_DELAY_SECONDS)
            )
        # Truth first, then the POST. A crash between them leaves a recorded intent with no
        # ping, and the diff correctly reports it -- the emission genuinely did not happen. The
        # other order would lose the record of a ping that DID go out, which is the failure
        # that matters. Same reasoning as the two commits in ingest/consumer.py:1-17.
        if truth is not None:
            truth.flush()
        api.post_pings(buffer, server_ts)
        stats["pings"] += len(buffer)
        buffer = []
        buffer_max_device_ts = None
```

And in the tick loop (currently `:307-312`):

```python
    while now < end:
        for vehicle in world.vehicles:
            reading = take_reading(vehicle, world, now)
            # Recorded at reading time, not flush time. A slice B sequence-gap row is dropped
            # before it ever reaches a batch, so a flush-time recorder could not see it.
            if truth is not None:
                truth.record(reading, now)
            buffer.append(reading)
            buffer_max_device_ts = now
            if len(buffer) >= PINGS_PER_BATCH:
                flush()
```

Add the import at the top of `simulator/run.py`, beside the existing `from fleet_telemetry import config`:

```python
from fleet_telemetry.truth import TruthRecorder
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m pytest tests/test_simulator_backfill.py -v`
Expected: PASS, 3 tests.

- [ ] **Step 5: Do the same for `simulate_forever`**

Change the signature (currently `:359-364`) to `simulate_forever(api, world, interval_seconds, stop, truth=None)`, and replace the single-line tick (currently `:384`):

```python
        readings = [take_reading(v, world, now) for v in world.vehicles]
        if truth is not None:
            for reading in readings:
                truth.record(reading, now)
            truth.flush()
        api.post_pings(readings, None)
```

- [ ] **Step 6: Run the forever tests to confirm nothing regressed**

Run: `python -m pytest tests/test_simulator_forever.py -v`
Expected: PASS, unchanged. Especially `tests/test_simulator_forever.py:242-253` — forever mode must still never send `server_ts_override`.

- [ ] **Step 7: Add the `--truth` flag, the run_id and the banner**

In `main` (`simulator/run.py:471-599`), after the `--reset` argument (`:501`):

```python
    parser.add_argument(
        "--truth",
        action="store_true",
        help=(
            "record every intended emission to truth.intended_pings in the warehouse, so "
            "pipeline output can be diffed against ground truth. Requires the warehouse to be "
            "up; off by default so the simulator keeps working with only oltp and the api."
        ),
    )
```

After the world is constructed and sequence numbers resumed (`:539-544`), build the recorder. The connection is opened here and closed with the run, mirroring how `fetch_depots` (`:81`) imports psycopg lazily so the module imports without a database:

```python
    recorder = None
    truth_conn = None
    if args.truth:
        from psycopg import connect as pg_connect

        from fleet_telemetry import truth as truth_module

        run_id = str(uuid.uuid4())
        target = config.warehouse()
        truth_conn = pg_connect(target.dsn())
        truth_module.apply(truth_conn)
        recorder = truth_module.TruthRecorder(
            run_id=run_id,
            seed=args.seed,
            sink=truth_module.postgres_sink(truth_conn),
        )
        print(f"  truth  : {target.safe_dsn()} run_id={run_id}")
```

Pass `truth=recorder` at both dispatch sites (`:569-572`), and in the `finally` / end-of-run path flush and close:

```python
    if recorder is not None:
        recorder.flush()
        print(f"  truth rows     : {recorder.written:,} written of {recorder.recorded:,} recorded")
    if truth_conn is not None:
        truth_conn.close()
```

Note `uuid` is already imported at `simulator/run.py:52`.

- [ ] **Step 8: Wire the compose service**

In `docker/docker-compose.yml`, the `simulator` service currently receives only `OLTP_*` (`:301-311`) and depends only on `api` (`:298-300`). Add:

```yaml
    depends_on:
      api:
        condition: service_healthy
      warehouse:
        condition: service_healthy
    environment:
      # ... existing OLTP_* and FLEET_* entries unchanged ...
      # Needed only by --truth. Harmless when the flag is off: nothing opens the connection.
      WAREHOUSE_HOST: warehouse
      WAREHOUSE_PORT: 5432
      WAREHOUSE_DB: ${WAREHOUSE_DB:-telemetry}
      WAREHOUSE_USER: ${WAREHOUSE_USER:-telemetry}
      WAREHOUSE_PASSWORD: ${WAREHOUSE_PASSWORD:-telemetry}
```

Container-side port is `5432`, per the convention note in that file.

- [ ] **Step 9: Run the full hermetic suite and lint**

Run: `python -m pytest -m "not integration" -q && python -m ruff check . && python -m ruff format --check .`
Expected: PASS, no lint findings.

- [ ] **Step 10: Commit**

```bash
git add simulator/run.py tests/test_simulator_backfill.py docker/docker-compose.yml
git commit -m "Record intended emissions from the simulator behind --truth"
```

---

## Task 5: CI

**Files:**
- Modify: `.github/workflows/ci.yml` — the `dbt` job

`dbt build` runs source tests **eagerly**, so a `not_null` test against a table that does not exist fails the build rather than reporting anything useful (`dbt/models/staging/_sources.yml:6-9`). The `truth` schema and table must therefore exist in CI before Task 6's source declaration lands. Do this task first so the source declaration never sees a red build.

- [ ] **Step 1: Add `truth` to the schema-creation step**

```yaml
      - name: Create medallion schemas
        run: |
          PGPASSWORD=telemetry psql -h localhost -U telemetry -d telemetry \
            -c "CREATE SCHEMA IF NOT EXISTS bronze;" \
            -c "CREATE SCHEMA IF NOT EXISTS silver;" \
            -c "CREATE SCHEMA IF NOT EXISTS gold;" \
            -c "CREATE SCHEMA IF NOT EXISTS marts;" \
            -c "CREATE SCHEMA IF NOT EXISTS truth;"
```

- [ ] **Step 2: Add the truth-table step beside the bronze one**

Immediately after the existing "Apply the bronze schema" step:

```yaml
      # Same reasoning as bronze above: truth is owned by src/fleet_telemetry/truth.py, not by
      # dbt, so the table does not exist until this runs -- and dbt build evaluates source tests
      # eagerly, so the not_null tests on it would fail the build rather than report anything.
      # Empty is fine: not_null and the grain assertion hold vacuously and would still catch a
      # column that went missing.
      - name: Apply the truth schema
        run: python -m fleet_telemetry.truth
```

- [ ] **Step 3: Verify the workflow parses**

Run: `python -c "import yaml,pathlib; yaml.safe_load(pathlib.Path('.github/workflows/ci.yml').read_text(encoding='utf-8')); print('ok')"`
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "Create the truth schema in CI before dbt evaluates its source tests"
```

---

## Task 6: Declare the dbt source and assert its grain

**Files:**
- Modify: `dbt/models/staging/_sources.yml`
- Create: `dbt/tests/assert_intended_pings_unique_on_run_vehicle_and_sequence.sql`

This is the project's **second** source and its first non-`bronze` one. Confirmed safe: `dbt/macros/generate_schema_name.sql` applies only to nodes dbt *builds*, so a source's schema comes from its `schema:` key verbatim — which is why the existing source already resolves to bare `bronze` rather than `silver_bronze`.

- [ ] **Step 1: Append the source entry**

At the end of `dbt/models/staging/_sources.yml`, as a second item under the existing `sources:` key (2-space indent for the `-`, matching the `bronze` entry):

```yaml
  - name: truth
    schema: truth
    description: >
      What the simulator INTENDED to emit, written by the simulator itself
      (src/fleet_telemetry/truth.py) rather than by any ingestion path. Not a medallion layer:
      it is the reference the layers are checked against, which is what makes a correctness
      claim falsifiable instead of merely plausible.

      EVERY COLUMN HERE IS TYPED, the exact opposite of the all-text rule at the top of this
      file, and the inversion is deliberate. Bronze is text because it must never reject the
      malformed evidence it exists to keep. These values are constructed by our own code
      immediately before the insert, so a cast that fails is a bug in the recorder and the
      loudest possible failure is what we want.

    tables:
      - name: intended_pings
        description: >
          One row per reading the simulator formed the intent to send, whether or not it went
          out. Only populated by runs started with `--truth`; a run without the flag leaves no
          rows and the diff simply has nothing to compare, which is why it holds vacuously in
          CI and on a fresh clone.
        columns:
          - name: ping_id
            description: >
              The device-generated UUID, and the ONLY join key into silver.stg_pings. It is an
              unseeded uuid4 (simulator/run.py:218), so it cannot be recomputed from the seed --
              a run that did not record it is unverifiable afterwards.
            tests:
              - not_null
              - unique
          - name: run_id
            description: >
              Which simulator process produced this row. Load-bearing for the diff's bound: two
              same-seed runs differ only in ping_id, sequence numbers resume from the database
              rather than restarting, and two backfill windows OVERLAP in device_ts because each
              walks from now-span to now. Without run_id there is no way to tell one run's
              arrival frontier from another's.
            tests: [not_null]
          - name: sequence_no
            description: >
              What the device claimed as its counter. Part of the declared grain together with
              run_id and vehicle_id, asserted in
              tests/assert_intended_pings_unique_on_run_vehicle_and_sequence.sql.
            tests: [not_null]
          - name: intended_device_ts
            description: >
              The device_ts value actually put on the wire. Compared for EQUALITY against
              silver.stg_pings.device_ts, so it must never be re-derived or re-rounded.
            tests: [not_null]
          - name: truth_ts
            description: >
              The real instant the reading describes. Identical to intended_device_ts on a clean
              run; the two diverge the moment the clock-skew flag exists. Recording both is what
              makes skew measurable rather than merely visible.
            tests: [not_null]
          - name: emitted
            description: >
              False when the intent was formed and deliberately not sent -- the sequence-gap
              pathology. Always true today. The diff filters on it, so an unemitted row is
              correctly NOT expected to appear in Silver.
            tests: [not_null]
          - name: pathology
            description: >
              Which flag caused this row to diverge from clean behaviour; null when clean, which
              is every row today. No not_null test, for the obvious reason.
```

- [ ] **Step 2: Write the grain assertion**

```sql
-- dbt/tests/assert_intended_pings_unique_on_run_vehicle_and_sequence.sql
-- The declared grain: one row per (run_id, vehicle_id, sequence_no).
--
-- UNLIKE EVERY OTHER GRAIN ASSERTION IN THIS DIRECTORY, THIS ONE GUARDS OUR OWN WRITES RATHER
-- THAN A MODEL'S DEDUPLICATION. There is no DISTINCT ON upstream of it and no GROUP BY -- the
-- rows are inserted one per take_reading() call, so a duplicate here means the recorder wrote
-- the same reading twice, which would silently inflate the denominator of every ground-truth
-- comparison downstream.
--
-- It is therefore also asserted in the database, as a unique index in
-- src/fleet_telemetry/truth.py. That is a departure from how pings is treated, where the
-- (vehicle_id, sequence_no) constraint is deliberately WITHHELD (docker/oltp/init.sql:236-238)
-- so the warehouse can detect an anomalous device rather than the app rejecting the evidence.
-- The difference is who wrote the row: a duplicate ping is evidence about a device, whereas a
-- duplicate truth row could only be our own bug.
--
-- Belt and braces on purpose. The index fails the simulator at the write; this fails the build.
-- The index cannot catch a row inserted by hand into a schema without it, which is exactly the
-- state a warehouse volume created before this ticket is in.
--
-- Holds vacuously on an empty table, which is its state in CI and on any clone that has not run
-- the simulator with --truth.

SELECT run_id,
       vehicle_id,
       sequence_no,
       count(*) AS row_count
  FROM {{ source('truth', 'intended_pings') }}
 GROUP BY run_id, vehicle_id, sequence_no
HAVING count(*) > 1
```

- [ ] **Step 3: Parse and build**

Run: `python -m dbt.cli.main parse --project-dir dbt --profiles-dir dbt && python -m dbt.cli.main build --project-dir dbt --profiles-dir dbt`
Expected: PASS. The new source tests and the grain assertion run against the (possibly empty) table and pass.

- [ ] **Step 4: Confirm the source resolved to bare `truth`, not a concatenation**

Run:
```bash
python -c "import json,pathlib; m=json.loads(pathlib.Path('dbt/target/manifest.json').read_text(encoding='utf-8')); print(sorted({v['schema'] for v in m['sources'].values()}))"
```
Expected: `['bronze', 'truth']`. If it prints `silver_truth`, `generate_schema_name.sql` is being applied to sources — stop and investigate, because that macro's header says it should not be.

- [ ] **Step 5: Commit**

```bash
git add dbt/models/staging/_sources.yml dbt/tests/assert_intended_pings_unique_on_run_vehicle_and_sequence.sql
git commit -m "Declare truth as dbt's second source, with its grain asserted"
```

---

## Task 7: The diff

**Files:**
- Create: `dbt/tests/assert_intended_pings_reached_silver.sql`

- [ ] **Step 1: Write the test**

```sql
-- dbt/tests/assert_intended_pings_reached_silver.sql
-- Every ping the simulator intended to send arrived in Silver, unchanged.
--
-- THIS IS THE ONE ASSERTION THE WHOLE PROJECT RESTS ON. Every other test here proves an internal
-- property: a grain holds, a geometry matches its own coordinates, a reconciliation balances
-- against Bronze. All of them can pass while the pipeline quietly drops or mangles data, because
-- Bronze is the earliest thing they can see and Bronze is downstream of the loss. This one
-- compares against what the simulator KNOWS it generated, which is the only reference outside the
-- pipeline. docs/superpowers/specs/2026-08-07-telemetry-platform-design.md:236-237 calls it "the
-- verification the previous project could not have."
--
-- MUST RETURN ZERO ROWS ON A CLEAN RUN, AND THAT IS WHY IT WAS BUILT BEFORE ANY PATHOLOGY FLAG
-- EXISTED. A diff harness first exercised against deliberately dirty data cannot distinguish an
-- injected pathology from a bug in itself. Validated while the answer is known, it can.
--
-- ------------------------------------------------------------------------------------------
-- THE BOUND, AND WHY THE OBVIOUS ONE IS WRONG
-- ------------------------------------------------------------------------------------------
--
-- Truth is always AHEAD of Silver: a recorded intent has to cross the API, the OLTP, Debezium,
-- Kafka and Bronze before a Silver view can see it. So an unbounded diff always fails, and every
-- failure is in-flight data rather than loss -- exactly the flapping that
-- tests/assert_ping_rows_are_modelled_or_rejected.sql:48-52 describes, where a hand-run
-- comparison showed a 60-row gap that was entirely the seconds between two queries. A test that
-- fails for reasons unrelated to correctness gets "fixed" by loosening it.
--
-- The obvious bound -- `intended_device_ts < (SELECT max(device_ts) FROM stg_pings)` -- is WRONG
-- HERE, and quietly so. That maximum is taken over ALL of Silver, including the ~190k pings from
-- runs predating this table. A backfill run walks its clock from `now - span` to `now`
-- (simulator/run.py:535-537), so its window can end BEFORE the historical maximum -- in which
-- case the bound admits every truth row including the ones still in flight, and the test fails on
-- a correct pipeline. Bronze is append-only, so `--reset` cannot clear the history either: it
-- deletes OLTP pings only (simulator/run.py:129-135).
--
-- The bound is therefore a PER-RUN ARRIVAL FRONTIER, derived only from rows that actually landed
-- for that run: max(device_ts) over the Silver rows whose ping_id matches one of this run's truth
-- rows. A run with nothing landed yet has a null frontier and is skipped rather than failed.
--
-- STRICT `<`, NOT `<=`. Every ping in one tick carries the same device_ts, and a batch flush
-- happens at PINGS_PER_BATCH = 500 regardless of tick boundaries -- with 40 vehicles that is 12.5
-- ticks per batch, so a flush lands mid-tick and the frontier tick is routinely half-arrived.
-- Excluding it costs one tick of coverage and removes the entire class of false failure.
--
-- WHAT THIS DOES NOT PROVE: that Silver holds no rows truth never recorded. The reverse direction
-- is not assertable while pre-truth history exists -- every one of those ~190k pings would be
-- reported. Stated rather than faked, because a reconciliation that has to be loosened once is
-- decorative from then on. When the reverse direction matters, it needs a warehouse whose Bronze
-- was empty when truth recording began.
--
-- WHAT THIS PROVES IN CI: nothing. truth.intended_pings is empty there, every frontier is null,
-- and this returns no rows. Said out loud because a green check reads like coverage and is not --
-- what CI catches is that the SQL parses and the columns exist.
--
-- FLOAT EQUALITY IS DELIBERATE. The recorder stores the values that went on the wire verbatim,
-- already rounded by simulator/run.py:222-225, and Silver casts the same JSON text back to double
-- precision. If this ever flags a coordinate mismatch, that is a real finding about the JSON round
-- trip and not a reason to introduce a tolerance -- diagnose it before loosening it.

WITH frontier AS (
    SELECT i.run_id,
           max(s.device_ts) AS arrived_through
      FROM {{ source('truth', 'intended_pings') }} i
      JOIN {{ ref('stg_pings') }} s ON s.ping_id = i.ping_id
     GROUP BY i.run_id
),

expected AS (
    SELECT i.*
      FROM {{ source('truth', 'intended_pings') }} i
      JOIN frontier f ON f.run_id = i.run_id
     WHERE i.emitted
       AND f.arrived_through IS NOT NULL
       AND i.intended_device_ts < f.arrived_through
),

compared AS (
    SELECT e.run_id,
           e.ping_id,
           e.vehicle_id,
           e.sequence_no,
           e.intended_device_ts,
           s.ping_id     AS silver_ping_id,
           s.device_ts   AS silver_device_ts,
           e.latitude    AS intended_latitude,
           s.latitude    AS silver_latitude,
           e.longitude   AS intended_longitude,
           s.longitude   AS silver_longitude,
           e.speed_kmh   AS intended_speed_kmh,
           s.speed_kmh   AS silver_speed_kmh,
           e.heading_deg AS intended_heading_deg,
           s.heading_deg AS silver_heading_deg
      FROM expected e
      LEFT JOIN {{ ref('stg_pings') }} s ON s.ping_id = e.ping_id
)

SELECT * FROM compared
 WHERE silver_ping_id IS NULL
    OR silver_device_ts   IS DISTINCT FROM intended_device_ts
    OR silver_latitude    IS DISTINCT FROM intended_latitude
    OR silver_longitude   IS DISTINCT FROM intended_longitude
    OR silver_speed_kmh   IS DISTINCT FROM intended_speed_kmh
    OR silver_heading_deg IS DISTINCT FROM intended_heading_deg
```

- [ ] **Step 2: Verify it holds vacuously before any data exists**

Run: `python -m dbt.cli.main build --project-dir dbt --profiles-dir dbt --select "assert_intended_pings_reached_silver"`
Expected: PASS with 0 rows (the table is empty, so every frontier is null).

- [ ] **Step 3: Prove it against a real clean run — the acceptance gate for this whole slice**

```bash
docker compose -f docker/docker-compose.yml up -d
python -m fleet_telemetry.truth
FLEET_ALLOW_SERVER_TS_OVERRIDE=true uvicorn app.main:app --port 8000   # separate shell
python -m simulator --vehicles 40 --minutes 30 --truth                  # separate shell
python -m fleet_telemetry.ingest.consumer --max-batches 500
python -m dbt.cli.main build --project-dir dbt --profiles-dir dbt
```

On PowerShell set the env var separately — there is no inline prefix.

Expected: `assert_intended_pings_reached_silver` PASSES with 0 rows, and the simulator's summary reports `truth rows written` equal to `pings sent`.

- [ ] **Step 4: Prove the test can actually fail**

A test never seen red is not known to work. Delete a landed row from Bronze, rebuild, confirm the diff catches it, then restore:

```bash
python -c "
import psycopg
from fleet_telemetry import config
with psycopg.connect(config.warehouse().dsn()) as c, c.cursor() as cur:
    cur.execute('''select _kafka_partition, _kafka_offset from bronze.raw_ping_events
                   where ping_id = (select i.ping_id from truth.intended_pings i
                                    join silver.stg_pings s on s.ping_id = i.ping_id
                                    order by i.intended_device_ts limit 1)''')
    print(cur.fetchone())
"
```

Take that coordinate, delete the row, run the single test, and expect **FAIL** with a `silver_ping_id IS NULL` row. Then re-run the consumer to re-ingest, or restore from a dump. Record the observed failure output in `docs/truth-harness.md` — a documented red is what makes the green meaningful.

- [ ] **Step 5: Commit**

```bash
git add dbt/tests/assert_intended_pings_reached_silver.sql
git commit -m "Diff Silver against recorded intent, bounded by a per-run arrival frontier"
```

---

## Task 8: Document it

**Files:**
- Create: `docs/truth-harness.md`
- Modify: `README.md`, `CLAUDE.md`
- Create: `docs/superpowers/specs/2026-08-28-dirty-data-analysis.md` (the analysis this plan implements)

- [ ] **Step 1: Write `docs/truth-harness.md`**

Follow the shape of `docs/silver-in-dbt.md` — what was built, what it measured, what it does **not** prove, and what is still open. Include: the measured row counts from Task 7 step 3; the observed red output from Task 7 step 4; the bound derivation and why the naive `max(device_ts)` bound fails; the un-assertable reverse direction; and the six open questions carried forward from the analysis.

- [ ] **Step 2: Add the commands to `CLAUDE.md`**

Under "Transformation (phase 3)", add:

```bash
python -m fleet_telemetry.truth                       # truth schema DDL, idempotent
python -m simulator --vehicles 40 --hours 6 --truth   # record intent while emitting
```

And add a Key Patterns entry:

> **Ground truth is typed where Bronze is text, and the inversion is the point.** Bronze is all
> `text` because a cast in `GENERATED ALWAYS` runs on INSERT and would let one malformed device
> reject a whole batch. `truth.intended_pings` is written by our own code three lines before the
> insert, so a failing cast there is a bug we want loudly. Never "align" the two.
>
> **The truth diff is bounded by a per-run arrival frontier, not by `max(device_ts)`.** Truth is
> always ahead of Silver by one pipeline traversal, so an unbounded diff always fails on
> in-flight data. The obvious bound is worse than useless: `max(device_ts)` spans all of Silver
> including pre-truth history, and a backfill run generates `device_ts` in the past, so the bound
> can sit past the run's own window and admit rows that have not arrived. The frontier is derived
> per `run_id` from rows that actually matched, with a strict `<` because a tick's pings share one
> `device_ts` and a 500-ping flush lands mid-tick.

- [ ] **Step 3: Add to `README.md`** — one line in the quickstart showing the `--truth` flag.

- [ ] **Step 4: Commit and open the PR**

```bash
git add docs/ README.md CLAUDE.md
git commit -m "Document the truth harness, including what it does not prove"
git push -u origin phase-3/truth-harness
```

---

## Dependencies

- **No new packages.** psycopg 3 is already in `[app]` and `[warehouse]`; dbt-core is in `[warehouse]`. Nothing is added to `pyproject.toml`.
- **New cross-module dependency:** `simulator/` gains an import from `fleet_telemetry.truth`, and with it **write** access to the warehouse — which it has never had. It already imports `fleet_telemetry.config` and reads OLTP directly (`simulator/run.py:81, 97, 108, 129`), so direct database access has precedent; the new surface is the warehouse specifically. Note this in the module docstring.
- **Compose:** the `simulator` service gains `WAREHOUSE_*` env and a `depends_on: warehouse` edge, so `docker compose up simulator` now waits on the warehouse being healthy even when `--truth` is off.
- **Migration:** additive only. One new schema, one new table, no existing table changes shape, no backfill. An existing warehouse volume gets the schema from `apply()` rather than from `init.sql`, which only runs on an empty data directory.
- **Data impact:** truth rows are 1:1 with pings — roughly 190k for a 6-hour/40-vehicle backfill, against a ~22.2M/30-day design target. Two indexes are created. No retention policy in this slice; flag it in `docs/truth-harness.md` as open.
- **Not blocked on, and does not block:** the six open stakeholder questions from the analysis all concern slices B and C, except one that this slice settles by construction — `run_id` is per simulator process.

## Verification

End-to-end, from a clean stack:

```bash
docker compose -f docker/docker-compose.yml up -d
python -m fleet_telemetry.load.schema
python -m fleet_telemetry.truth
python -m fleet_telemetry.ingest.connector --register
```

Then, in separate shells (PowerShell needs the env var set separately — there is no inline prefix):

```bash
FLEET_ALLOW_SERVER_TS_OVERRIDE=true uvicorn app.main:app --port 8000
python -m simulator --vehicles 40 --minutes 30 --reset --truth
python -m fleet_telemetry.ingest.consumer --max-batches 500
python -m dbt.cli.main build --project-dir dbt --profiles-dir dbt
```

**Acceptance criteria:**

| Check | Expected |
| --- | --- |
| `assert_intended_pings_reached_silver` | **0 rows.** This is the gate for the slice. |
| `assert_intended_pings_unique_on_run_vehicle_and_sequence` | 0 rows |
| Simulator summary | `truth rows written` == `pings sent` |
| The deliberate break (Task 7 step 4) | The diff **FAILS**, naming the deleted `ping_id` |
| `python -m pytest -m "not integration"` | PASS — includes 11 new hermetic tests (8 in `test_truth.py`, 3 in `test_simulator_backfill.py`) |
| `python -m pytest -m integration` | PASS — includes 3 new integration tests in `test_truth_load.py` |
| `python -m pytest tests/test_simulator_forever.py` | PASS, unchanged |
| `python -m pytest tests/test_world.py` | PASS, unchanged |
| `python -m ruff check . && python -m ruff format --check .` | clean |
| dbt manifest source schemas | `['bronze', 'truth']`, never `silver_truth` |
| A run **without** `--truth` | works with the warehouse stopped; no connection opened |
| CI | all three jobs green |

**The one check that is not a command:** re-read `assert_intended_pings_reached_silver.sql`'s header against what Task 7 step 4 actually printed. If the observed failure does not look like what the header claims it would, the header is wrong and that is the finding.
