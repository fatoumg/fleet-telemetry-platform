# Phase 2 Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Populate `bronze.*` from the OLTP database by two paths — a naive batch poller and Debezium CDC via Redpanda — and write down the diff between what each captured from the same simulator run.

**Architecture:** The poller reads `WHERE updated_at > watermark` on a loop and lands current-state row images in `bronze.poll_rows`. The CDC path runs Debezium inside Kafka Connect against the OLTP write-ahead log, publishing to Redpanda; a Python consumer lands verbatim change envelopes in `bronze.raw_ping_events` / `raw_cdc_entities` / `raw_job_events`. Both paths write bronze; neither transforms. The shape difference between the two output tables *is* the deliverable.

**Tech Stack:** Python 3.13, `confluent-kafka` (already declared, `pyproject.toml:35`), `psycopg[binary]`, Redpanda (Kafka API), Debezium 3.x on Kafka Connect, Postgres 16 logical decoding via `pgoutput`, dbt-postgres for the source contract.

---

## Context

Phase 1 is complete: the source system exists and has been measured. Phase 2 answers *"How do I get data out, without losing or duplicating any?"* (`docs/learn/README.md`, phase table).

The design prescribes naive-first (`docs/superpowers/specs/2026-08-07-telemetry-platform-design.md:370-376`) and makes the comparison the deliverable:

> **Deliverable:** `bronze.*` populated by both paths, and a written comparison of what each captured from the same simulator run. The diff *is* the lesson. — spec `:378-379`

The gap the poller cannot close is already measured, not hypothesised: the simulator performed **12** vehicle reassignments and the OLTP shows **8** changed rows (`docs/source-system-reference.md`, entity-churn section). Four committed changes are invisible to any poller at any frequency. Plus every `DELETE` — `DELETE /vehicles/{id}` is a hard delete on purpose (`app/main.py:318-323`; rationale in `CLAUDE.md` Key Patterns), so a deleted row leaves no `updated_at` for a watermark to find.

Bronze is load-bearing because a Kafka topic has a retention window, not a memory (spec `:257-258`; `src/fleet_telemetry/ingest/__init__.py:6-8`).

---

## Global Constraints

Copied verbatim from the repo's own rules. Every task's requirements implicitly include this section.

- **Python only moves bytes.** No transformation logic in `src/fleet_telemetry/`. Every business rule is a dbt model. (`README.md:59-61`, `CLAUDE.md` Architecture)
- **Bronze is owned by the Python loader, not by dbt.** (`dbt/profiles.yml:28`, `dbt/models/staging/_sources.yml:3-4`, `docker/warehouse/init.sql:3-5`)
- **Nothing in `docker/warehouse/init.sql` may define a table.** Bronze DDL lives in Python.
- **Bronze never rejects a row.** Malformed payloads are stored with the parse error alongside. (`src/fleet_telemetry/load/__init__.py:6-8`)
- **Config resolves env var → `.env` → built-in default, and records which.** All new settings go through `src/fleet_telemetry/config.py`; defaults must mirror `docker/docker-compose.yml` exactly. Secrets never printed.
- **Use `127.0.0.1`, not `localhost`.** Docker Desktop on Windows does not forward IPv6. (`dbt/profiles.yml:10-13`)
- **Ports are deliberately odd.** warehouse `55432`, oltp `55433`, Kafka external `19092` (`src/fleet_telemetry/config.py:86`). Container-side ports stay standard.
- **Every module starts with a docstring explaining *why*.** `from __future__ import annotations` at the top of each file. Type hints on signatures. Private helpers `_`-prefixed.
- **SQL in Python is lowercase for inline statements; DDL uses uppercase keywords** (follow `docker/oltp/init.sql`).
- **Comment density is the deliverable.** Record what broke before fixing it.
- **Tests are hermetic unless marked `integration`.** Pure-logic modules must not import `confluent_kafka` at module scope — CI installs `[dev]` only.
- **CI must stay credential-free.** New tests pass explicit env mappings rather than reading `os.environ`.
- **Line length 100**, ruff `E,W,F,I,UP,B,SIM,PTH,RUF`, double quotes.
- Before pushing: `ruff check . && pytest`.

---

## Decisions Taken (and why)

| Decision | Rationale |
| --- | --- |
| Keep Redpanda + Kafka Connect rather than embedding Debezium | Decouples failure domains. `docker/docker-compose.yml:52-54` already warns that an undrained replication slot retains WAL forever; with a broker, draining is Connect's job and a loader outage is not an OLTP disk-full incident. Also makes replay possible and consumer offsets a real configured object (spec `:382,:385`). |
| Bronze = `payload jsonb` + `GENERATED ALWAYS AS ... STORED` projections | Python parses nothing (honours `load/__init__.py:3-4`) while the columns promised at `_sources.yml:26-79` remain real columns Silver can select and the dedup index can sit on. |
| **All scalar generated columns are `text`** — refinement of the above | A cast in a generated column can *raise on INSERT*. `(payload#>>'{after,ping_id}')::uuid` fails the whole batch if a device ever sends `"banana"` — which would make Bronze reject exactly the evidence it exists to keep. Postgres 16 has no `TRY_CAST`, so no cast is safe here. `jsonb` projections (`before`, `after`) cannot fail and stay typed. Silver casts. |
| Dedup on `(_kafka_partition, _kafka_offset)`, not `ping_id` | The OLTP already killed device retries via `ON CONFLICT (ping_id) DO NOTHING` (`docker/oltp/init.sql:189-191`), so a duplicate reaching Bronze can only be a Kafka redelivery. `ping_id` would also wrongly collapse a genuine re-snapshot (`op='r'` arriving after `op='c'`). |
| Offsets committed to Kafka **after** the Postgres commit | No skip, by construction. The window between the two commits produces duplicates, which the unique index absorbs — and the suppressed count is logged per batch, so at-least-once becomes a number you watch. |
| Poller writes `bronze.poll_rows`, a separate table | It cannot produce a before-image or an op code. Forcing its output into a Debezium-shaped envelope would fabricate fields. The missing columns are the lesson. |
| `tombstones.on.delete = false` | Bronze topics are not compacted, and a tombstone carries nothing the preceding `op='d'` event lacks. The consumer still handles a null value if one appears (recorded, never dropped). |
| No `ExtractNewRecordState` SMT | It discards before-images and rewrites deletes as tombstones — the exact fields SCD Type 2 needs (`_sources.yml:53-65`, spec `:276-283`). |

---

## File Structure

**Create**

| Path | Responsibility |
| --- | --- |
| `src/fleet_telemetry/load/schema.py` | All Bronze DDL, idempotent `apply(conn)`. The single owner of the bronze schema. |
| `src/fleet_telemetry/load/writer.py` | Batch INSERT with `ON CONFLICT DO NOTHING`; returns inserted/suppressed counts. No parsing. |
| `src/fleet_telemetry/ingest/poller.py` | The naive batch poller and its watermark table. Phase 2 step 1. |
| `src/fleet_telemetry/ingest/envelope.py` | Pure logic: topic→table routing, payload decode, malformed classification. **No `confluent_kafka` import.** |
| `src/fleet_telemetry/ingest/consumer.py` | The Kafka consumer loop. Imports `confluent_kafka`. |
| `src/fleet_telemetry/ingest/connector.py` | Registers/inspects the Debezium connector over the Connect REST API using stdlib `urllib`. |
| `src/fleet_telemetry/ingest/compare.py` | Produces the poller-vs-CDC diff, every number from a query. |
| `docker/debezium/fleet-connector.json` | Connector config, one comment per key, `${OLTP_PASSWORD}` placeholder. |
| `tests/test_envelope.py` | Hermetic: routing, decode, malformed, tombstone. |
| `tests/test_poller.py` | Hermetic watermark arithmetic + `integration` poller run. |
| `tests/test_bronze_load.py` | `integration`: DDL, generated columns, dedup, restart. |
| `docs/learn/02-ingestion.md` | The curriculum guide and the written diff. |

**Modify**

| Path | Change |
| --- | --- |
| `docker/docker-compose.yml` | Add `redpanda` and `connect` services. |
| `src/fleet_telemetry/config.py` | Extend `KafkaConfig` with `consumer_group` and `connect_url`; `defaulted` becomes a `frozenset` mirroring `DatabaseConfig`. |
| `.env.example` | Document the two new Kafka settings. |
| `dbt/models/staging/_sources.yml` | Add `poll_rows`; add the `tests:` keys the file's header defers to "the same change that creates the tables". |
| `.github/workflows/ci.yml` | Apply the bronze DDL before `dbt build`, so source tests have tables. |
| `README.md`, `CLAUDE.md` | Layout + run instructions for the two new paths. |

**Branches** (`CLAUDE.md` Naming Conventions — one per phase or slice):
`phase-2/batch-poller` for Tasks 1–4, then `phase-2/cdc` for Tasks 5–12. Both off `main`.

---

## Task 1: Bronze schema, owned by Python

**Files:**
- Create: `src/fleet_telemetry/load/schema.py`
- Test: `tests/test_bronze_load.py`

**Interfaces:**
- Produces: `schema.DDL: str`, `schema.apply(conn) -> None`, `schema.BRONZE_TABLES: tuple[str, ...]`, `schema.main() -> int`

- [ ] **Step 1: Write the failing test**

`tests/test_bronze_load.py`:

```python
"""Bronze DDL and the loader, against the real warehouse.

Marked `integration` because these need the warehouse container:

    docker compose -f docker/docker-compose.yml up -d warehouse
    pytest -m integration

Not mocked, for the same reason tests/test_app.py is not: the behaviour under test is the
database's -- GENERATED ALWAYS columns, ON CONFLICT DO NOTHING, and whether a malformed
payload survives an INSERT. A mocked connection would assert we call psycopg correctly,
which is not the same thing.
"""

from __future__ import annotations

import json

import pytest

from fleet_telemetry import config
from fleet_telemetry.load import schema

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def conn():
    psycopg = pytest.importorskip("psycopg")
    try:
        connection = psycopg.connect(config.warehouse().dsn(), connect_timeout=3)
    except Exception as exc:
        pytest.skip(f"warehouse not reachable ({type(exc).__name__}); start docker compose")
    with connection:
        schema.apply(connection)
        yield connection


def test_apply_is_idempotent(conn):
    """Every startup path calls apply(); running it twice must not error or drop data."""
    schema.apply(conn)
    schema.apply(conn)
    with conn.cursor() as cur:
        cur.execute(
            "select table_name from information_schema.tables where table_schema = 'bronze'"
        )
        present = {row[0] for row in cur.fetchall()}
    assert set(schema.BRONZE_TABLES) <= present


def test_generated_columns_project_the_payload(conn):
    """Python inserts the envelope and nothing else; the columns Silver reads are SQL."""
    envelope = {
        "op": "u",
        "before": {"vehicle_id": 7, "current_driver_id": 3},
        "after": {"vehicle_id": 7, "current_driver_id": 9},
        "source": {"table": "vehicles", "ts_ms": 1754568000000},
    }
    with conn.cursor() as cur:
        cur.execute(
            "insert into bronze.raw_cdc_entities "
            "(_topic, _kafka_partition, _kafka_offset, payload) "
            "values (%s, %s, %s, %s) "
            'returning source_table, op, "before", "after", source_ts_ms',
            ("fleet.public.vehicles", 0, 1_000_001, json.dumps(envelope)),
        )
        source_table, op, before, after, source_ts_ms = cur.fetchone()
    conn.rollback()

    assert source_table == "vehicles"
    assert op == "u"
    assert before["current_driver_id"] == 3
    assert after["current_driver_id"] == 9
    # text, not bigint: see the module docstring on why no generated column may cast.
    assert source_ts_ms == "1754568000000"


def test_a_malformed_payload_is_stored_rather_than_rejected(conn):
    """Bronze keeps the evidence. A row that cannot be parsed still lands, with the error."""
    with conn.cursor() as cur:
        cur.execute(
            "insert into bronze.raw_ping_events "
            "(_topic, _kafka_partition, _kafka_offset, payload, raw_payload, parse_error) "
            "values (%s, %s, %s, %s, %s, %s) returning ping_id, raw_payload, parse_error",
            ("fleet.public.pings", 0, 2_000_001, None, "{not json", "Expecting property name"),
        )
        ping_id, raw_payload, parse_error = cur.fetchone()
    conn.rollback()

    assert ping_id is None
    assert raw_payload == "{not json"
    assert parse_error


def test_a_ping_id_that_is_not_a_uuid_still_lands(conn):
    """The reason every scalar generated column is text.

    A cast inside GENERATED ALWAYS raises on INSERT, which would make Bronze reject exactly
    the malformed evidence it exists to preserve. Postgres 16 has no TRY_CAST.
    """
    envelope = {"op": "c", "after": {"ping_id": "banana", "sequence_no": "not-a-number"}}
    with conn.cursor() as cur:
        cur.execute(
            "insert into bronze.raw_ping_events "
            "(_topic, _kafka_partition, _kafka_offset, payload) values (%s, %s, %s, %s) "
            "returning ping_id, sequence_no",
            ("fleet.public.pings", 0, 2_000_002, json.dumps(envelope)),
        )
        ping_id, sequence_no = cur.fetchone()
    conn.rollback()

    assert ping_id == "banana"
    assert sequence_no == "not-a-number"


def test_the_same_kafka_offset_cannot_land_twice(conn):
    """At-least-once delivery means a replayed message must be absorbed, not duplicated."""
    row = ("fleet.public.pings", 0, 3_000_001, json.dumps({"op": "c", "after": {}}))
    with conn.cursor() as cur:
        for _ in range(2):
            cur.execute(
                "insert into bronze.raw_ping_events "
                "(_topic, _kafka_partition, _kafka_offset, payload) values (%s, %s, %s, %s) "
                "on conflict (_kafka_partition, _kafka_offset) do nothing",
                row,
            )
        cur.execute(
            "select count(*) from bronze.raw_ping_events where _kafka_offset = %s",
            (3_000_001,),
        )
        (n,) = cur.fetchone()
    conn.rollback()

    assert n == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bronze_load.py -v -m integration`
Expected: FAIL — `ModuleNotFoundError: No module named 'fleet_telemetry.load.schema'`

- [ ] **Step 3: Write the schema module**

`src/fleet_telemetry/load/schema.py`:

```python
"""The bronze schema. Owned here, not by dbt and not by docker/warehouse/init.sql.

Three owners of one schema is how a warehouse acquires two sources of truth, so the rule in
docker/warehouse/init.sql:3-5 is absolute: that file creates extensions and schemas, nothing
else. dbt owns silver, gold and marts (dbt/profiles.yml:28). Bronze is ours.

Applied at the start of every ingest run rather than as a manual step, because a loader that
requires someone to remember a setup command is a loader that fails on a fresh clone.

TWO DESIGN POINTS WORTH THE READ.

**Python inserts the envelope and nothing else.** Every column Silver reads is declared here as
GENERATED ALWAYS AS ... STORED -- a projection over `payload`, evaluated by Postgres. That keeps
the promise in load/__init__.py ("no reshaping") while keeping the contract in
dbt/models/staging/_sources.yml honest: those really are columns, so `source()` references and
dbt tests work unchanged.

**No generated column casts.** They are all text, including `source_ts_ms` and `sequence_no`,
which are obviously numeric. A cast inside GENERATED ALWAYS is evaluated on INSERT, so a device
sending ping_id "banana" would raise and take the whole batch with it -- Bronze rejecting
precisely the malformed evidence it exists to preserve. Postgres 16 has no TRY_CAST and no way
to mark a cast non-fatal, so the only safe projection is the one that cannot fail: `#>>` returns
text or NULL, always. Silver casts, where a failure is a test result rather than data loss.

`before` and `after` stay jsonb because `->` cannot fail either, and they are quoted because
BEFORE and AFTER are keywords -- unreserved, so unquoted would work, but quoting removes any
question about how dbt's Jinja renders them.
"""

from __future__ import annotations

import argparse

from psycopg import Connection, connect

from fleet_telemetry import config

BRONZE_TABLES = (
    "raw_ping_events",
    "raw_cdc_entities",
    "raw_job_events",
    "poll_rows",
    "poll_watermarks",
)

# Columns every CDC table carries. The Kafka coordinate is the deduplication key: the OLTP
# already removed device retries with ON CONFLICT (ping_id) DO NOTHING, so a duplicate arriving
# here can only be a broker redelivery, and (partition, offset) identifies a message uniquely
# and immutably. ping_id would be wrong twice over -- it is null for deletes, and it would
# collapse a genuine re-snapshot (op='r' after op='c') into the create it re-reads.
_KAFKA_COLUMNS = """
    _topic           text        NOT NULL,
    _kafka_partition integer     NOT NULL,
    _kafka_offset    bigint      NOT NULL,
    -- Broker-assigned; null when the producer set no timestamp. Not an event time: use
    -- source.ts_ms for that, which is the OLTP commit time from the WAL.
    _kafka_timestamp bigint,
    _ingested_at     timestamptz NOT NULL DEFAULT now(),
    -- payload is null only when the bytes would not parse. raw_payload is null only when they
    -- did. The CHECK below makes "neither" impossible, so no row can arrive carrying nothing.
    payload          jsonb,
    raw_payload      text,
    parse_error      text
"""

DDL = f"""
CREATE TABLE IF NOT EXISTS bronze.raw_ping_events (
{_KAFKA_COLUMNS},
    op          text GENERATED ALWAYS AS (payload ->> 'op') STORED,
    ping_id     text GENERATED ALWAYS AS (payload #>> '{{after,ping_id}}') STORED,
    sequence_no text GENERATED ALWAYS AS (payload #>> '{{after,sequence_no}}') STORED,
    device_ts   text GENERATED ALWAYS AS (payload #>> '{{after,device_ts}}') STORED,
    server_ts   text GENERATED ALWAYS AS (payload #>> '{{after,server_ts}}') STORED,
    CONSTRAINT raw_ping_events_has_evidence
        CHECK (payload IS NOT NULL OR raw_payload IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS raw_ping_events_kafka_uk
    ON bronze.raw_ping_events (_kafka_partition, _kafka_offset);

CREATE TABLE IF NOT EXISTS bronze.raw_cdc_entities (
{_KAFKA_COLUMNS},
    source_table text  GENERATED ALWAYS AS (payload #>> '{{source,table}}') STORED,
    op           text  GENERATED ALWAYS AS (payload ->> 'op') STORED,
    "before"     jsonb GENERATED ALWAYS AS (payload -> 'before') STORED,
    "after"      jsonb GENERATED ALWAYS AS (payload -> 'after') STORED,
    source_ts_ms text  GENERATED ALWAYS AS (payload #>> '{{source,ts_ms}}') STORED,
    CONSTRAINT raw_cdc_entities_has_evidence
        CHECK (payload IS NOT NULL OR raw_payload IS NOT NULL)
);

-- Four topics land here, so the topic is part of the key. Small table: ~2,000 changes over
-- 30 days at target scale (docs/source-system-reference.md, entity churn).
CREATE UNIQUE INDEX IF NOT EXISTS raw_cdc_entities_kafka_uk
    ON bronze.raw_cdc_entities (_topic, _kafka_partition, _kafka_offset);

CREATE TABLE IF NOT EXISTS bronze.raw_job_events (
{_KAFKA_COLUMNS},
    op          text GENERATED ALWAYS AS (payload ->> 'op') STORED,
    job_id      text GENERATED ALWAYS AS (payload #>> '{{after,job_id}}') STORED,
    from_status text GENERATED ALWAYS AS (payload #>> '{{after,from_status}}') STORED,
    to_status   text GENERATED ALWAYS AS (payload #>> '{{after,to_status}}') STORED,
    CONSTRAINT raw_job_events_has_evidence
        CHECK (payload IS NOT NULL OR raw_payload IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS raw_job_events_kafka_uk
    ON bronze.raw_job_events (_kafka_partition, _kafka_offset);

-- --------------------------------------------------------------------------------------
-- The batch poller's output. Deliberately a different shape.
-- --------------------------------------------------------------------------------------
--
-- Note what this table CANNOT have: an op code and a before-image. A poller reads current
-- state, so it cannot know whether a row is new or changed, cannot see what it used to say,
-- and cannot observe a row that no longer exists. Forcing this into a Debezium-shaped envelope
-- would fabricate those fields. The missing columns are the finding.
CREATE TABLE IF NOT EXISTS bronze.poll_rows (
    poll_row_id     bigserial PRIMARY KEY,
    source_table    text        NOT NULL,
    row_image       jsonb       NOT NULL,
    -- The watermark value that caused this row to be selected. Keeping it makes a leaked
    -- watermark diagnosable after the fact rather than merely suspected.
    watermark_value timestamptz NOT NULL,
    _polled_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS poll_rows_source_table_idx ON bronze.poll_rows (source_table);

CREATE TABLE IF NOT EXISTS bronze.poll_watermarks (
    source_table     text        PRIMARY KEY,
    -- pings has no updated_at (docker/oltp/init.sql:214-224), so it watermarks on server_ts.
    -- Recording which column was used stops a later reader assuming they are the same thing.
    watermark_column text        NOT NULL,
    watermark_value  timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL DEFAULT now()
);
"""


def apply(conn: Connection) -> None:
    """Create anything missing. Safe to call on every startup, and it is."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        cur.execute(DDL)
    conn.commit()


def main() -> int:
    """`python -m fleet_telemetry.load.schema` -- also what CI runs before dbt build."""
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    target = config.warehouse()
    print(f"applying bronze schema to {target.safe_dsn()}")
    with connect(target.dsn()) as conn:
        apply(conn)
    print(f"bronze tables present: {', '.join(BRONZE_TABLES)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the tests**

Run: `docker compose -f docker/docker-compose.yml up -d warehouse` then `pytest tests/test_bronze_load.py -v -m integration`
Expected: 5 passed.

- [ ] **Step 5: Confirm the non-integration suite is untouched**

Run: `pytest -m "not integration"`
Expected: PASS, and `tests/test_bronze_load.py` collected but deselected.

- [ ] **Step 6: Lint and commit**

```bash
ruff check . && ruff format --check .
git checkout -b phase-2/batch-poller
git add src/fleet_telemetry/load/schema.py tests/test_bronze_load.py
git commit -m "feat(bronze): own the bronze schema in Python, with non-casting generated columns"
```

---

## Task 2: Watermark arithmetic, hermetic

**Files:**
- Create: `src/fleet_telemetry/ingest/poller.py`
- Test: `tests/test_poller.py`

**Interfaces:**
- Consumes: `fleet_telemetry.load.schema.apply`
- Produces: `poller.POLLED_TABLES: dict[str, str]` (table → watermark column), `poller.next_watermark(rows, column, current) -> datetime`

- [ ] **Step 1: Write the failing test**

`tests/test_poller.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_poller.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'fleet_telemetry.ingest.poller'`

- [ ] **Step 3: Write the minimal implementation**

`src/fleet_telemetry/ingest/poller.py` (header and pure functions only; the loop arrives in Task 3):

```python
"""The naive batch poller: `SELECT ... WHERE updated_at > watermark`, on a loop.

This is phase 2 step 1 and it is meant to be replaced. It works, and building it first is what
makes the CDC configuration in step 2 mean anything -- every key in that connector answers a
failure reproduced here (design spec section 13, lines 370-376).

Three failures to find by experiment, not by reading:

  1. It never sees a DELETE. A deleted row has no updated_at to exceed the watermark, and no
     query over current state can return a row that is not there.
  2. It misses a row changed twice between two polls. It reads state, not changes, so two
     commits collapse into one observation.
  3. It competes with the application for the OLTP database, on the same connection budget.

WATERMARK COLUMNS DIFFER PER TABLE, AND THAT IS NOT A DETAIL.
`pings` has no updated_at (docker/oltp/init.sql:214-224) -- it is append-only, so there is
nothing to update -- and watermarks on `server_ts` instead. The mutable tables carry updated_at
maintained by a database TRIGGER rather than by application code, deliberately: if the app
forgot to set it on one path the poller would silently skip those rows, which is a data-loss
bug that looks like nothing (docker/oltp/init.sql:28-34).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

# Table -> the column a watermark can be built from. depots is included even though it changes
# almost never: "changes rarely" and "never changes" differ, and only one of them is safe to
# leave out.
POLLED_TABLES: dict[str, str] = {
    "depots": "updated_at",
    "drivers": "updated_at",
    "vehicles": "updated_at",
    "jobs": "updated_at",
    "job_events": "created_at",
    "pings": "server_ts",
}


def next_watermark(
    rows: Sequence[dict[str, Any]], column: str, current: datetime
) -> datetime:
    """The highest value seen, never lower than where we already were.

    Two guards, both earned:

    An empty result returns `current` rather than `now()`. Advancing on an empty poll is the
    classic watermark leak -- a transaction that committed with an earlier timestamp but became
    visible afterwards is then permanently behind the mark and never read.

    The result is clamped at `current` because the clock can go backwards. The profiler measured
    the container stepping back roughly 2.7 seconds every 30 under WSL2
    (docs/source-system-reference.md, section 8). With a strict `>` comparison a retreating
    watermark would skip every row committed in between.
    """
    highest = current
    for row in rows:
        value = row[column]
        if value is not None and value > highest:
            highest = value
    return highest
```

- [ ] **Step 4: Run the tests**

Run: `pytest tests/test_poller.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
ruff check . && ruff format --check .
git add src/fleet_telemetry/ingest/poller.py tests/test_poller.py
git commit -m "feat(poller): watermark arithmetic, with the leak and the retreating clock guarded"
```

---

## Task 3: The poller loop

**Files:**
- Modify: `src/fleet_telemetry/ingest/poller.py`
- Test: `tests/test_poller.py`

**Interfaces:**
- Produces: `poller.poll_once(oltp_conn, wh_conn) -> dict[str, int]` (table → rows landed), `poller.run(interval, cycles) -> None`, `poller.main() -> int`

- [ ] **Step 1: Write the failing integration test**

Append to `tests/test_poller.py`:

```python
import pytest

from fleet_telemetry import config
from fleet_telemetry.load import schema


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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_poller.py -v -m integration`
Expected: FAIL — `AttributeError: module 'fleet_telemetry.ingest.poller' has no attribute 'poll_once'`

- [ ] **Step 3: Implement the loop**

Append to `src/fleet_telemetry/ingest/poller.py`:

```python
import argparse
import json
import time
from datetime import UTC, datetime

from psycopg import Connection, connect
from psycopg.rows import dict_row

from fleet_telemetry import config
from fleet_telemetry.load import schema

# Far enough back to select everything on the first run. Not datetime.min, because Postgres
# accepts it but the timestamptz range checks make the intent unreadable.
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Rows per statement. Small enough that a poll does not hold a long-running snapshot open on
# the OLTP -- which is failure 3, and the reason this whole approach loses.
BATCH = 1000


def _watermarks(wh_conn: Connection) -> dict[str, datetime]:
    """Where each table's poll last reached. Missing means "never polled"."""
    with wh_conn.cursor() as cur:
        cur.execute("select source_table, watermark_value from bronze.poll_watermarks")
        stored = dict(cur.fetchall())
    return {table: stored.get(table, EPOCH) for table in POLLED_TABLES}


def poll_once(oltp_conn: Connection, wh_conn: Connection) -> dict[str, int]:
    """One pass over every polled table. Returns rows landed per table.

    Strictly `>` rather than `>=`, so a row is not re-read on every poll forever. The cost is
    that two rows sharing a timestamp to the microsecond, straddling a poll boundary, lose the
    second one -- a real hazard the CDC path does not have, and worth stating rather than
    hiding behind `>=` and a deduplication step.
    """
    schema.apply(wh_conn)
    marks = _watermarks(wh_conn)
    landed: dict[str, int] = {}

    for table, column in POLLED_TABLES.items():
        mark = marks[table]
        with oltp_conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"select * from {table} where {column} > %s order by {column} limit %s",
                (mark, BATCH),
            )
            rows = cur.fetchall()
        # The read is finished; release the OLTP snapshot before touching the warehouse.
        oltp_conn.rollback()

        if rows:
            with wh_conn.cursor() as cur:
                cur.executemany(
                    "insert into bronze.poll_rows "
                    "(source_table, row_image, watermark_value) values (%s, %s, %s)",
                    [(table, json.dumps(row, default=str), row[column]) for row in rows],
                )
        advanced = next_watermark(rows, column, mark)
        with wh_conn.cursor() as cur:
            cur.execute(
                "insert into bronze.poll_watermarks "
                "(source_table, watermark_column, watermark_value) values (%s, %s, %s) "
                "on conflict (source_table) do update "
                "set watermark_value = excluded.watermark_value, updated_at = now()",
                (table, column, advanced),
            )
        wh_conn.commit()
        landed[table] = len(rows)

    return landed


def run(interval: float, cycles: int | None) -> None:
    """Poll forever, or a fixed number of times. `cycles=None` means forever."""
    with connect(config.oltp().dsn()) as oltp_conn, connect(config.warehouse().dsn()) as wh_conn:
        done = 0
        while cycles is None or done < cycles:
            landed = poll_once(oltp_conn, wh_conn)
            total = sum(landed.values())
            detail = ", ".join(f"{t}={n}" for t, n in landed.items() if n)
            print(f"poll {done + 1}: {total} rows" + (f" ({detail})" if detail else ""))
            done += 1
            if cycles is None or done < cycles:
                time.sleep(interval)


def main() -> int:
    parser = argparse.ArgumentParser(description="The naive batch poller. Phase 2, step 1.")
    parser.add_argument("--interval", type=float, default=10.0, help="seconds between polls")
    parser.add_argument("--cycles", type=int, default=None, help="stop after N polls")
    args = parser.parse_args()
    run(args.interval, args.cycles)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the tests**

Run: `docker compose -f docker/docker-compose.yml up -d oltp warehouse` then `pytest tests/test_poller.py -v`
Expected: 6 passed.

- [ ] **Step 5: Run it for real**

Run: `python -m fleet_telemetry.ingest.poller --cycles 2 --interval 3`
Expected: first poll lands the seeded rows and whatever pings exist; second lands 0.

- [ ] **Step 6: Commit**

```bash
ruff check . && ruff format --check .
git add src/fleet_telemetry/ingest/poller.py tests/test_poller.py
git commit -m "feat(poller): poll loop landing current-state row images in bronze.poll_rows"
```

---

## Task 4: Reproduce the poller's three failures

**Files:**
- Test: `tests/test_poller.py`
- Create: `src/fleet_telemetry/ingest/compare.py` (the delete/double-change measurements; the CDC side is added in Task 12)

**Interfaces:**
- Produces: `compare.poller_blind_spots(oltp_conn, wh_conn) -> dict[str, Any]`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_poller.py`:

```python
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
        cur.execute(
            "update drivers set home_depot_id = 5 where driver_id = %s", (TEST_DRIVER_ID,)
        )
        oltp.commit()
        cur.execute(
            "update drivers set home_depot_id = 1 where driver_id = %s", (TEST_DRIVER_ID,)
        )
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_poller.py -v -m integration -k "delete or collapse"`
Expected: FAIL — the vehicle insert violates nothing, but `bronze.poll_rows` is empty because the fixture truncates it and `poll_once` has not yet been run with these ids. Confirm the failure is an assertion, not an error, before proceeding.

- [ ] **Step 3: Write the measurement module**

`src/fleet_telemetry/ingest/compare.py`:

```python
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

        cur.execute(
            "select source_table, count(*) as rows, count(distinct row_image::text) as distinct_images "
            "from bronze.poll_rows group by source_table order by source_table"
        )
        out["poll_rows_by_table"] = cur.fetchall()

    out["vehicles_live"] = len(live_vehicles)
    out["vehicles_in_poll_rows"] = len(polled_vehicles)
    # Vehicles bronze believes exist, that the OLTP has deleted. The poller cannot ever
    # shrink this set.
    out["deleted_but_still_present_in_bronze"] = sorted(polled_vehicles - live_vehicles)
    return out
```

- [ ] **Step 4: Run the tests**

Run: `pytest tests/test_poller.py -v`
Expected: 8 passed.

- [ ] **Step 5: Measure failure 3 — the poller competes with the application**

Run, with the simulator running (`docker compose -f docker/docker-compose.yml up -d`):

```bash
psql "postgresql://fleet:fleet@127.0.0.1:55433/fleet" -c "select tup_returned, tup_fetched, blks_read from pg_stat_database where datname = 'fleet'"
python -m fleet_telemetry.ingest.poller --cycles 30 --interval 1
psql "postgresql://fleet:fleet@127.0.0.1:55433/fleet" -c "select tup_returned, tup_fetched, blks_read from pg_stat_database where datname = 'fleet'"
```

Record both readings and the delta. Expected: `tup_returned` grows by far more than the number of rows the poller actually landed, because every poll scans `pings` by index and the table is ~172,800 rows at the phase 1 baseline. Write the two numbers down — Task 12 uses them.

- [ ] **Step 6: Commit**

```bash
ruff check . && ruff format --check .
git add src/fleet_telemetry/ingest/compare.py tests/test_poller.py
git commit -m "test(poller): reproduce the delete blind spot and the collapsed double change"
```

---

## Task 5: Redpanda and Kafka Connect in compose

**Files:**
- Modify: `docker/docker-compose.yml`

**Interfaces:**
- Produces: broker reachable at `127.0.0.1:19092` from the host and `redpanda:9092` inside the network; Connect REST API at `127.0.0.1:8083`.

- [ ] **Step 1: Add the services**

Insert after the `warehouse` service block in `docker/docker-compose.yml`, and update the header comment at line 13 (`Redpanda and Debezium arrive in phase 2`) to say they have arrived:

```yaml
  # ------------------------------------------------------------------------------------
  # Redpanda -- the broker. Kafka wire protocol, one container, no ZooKeeper or KRaft.
  # ------------------------------------------------------------------------------------
  #
  # WHY A BROKER AT ALL, when both databases are on this machine and there is one consumer.
  #
  # Not throughput: at the compose defaults this carries about 2 events a second. It is there
  # to decouple failure domains. Debezium's replication slot must be drained continuously or
  # Postgres retains WAL forever and fills the disk (see the oltp service, above). With the
  # broker in between, draining is Connect's only job, and the Bronze loader can be down for an
  # hour without that becoming an OLTP incident. It also makes replay possible: a consumer group
  # can be rewound and bronze rebuilt, which reading the WAL directly cannot do because the WAL
  # is gone the moment it is consumed.
  #
  # It is NOT durable storage. A topic has a retention window, not a memory -- which is why
  # bronze.* exists downstream of it.
  redpanda:
    image: redpandadata/redpanda:v24.2.7
    container_name: fleet-redpanda
    restart: unless-stopped
    command:
      - redpanda
      - start
      # dev-container relaxes the production checks (fsync, memory locking) that make a
      # single-node broker on a laptop refuse to start.
      - --mode=dev-container
      - --smp=1
      - --memory=1G
      # TWO LISTENERS, AND THIS IS THE PART THAT COSTS AN AFTERNOON IF IT IS WRONG.
      #
      # A Kafka client connects to a bootstrap address, then reconnects to whatever address the
      # broker ADVERTISES. One listener cannot serve both sides: Connect resolves `redpanda`,
      # the host does not; the host reaches 127.0.0.1:19092, the container cannot. Advertise one
      # address and the other side connects, gets told to go somewhere unreachable, and fails
      # with a timeout that looks like the broker is down.
      #
      # 19092 externally, matching KAFKA_DEFAULT_BOOTSTRAP in src/fleet_telemetry/config.py.
      - --kafka-addr=internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr=internal://redpanda:9092,external://127.0.0.1:19092
      # Connect creates its own three internal topics, and Debezium creates one per captured
      # table. Without this they must all be created by hand before anything works.
      - --set=redpanda.auto_create_topics_enabled=true
    ports:
      - "${KAFKA_PORT:-19092}:19092"
      # Admin API, for `rpk` from the host.
      - "19644:9644"
    volumes:
      - redpanda_data:/var/lib/redpanda/data
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -q 'Healthy:.*true'"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 20s

  # ------------------------------------------------------------------------------------
  # Kafka Connect running Debezium -- reads the OLTP write-ahead log
  # ------------------------------------------------------------------------------------
  #
  # The connector itself is NOT defined here. It is registered over the REST API by
  # `python -m fleet_telemetry.ingest.connector --register`, because its config contains a
  # password and because registering it is an act you should be able to repeat, inspect and
  # reverse without recreating a container.
  connect:
    image: quay.io/debezium/connect:3.0
    container_name: fleet-connect
    restart: unless-stopped
    depends_on:
      redpanda:
        condition: service_healthy
      oltp:
        condition: service_healthy
    environment:
      # Service name and CONTAINER port -- the 19092 host mapping does not exist in here.
      BOOTSTRAP_SERVERS: redpanda:9092
      GROUP_ID: fleet-connect
      CONFIG_STORAGE_TOPIC: _connect_configs
      OFFSET_STORAGE_TOPIC: _connect_offsets
      STATUS_STORAGE_TOPIC: _connect_statuses
      # Single-node broker: the default of 3 makes topic creation fail outright, with an error
      # about insufficient replicas that reads as a cluster problem rather than a config one.
      CONFIG_STORAGE_REPLICATION_FACTOR: 1
      OFFSET_STORAGE_REPLICATION_FACTOR: 1
      STATUS_STORAGE_REPLICATION_FACTOR: 1
      KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      # Schemas OFF. With them on, every single message carries a full Avro-style schema
      # describing itself -- several kilobytes of identical preamble per ping, and the actual
      # change buried under a `payload` key. Off, the message IS the Debezium envelope:
      # before, after, source, op, ts_ms at the top level. bronze's generated columns assume
      # this; turning schemas on silently makes every one of them null.
      CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE: "false"
      CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
    ports:
      - "${CONNECT_PORT:-8083}:8083"
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8083/connectors || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 40s
```

And add to the `volumes:` block at the bottom:

```yaml
  redpanda_data:
    name: fleet-redpanda-data
```

- [ ] **Step 2: Bring the stack up**

Run: `docker compose -f docker/docker-compose.yml up -d redpanda connect`

- [ ] **Step 3: Verify both sides of the listener split**

```bash
docker exec fleet-redpanda rpk cluster health
docker exec fleet-connect curl -s http://localhost:8083/connector-plugins | head -c 400
```
Expected: cluster reports `Healthy: true`; the plugin list contains `io.debezium.connector.postgresql.PostgresConnector`.

- [ ] **Step 4: Verify the host-side listener specifically**

Run: `python -c "from confluent_kafka.admin import AdminClient; print(AdminClient({'bootstrap.servers':'127.0.0.1:19092'}).list_topics(timeout=10).topics.keys())"`
Expected: the three `_connect_*` topics. If this times out while `rpk cluster health` passes, the advertised external address is wrong — that is the listener split described in the comment.

- [ ] **Step 5: Commit**

```bash
git checkout -b phase-2/cdc
git add docker/docker-compose.yml
git commit -m "feat(cdc): add redpanda and kafka connect, with the two-listener split explained"
```

---

## Task 6: The Debezium connector

**Files:**
- Create: `docker/debezium/fleet-connector.json`, `src/fleet_telemetry/ingest/connector.py`
- Modify: `src/fleet_telemetry/config.py`, `.env.example`
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: `config.kafka()`
- Produces: `config.KafkaConfig(bootstrap_servers, consumer_group, connect_url, defaulted: frozenset[str])`; `connector.load_config(env=None) -> dict`, `connector.register() -> dict`, `connector.status() -> dict`, `connector.main() -> int`

- [ ] **Step 1: Write the failing config test**

Append to `tests/test_config.py`:

```python
def test_kafka_settings_default_and_override():
    """The consumer group is a real configured object, not a literal buried in the consumer.

    Rewinding a group to replay bronze means naming it, so it belongs with everything else that
    knows where settings come from.
    """
    default = config.kafka({})
    assert default.consumer_group == config.KAFKA_DEFAULT_GROUP
    assert default.connect_url == config.CONNECT_DEFAULT_URL
    assert default.defaulted == frozenset({"bootstrap_servers", "consumer_group", "connect_url"})

    explicit = config.kafka(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
            "KAFKA_CONSUMER_GROUP": "replay-2026-08-11",
            "DEBEZIUM_CONNECT_URL": "http://connect:8083",
        }
    )
    assert explicit.consumer_group == "replay-2026-08-11"
    assert explicit.defaulted == frozenset()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_config.py -v -k kafka`
Expected: FAIL — `AttributeError: module 'fleet_telemetry.config' has no attribute 'KAFKA_DEFAULT_GROUP'`

- [ ] **Step 3: Extend the config module**

In `src/fleet_telemetry/config.py`, beside `KAFKA_DEFAULT_BOOTSTRAP` (line 86):

```python
# The consumer group. Named here rather than hardcoded in the consumer because rewinding a
# group -- `rpk group seek bronze-loader --to start` -- is a deliberate operation, and an
# operation you can perform needs a name you can find.
KAFKA_DEFAULT_GROUP = "bronze-loader"

# Kafka Connect's REST API. Where the Debezium connector is registered and inspected.
CONNECT_DEFAULT_URL = "http://127.0.0.1:8083"
```

Replace the `KafkaConfig` dataclass (lines 134-139) and the `kafka()` loader (lines 219-222):

```python
@dataclass(frozen=True)
class KafkaConfig:
    """Broker endpoint, consumer group, and the Connect REST API.

    `defaulted` is a frozenset rather than a bool, matching DatabaseConfig: with three fields,
    "something fell back" is no longer a useful answer -- a defaulted broker address locally is
    expected, and a defaulted one in a deployed environment is a finding.
    """

    bootstrap_servers: str
    consumer_group: str
    connect_url: str
    defaulted: frozenset[str] = frozenset()


def kafka(env: Mapping[str, str] | None = None) -> KafkaConfig:
    """Broker carrying Debezium change events out of the OLTP database."""
    fields = {
        "bootstrap_servers": ("KAFKA_BOOTSTRAP_SERVERS", KAFKA_DEFAULT_BOOTSTRAP),
        "consumer_group": ("KAFKA_CONSUMER_GROUP", KAFKA_DEFAULT_GROUP),
        "connect_url": ("DEBEZIUM_CONNECT_URL", CONNECT_DEFAULT_URL),
    }
    resolved: dict[str, str] = {}
    defaulted: set[str] = set()
    for field, (var, fallback) in fields.items():
        value = _get(env, var)
        if value is None:
            value = fallback
            defaulted.add(field)
        resolved[field] = value
    return KafkaConfig(defaulted=frozenset(defaulted), **resolved)
```

In `describe()` (lines 273-277), replace the Kafka row so the detail names what fell back, matching the database rows:

```python
        (
            "Kafka",
            not broker.defaulted,
            f"{broker.bootstrap_servers} group={broker.consumer_group} "
            f"connect={broker.connect_url}"
            + (f" [defaults: {', '.join(sorted(broker.defaulted))}]" if broker.defaulted else ""),
        ),
```

- [ ] **Step 4: Run the config tests**

Run: `pytest tests/test_config.py -v`
Expected: all pass, including the pre-existing `test_describe_reports_every_component` which asserts `status["Kafka"] is False` on an empty environment.

- [ ] **Step 5: Write the connector config**

`docker/debezium/fleet-connector.json` — one comment per key is impossible in JSON, so the comments live in the module that loads it (Step 6). Keys chosen deliberately:

```json
{
  "name": "fleet-oltp",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "oltp",
    "database.port": "5432",
    "database.dbname": "fleet",
    "database.user": "${OLTP_USER}",
    "database.password": "${OLTP_PASSWORD}",
    "topic.prefix": "fleet",
    "slot.name": "fleet_debezium",
    "publication.name": "fleet_publication",
    "publication.autocreate.mode": "filtered",
    "table.include.list": "public.pings,public.job_events,public.vehicles,public.drivers,public.depots,public.jobs",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "false",
    "heartbeat.interval.ms": "10000",
    "topic.creation.enable": "false"
  }
}
```

- [ ] **Step 6: Write the registrar**

`src/fleet_telemetry/ingest/connector.py`:

```python
"""Register and inspect the Debezium connector over the Kafka Connect REST API.

Not a shell script, for two reasons: this repository is Windows-first, and the connector config
contains a password that must come from the one place that knows where settings come from
(src/fleet_telemetry/config.py) rather than being committed.

stdlib urllib rather than httpx, because httpx lives in the [app] extra and this is [warehouse]
work -- an import that happens to resolve on a developer's machine and not in a container is
exactly the mistake pyproject.toml:24-29 already records.

    python -m fleet_telemetry.ingest.connector --register
    python -m fleet_telemetry.ingest.connector --status
    python -m fleet_telemetry.ingest.connector --delete

WHY EACH KEY IN docker/debezium/fleet-connector.json IS WHAT IT IS.

  plugin.name = pgoutput
      Postgres 16's built-in logical decoding output plugin. decoderbufs and wal2json need a
      shared library installed into the database image; pgoutput needs nothing, which is why
      docker/docker-compose.yml can use postgres:16-alpine unmodified.

  database.hostname = oltp, database.port = 5432
      NOT config.oltp().host. This config is evaluated by Connect, inside the compose network,
      where services address each other by service name on the container port. The 127.0.0.1
      and 55433 a developer uses do not exist in there. Same trap as the api service's
      environment block in docker-compose.yml.

  slot.name = fleet_debezium
      One named slot, within the max_replication_slots=4 the oltp service sets. Naming it means
      `select * from pg_replication_slots` is readable, and a stale slot can be dropped
      deliberately rather than found by accident after the disk fills.

  publication.autocreate.mode = filtered
      Debezium creates a publication covering only the tables in table.include.list. The
      default, `all_tables`, publishes every table in the database including ones added later.

  snapshot.mode = initial
      On first start, read every existing row and emit it as op='r'. That is ~172,800 ping rows
      at the phase 1 baseline and it takes a minute. Worth it: without the snapshot, bronze
      begins mid-history and nothing downstream can be rebuilt from it.

  tombstones.on.delete = false
      Debezium's default follows a delete with a null-value message so log-compacted topics can
      drop the key. These topics are not compacted, and the tombstone carries nothing the
      preceding op='d' event lacks. The consumer still handles one if it appears -- recorded,
      never dropped -- because "should not happen" is not a guarantee.

  heartbeat.interval.ms = 10000
      The one nobody expects. Debezium only advances the slot's confirmed LSN when it emits
      something. With a captured table that goes quiet, the slot stops advancing while the WAL
      keeps growing, and the symptom is a disk filling with a healthy-looking connector. A
      heartbeat forces the advance.

  NO ExtractNewRecordState SMT
      The popular `unwrap` transform flattens the envelope to just the after-image and turns
      deletes into tombstones. That discards the before-image and the op code -- the two fields
      Type 2 dimensions are built from (dbt/models/staging/_sources.yml:53-65). Bronze keeps
      the envelope; Silver unwraps it.
"""

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from pathlib import Path
from string import Template
from typing import Any

from fleet_telemetry import config

CONFIG_PATH = config.PROJECT_ROOT / "docker" / "debezium" / "fleet-connector.json"


def load_config(path: Path | None = None) -> dict[str, Any]:
    """Read the connector definition and substitute the credential placeholders.

    Template.substitute rather than an f-string: the file is valid JSON on disk, so it can be
    linted and diffed, and a missing variable raises rather than silently producing the literal
    string "${OLTP_PASSWORD}" as a password -- which would present as an authentication failure
    against a database that is demonstrably up.
    """
    oltp = config.oltp()
    raw = (path or CONFIG_PATH).read_text(encoding="utf-8")
    filled = Template(raw).substitute(OLTP_USER=oltp.user, OLTP_PASSWORD=oltp.password)
    return json.loads(filled)


def _request(method: str, path: str, body: dict | None = None) -> Any:
    url = f"{config.kafka().connect_url.rstrip('/')}{path}"
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(  # noqa: S310 - fixed scheme, local Connect API
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            payload = response.read().decode()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{method} {url} -> {exc.code}: {exc.read().decode()}") from exc
    return json.loads(payload) if payload else {}


def register() -> dict:
    """Create or update the connector. Idempotent -- PUT of the config, not POST of the whole."""
    definition = load_config()
    return _request("PUT", f"/connectors/{definition['name']}/config", definition["config"])


def status() -> dict:
    return _request("GET", f"/connectors/{load_config()['name']}/status")


def delete() -> None:
    """Removes the connector but NOT the replication slot.

    Debezium leaves the slot behind on purpose, so a connector can be recreated and resume. The
    consequence is that deleting a connector you do not intend to recreate leaves Postgres
    retaining WAL forever. Drop it deliberately:

        select pg_drop_replication_slot('fleet_debezium');
    """
    _request("DELETE", f"/connectors/{load_config()['name']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage the Debezium connector.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--register", action="store_true")
    group.add_argument("--status", action="store_true")
    group.add_argument("--delete", action="store_true")
    args = parser.parse_args()

    if args.register:
        register()
        print(json.dumps(status(), indent=2))
    elif args.status:
        print(json.dumps(status(), indent=2))
    else:
        delete()
        print("connector deleted; the replication slot is still there -- see the docstring")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 7: Document the new settings**

Append to `.env.example`, after the existing broker block (line 57):

```bash
# The consumer group the Bronze loader joins. Named rather than hardcoded because
# rewinding it is a deliberate operation:
#   docker exec fleet-redpanda rpk group seek bronze-loader --to start
KAFKA_CONSUMER_GROUP=bronze-loader

# Kafka Connect's REST API, where the Debezium connector is registered.
DEBEZIUM_CONNECT_URL=http://127.0.0.1:8083
```

- [ ] **Step 8: Register it and verify against the database**

```bash
python -m fleet_telemetry.ingest.connector --register
psql "postgresql://fleet:fleet@127.0.0.1:55433/fleet" -c "select slot_name, plugin, active, wal_status from pg_replication_slots"
docker exec fleet-redpanda rpk topic list
```
Expected: status shows `RUNNING`; one slot named `fleet_debezium`, `active = t`, `wal_status = reserved`; topics `fleet.public.pings`, `fleet.public.vehicles`, `fleet.public.drivers`, `fleet.public.depots`, `fleet.public.jobs`, `fleet.public.job_events`.

- [ ] **Step 9: Look at one message before writing any code that parses them**

Run: `docker exec fleet-redpanda rpk topic consume fleet.public.vehicles --num 1`
Expected: the value is a bare envelope with top-level `before`, `after`, `source`, `op`, `ts_ms`. If instead it has a `schema` and a `payload` key, `CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE` did not take — fix that before Task 8, because every generated column depends on it.

- [ ] **Step 10: Commit**

```bash
ruff check . && ruff format --check .
git add docker/debezium/ src/fleet_telemetry/ingest/connector.py src/fleet_telemetry/config.py .env.example tests/test_config.py
git commit -m "feat(cdc): debezium connector, registered from python, with every key justified"
```

---

## Task 7: Envelope routing and decoding, hermetic

**Files:**
- Create: `src/fleet_telemetry/ingest/envelope.py`, `tests/test_envelope.py`

**Interfaces:**
- Produces: `envelope.TOPIC_TABLES: dict[str, str]`, `envelope.table_for_topic(topic) -> str | None`, `envelope.decode(value: bytes | None) -> DecodedValue` where `DecodedValue = tuple[str | None, str | None, str | None]` = `(payload_json, raw_payload, parse_error)`

- [ ] **Step 1: Write the failing test**

`tests/test_envelope.py`:

```python
"""Routing and decoding. Hermetic on purpose.

This module must not import confluent_kafka, and this test proves it: CI installs [dev] only
(.github/workflows/ci.yml:37), so anything importing the Kafka client at module scope would
turn a logic test into a collection error.
"""

from __future__ import annotations

import json

from fleet_telemetry.ingest import envelope


def test_the_three_streams_route_to_three_tables():
    assert envelope.table_for_topic("fleet.public.pings") == "raw_ping_events"
    assert envelope.table_for_topic("fleet.public.job_events") == "raw_job_events"
    for entity in ("vehicles", "drivers", "depots", "jobs"):
        assert envelope.table_for_topic(f"fleet.public.{entity}") == "raw_cdc_entities"


def test_an_unknown_topic_routes_nowhere_rather_than_guessing():
    """A topic appearing that nothing expects is a finding. Guessing a destination would bury
    it in a table where nobody would look for it."""
    assert envelope.table_for_topic("fleet.public.audit_log") is None
    assert envelope.table_for_topic("_connect_offsets") is None


def test_valid_json_decodes_with_no_error_and_no_raw_copy():
    """The raw bytes are kept only when they could not be parsed. Keeping both for every row
    would roughly double a table already headed for ~22M rows."""
    value = json.dumps({"op": "c", "after": {"ping_id": "abc"}}).encode()
    payload, raw, error = envelope.decode(value)
    assert json.loads(payload)["op"] == "c"
    assert raw is None
    assert error is None


def test_malformed_bytes_are_kept_verbatim_with_the_error():
    payload, raw, error = envelope.decode(b'{"op": "c", ')
    assert payload is None
    assert raw == '{"op": "c", '
    assert "Expecting" in error


def test_undecodable_bytes_are_still_kept():
    """A payload that is not even valid UTF-8. Bronze has no way to store it as text without
    loss, so it stores the repr and says so -- which is still more than dropping it."""
    payload, raw, error = envelope.decode(b"\xff\xfe\x00")
    assert payload is None
    assert raw is not None
    assert "utf-8" in error.lower()


def test_a_tombstone_is_recorded_rather_than_dropped():
    """tombstones.on.delete is false, so one should never arrive. 'Should never' is not a
    guarantee, and a silently dropped message is an offset gap nobody can explain later."""
    payload, raw, error = envelope.decode(None)
    assert payload is None
    assert raw == ""
    assert "tombstone" in error


def test_valid_json_that_is_not_an_object_is_kept_and_flagged():
    """jsonb accepts a bare array or number quite happily, and then every generated column is
    null with no explanation. Flagging it here makes the cause findable."""
    payload, raw, error = envelope.decode(b"[1, 2, 3]")
    assert payload is None
    assert raw == "[1, 2, 3]"
    assert "object" in error
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_envelope.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'fleet_telemetry.ingest.envelope'`

- [ ] **Step 3: Write the module**

`src/fleet_telemetry/ingest/envelope.py`:

```python
"""Which bronze table a topic belongs to, and how to decode a message without ever losing one.

Deliberately free of any Kafka import. Two reasons, and the second is the real one:

  * CI installs [dev] only, so a module-scope `import confluent_kafka` would turn every test in
    this file into a collection error;
  * the decisions here -- what counts as malformed, what happens to a message nothing expects --
    are the ones worth testing, and they should be testable without a broker.

THE ONE RULE: this function never raises and never returns nothing. Every input produces a row.
Bronze exists to hold the evidence, and the evidence you most want is the message that broke
something (src/fleet_telemetry/load/__init__.py:6-8).
"""

from __future__ import annotations

import json

# Topic -> bronze table. Topics are `{topic.prefix}.{schema}.{table}`, and the prefix is
# `fleet`, set in docker/debezium/fleet-connector.json.
#
# Three tables rather than six, because the streams have genuinely different shapes: pings are
# append-only and enormous, job_events are append-only and arrive out of order, and the four
# mutable entities carry before/after images that Type 2 dimensions are built from
# (src/fleet_telemetry/ingest/__init__.py:10-14).
TOPIC_TABLES: dict[str, str] = {
    "fleet.public.pings": "raw_ping_events",
    "fleet.public.job_events": "raw_job_events",
    "fleet.public.vehicles": "raw_cdc_entities",
    "fleet.public.drivers": "raw_cdc_entities",
    "fleet.public.depots": "raw_cdc_entities",
    "fleet.public.jobs": "raw_cdc_entities",
}

# (payload as JSON text, raw bytes as text, parse error). Exactly one of the first two is
# non-null; the third is non-null whenever the first is null.
DecodedValue = tuple[str | None, str | None, str | None]


def table_for_topic(topic: str) -> str | None:
    """None for anything unrecognised -- including Connect's own internal topics.

    Returning None rather than a default table is the point. A topic nobody planned for is a
    finding: someone added a table to table.include.list, or the prefix changed. Routing it to
    a plausible-looking destination would hide that in a table where nobody would look.
    """
    return TOPIC_TABLES.get(topic)


def decode(value: bytes | None) -> DecodedValue:
    """Turn message bytes into something bronze can store. Never raises.

    A null value is a tombstone. tombstones.on.delete is false in the connector config so one
    should not arrive, but dropping the message instead of recording it would leave a gap in
    the offset sequence with nothing to explain it -- and offset gaps are how you conclude the
    loader lost data when it did not.
    """
    if value is None:
        return None, "", "tombstone: null message value"

    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError as exc:
        # No lossless text representation exists, so keep the repr rather than nothing. The
        # bytes are still in the topic until retention expires; the repr is what survives.
        return None, repr(value), f"utf-8 decode failed: {exc}"

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        return None, text, str(exc)

    if not isinstance(parsed, dict):
        # jsonb would accept `[1,2,3]` without complaint, and then every generated column is
        # null with no visible cause. Better to record why.
        return None, text, f"payload is a {type(parsed).__name__}, not a JSON object"

    return text, None, None
```

- [ ] **Step 4: Run the tests**

Run: `pytest tests/test_envelope.py -v`
Expected: 7 passed.

- [ ] **Step 5: Prove the hermetic claim**

Run: `python -c "import sys; import fleet_telemetry.ingest.envelope; assert 'confluent_kafka' not in sys.modules; print('clean')"`
Expected: `clean`

- [ ] **Step 6: Commit**

```bash
ruff check . && ruff format --check .
git add src/fleet_telemetry/ingest/envelope.py tests/test_envelope.py
git commit -m "feat(cdc): topic routing and a decoder that never drops a message"
```

---

## Task 8: The bronze writer

**Files:**
- Create: `src/fleet_telemetry/load/writer.py`
- Test: `tests/test_bronze_load.py`

**Interfaces:**
- Consumes: `envelope.DecodedValue`, `schema.apply`
- Produces: `writer.BronzeRow` (NamedTuple: `table, topic, partition, offset, timestamp, payload, raw_payload, parse_error`), `writer.write(conn, rows) -> tuple[int, int]` returning `(inserted, suppressed)`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_bronze_load.py`:

```python
from fleet_telemetry.load import writer


def _row(offset: int, table: str = "raw_ping_events", **kwargs) -> writer.BronzeRow:
    defaults = {
        "table": table,
        "topic": "fleet.public.pings",
        "partition": 0,
        "offset": offset,
        "timestamp": 1_754_568_000_000,
        "payload": json.dumps({"op": "c", "after": {"ping_id": f"id-{offset}"}}),
        "raw_payload": None,
        "parse_error": None,
    }
    return writer.BronzeRow(**{**defaults, **kwargs})


def test_write_reports_inserted_and_suppressed(conn):
    """The suppressed count is the point. At-least-once delivery is a slogan until it is a
    number you can watch go up after a restart."""
    rows = [_row(4_000_001), _row(4_000_002)]
    inserted, suppressed = writer.write(conn, rows)
    assert (inserted, suppressed) == (2, 0)

    replayed, suppressed = writer.write(conn, rows + [_row(4_000_003)])
    assert (replayed, suppressed) == (1, 2)
    conn.rollback()


def test_a_batch_spanning_two_tables_writes_to_both(conn):
    """One poll returns messages from every subscribed topic, so a batch is heterogeneous."""
    rows = [
        _row(5_000_001, table="raw_ping_events"),
        _row(
            5_000_001,
            table="raw_cdc_entities",
            topic="fleet.public.vehicles",
            payload=json.dumps({"op": "u", "source": {"table": "vehicles"}}),
        ),
    ]
    inserted, suppressed = writer.write(conn, rows)
    assert (inserted, suppressed) == (2, 0)
    conn.rollback()


def test_a_malformed_row_does_not_take_the_batch_with_it(conn):
    """The failure mode that would defeat the whole design: one bad message rolling back the
    good ones, so the offsets advance past data that never landed."""
    rows = [
        _row(6_000_001),
        _row(6_000_002, payload=None, raw_payload="{not json", parse_error="Expecting"),
        _row(6_000_003),
    ]
    inserted, suppressed = writer.write(conn, rows)
    assert (inserted, suppressed) == (3, 0)
    conn.rollback()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_bronze_load.py -v -m integration -k "suppressed or batch or malformed"`
Expected: FAIL — `ImportError: cannot import name 'writer'`

- [ ] **Step 3: Write the writer**

`src/fleet_telemetry/load/writer.py`:

```python
"""Insert decoded messages into bronze. The only thing in this project that writes bronze.*.

No parsing, no typing, no deduplication of business keys -- all of that is dbt's
(src/fleet_telemetry/load/__init__.py:3-4). What happens here is one INSERT per bronze table
per batch, and a count of how many rows the unique index refused.

THAT COUNT IS THE INTERESTING OUTPUT. The consumer commits its Kafka offsets only after the
Postgres transaction commits, so a crash in between replays messages that already landed. The
unique index on the Kafka coordinate absorbs them, and `suppressed` is how many. Zero forever
means nothing has crashed yet; a jump after a restart is the delivery guarantee doing exactly
what it says.

`executemany` rather than COPY: COPY is faster and cannot do ON CONFLICT, and correctness under
replay is worth more here than throughput at 8 events a second
(docs/source-system-reference.md, continuous mode). If the load ever justifies it, the shape is
COPY into an UNLOGGED staging table then INSERT ... SELECT ... ON CONFLICT -- measure first.
"""

from __future__ import annotations

from collections import defaultdict
from typing import NamedTuple

from psycopg import Connection


class BronzeRow(NamedTuple):
    """One message, already decoded, addressed to one bronze table."""

    table: str
    topic: str
    partition: int
    offset: int
    timestamp: int | None
    payload: str | None
    raw_payload: str | None
    parse_error: str | None


# raw_cdc_entities carries four topics, so its uniqueness includes the topic. The other two
# carry one each, where (partition, offset) is already unique and the narrower index is cheaper
# on a table headed for ~22M rows.
_CONFLICT_TARGET = {
    "raw_ping_events": "(_kafka_partition, _kafka_offset)",
    "raw_job_events": "(_kafka_partition, _kafka_offset)",
    "raw_cdc_entities": "(_topic, _kafka_partition, _kafka_offset)",
}


def write(conn: Connection, rows: list[BronzeRow]) -> tuple[int, int]:
    """Insert a heterogeneous batch. Returns (inserted, suppressed).

    Does NOT commit. The caller owns the transaction boundary, because the whole restart
    guarantee depends on the Kafka offset commit happening strictly after the database one.
    """
    if not rows:
        return 0, 0

    by_table: dict[str, list[BronzeRow]] = defaultdict(list)
    for row in rows:
        by_table[row.table].append(row)

    inserted = 0
    with conn.cursor() as cur:
        for table, batch in by_table.items():
            cur.executemany(
                f"insert into bronze.{table} "
                "(_topic, _kafka_partition, _kafka_offset, _kafka_timestamp, "
                " payload, raw_payload, parse_error) "
                "values (%s, %s, %s, %s, %s, %s, %s) "
                f"on conflict {_CONFLICT_TARGET[table]} do nothing",
                [
                    (
                        row.topic,
                        row.partition,
                        row.offset,
                        row.timestamp,
                        row.payload,
                        row.raw_payload,
                        row.parse_error,
                    )
                    for row in batch
                ],
            )
            # psycopg 3 accumulates rowcount across an executemany, and ON CONFLICT DO NOTHING
            # reports only the rows that actually landed.
            inserted += cur.rowcount

    return inserted, len(rows) - inserted
```

- [ ] **Step 4: Run the tests**

Run: `pytest tests/test_bronze_load.py -v -m integration`
Expected: 8 passed.

- [ ] **Step 5: Commit**

```bash
ruff check . && ruff format --check .
git add src/fleet_telemetry/load/writer.py tests/test_bronze_load.py
git commit -m "feat(bronze): batch writer reporting inserted and suppressed counts"
```

---

## Task 9: The consumer loop

**Files:**
- Create: `src/fleet_telemetry/ingest/consumer.py`
- Test: `tests/test_bronze_load.py`

**Interfaces:**
- Consumes: `envelope.table_for_topic`, `envelope.decode`, `writer.BronzeRow`, `writer.write`, `schema.apply`, `config.kafka`
- Produces: `consumer.to_rows(messages) -> tuple[list[BronzeRow], list[str]]` (rows, unrouted topics), `consumer.run(max_batches=None) -> None`, `consumer.main() -> int`

- [ ] **Step 1: Write the failing hermetic test**

Create `tests/test_consumer.py`:

```python
"""The consumer's message-to-row mapping. Hermetic -- no broker, no database.

confluent_kafka is imported inside the test rather than at module scope, and skipped if
absent, so CI (which installs [dev] only) still runs everything above it.
"""

from __future__ import annotations

import json

from fleet_telemetry.ingest import consumer


class FakeMessage:
    """The four accessors the consumer uses. Not a mock of the client -- a stand-in for one
    message, which is a value, not a behaviour."""

    def __init__(self, topic, partition, offset, value, timestamp=(1, 1_754_568_000_000)):
        self._topic, self._partition, self._offset = topic, partition, offset
        self._value, self._timestamp = value, timestamp

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def timestamp(self):
        return self._timestamp

    def error(self):
        return None


def test_messages_map_to_rows_addressed_to_the_right_tables():
    messages = [
        FakeMessage("fleet.public.pings", 0, 10, json.dumps({"op": "c"}).encode()),
        FakeMessage("fleet.public.vehicles", 0, 3, json.dumps({"op": "u"}).encode()),
    ]
    rows, unrouted = consumer.to_rows(messages)
    assert [row.table for row in rows] == ["raw_ping_events", "raw_cdc_entities"]
    assert rows[0].offset == 10
    assert unrouted == []


def test_an_unrouted_topic_is_reported_not_silently_dropped():
    messages = [FakeMessage("fleet.public.audit_log", 0, 1, b"{}")]
    rows, unrouted = consumer.to_rows(messages)
    assert rows == []
    assert unrouted == ["fleet.public.audit_log"]


def test_a_malformed_message_becomes_a_row_carrying_its_error():
    messages = [FakeMessage("fleet.public.pings", 0, 11, b"{not json")]
    rows, _ = consumer.to_rows(messages)
    assert rows[0].payload is None
    assert rows[0].raw_payload == "{not json"
    assert rows[0].parse_error


def test_a_message_with_no_broker_timestamp_still_maps():
    """timestamp() returns (TIMESTAMP_NOT_AVAILABLE, -1) when the producer set none."""
    messages = [FakeMessage("fleet.public.pings", 0, 12, b"{}", timestamp=(0, -1))]
    rows, _ = consumer.to_rows(messages)
    assert rows[0].timestamp is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_consumer.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'fleet_telemetry.ingest.consumer'`

- [ ] **Step 3: Write the consumer**

`src/fleet_telemetry/ingest/consumer.py`:

```python
"""Read the change topics and land every message in bronze. Append-only, restart-safe.

THE ORDER OF THE TWO COMMITS IS THE ENTIRE DESIGN.

    1. poll a batch
    2. INSERT ... ON CONFLICT DO NOTHING
    3. Postgres COMMIT
    4. consumer.commit()

Nothing is skipped, because the Kafka offset only advances after the rows are durable: crash
before step 3 and the same messages are redelivered. Nothing is duplicated, because a redelivery
carries the same (partition, offset) and the unique index refuses it.

Reversing 3 and 4 -- or leaving enable.auto.commit at its default of true, which effectively
does reverse them -- produces silent data loss. The loader reports success, the offsets are
past the data, and nothing anywhere says a batch went missing. That is why auto-commit is
turned off explicitly rather than left alone.

WHY THE DEDUPLICATION KEY IS THE KAFKA COORDINATE AND NOT ping_id.
The application already removed device retries with ON CONFLICT (ping_id) DO NOTHING
(docker/oltp/init.sql:189-191), so the OLTP holds one row per ping and Debezium emits one
message for it. A duplicate arriving here can therefore only be a broker redelivery, which the
coordinate identifies exactly. ping_id would be wrong twice over: it is null for deletes, and
a re-snapshot (op='r' after an earlier op='c') is a genuinely new observation that ping_id
would collapse into the create it re-reads.

    python -m fleet_telemetry.ingest.consumer
    python -m fleet_telemetry.ingest.consumer --max-batches 5
"""

from __future__ import annotations

import argparse
import signal
from collections.abc import Sequence
from typing import Any

from psycopg import connect

from fleet_telemetry import config
from fleet_telemetry.ingest import envelope
from fleet_telemetry.load import schema, writer

# Messages per poll. Large enough that one transaction covers many rows; small enough that a
# crash replays little and the suppressed count stays readable.
BATCH_SIZE = 500

# Seconds to wait for a full batch before taking what has arrived. Bounds how long a message
# sits unlanded when the stream is quiet.
POLL_TIMEOUT = 2.0


def to_rows(messages: Sequence[Any]) -> tuple[list[writer.BronzeRow], list[str]]:
    """Map messages to bronze rows. Returns (rows, topics that routed nowhere).

    Unrouted topics are returned rather than logged-and-forgotten because the caller has to
    decide: a topic nobody planned for means table.include.list changed, and quietly skipping
    it while advancing the offset past it is unrecoverable once retention expires.
    """
    rows: list[writer.BronzeRow] = []
    unrouted: list[str] = []

    for message in messages:
        topic = message.topic()
        table = envelope.table_for_topic(topic)
        if table is None:
            unrouted.append(topic)
            continue

        payload, raw_payload, parse_error = envelope.decode(message.value())
        # (TIMESTAMP_NOT_AVAILABLE, -1) when the producer set none.
        kind, value = message.timestamp()
        rows.append(
            writer.BronzeRow(
                table=table,
                topic=topic,
                partition=message.partition(),
                offset=message.offset(),
                timestamp=value if kind else None,
                payload=payload,
                raw_payload=raw_payload,
                parse_error=parse_error,
            )
        )

    return rows, unrouted


def run(max_batches: int | None = None) -> None:
    """Consume until interrupted, or for a fixed number of batches."""
    from confluent_kafka import Consumer  # noqa: PLC0415 - see the module docstring in envelope

    broker = config.kafka()
    client = Consumer(
        {
            "bootstrap.servers": broker.bootstrap_servers,
            "group.id": broker.consumer_group,
            # The whole point. See the module docstring.
            "enable.auto.commit": False,
            # A new group starts from the beginning of every topic, so the Debezium snapshot
            # is not skipped. The default, "latest", would silently discard it.
            "auto.offset.reset": "earliest",
        }
    )
    client.subscribe(sorted(envelope.TOPIC_TABLES))

    stopping = False

    def _stop(*_: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    totals = {"inserted": 0, "suppressed": 0, "malformed": 0}
    batches = 0

    with connect(config.warehouse().dsn()) as conn:
        schema.apply(conn)
        try:
            while not stopping and (max_batches is None or batches < max_batches):
                messages = client.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT)
                messages = [m for m in messages if m.error() is None]
                if not messages:
                    continue

                rows, unrouted = to_rows(messages)
                for topic in sorted(set(unrouted)):
                    print(f"WARNING: no bronze table for topic {topic}; not committing past it")
                if unrouted:
                    # Refusing to advance is deliberate. Skipping an unrouted topic loses it
                    # permanently once retention expires, and the offsets would show no gap.
                    raise RuntimeError(f"unrouted topics: {sorted(set(unrouted))}")

                inserted, suppressed = writer.write(conn, rows)
                conn.commit()          # (3) rows are durable
                client.commit(asynchronous=False)  # (4) only now may the offset move

                malformed = sum(1 for row in rows if row.parse_error)
                totals["inserted"] += inserted
                totals["suppressed"] += suppressed
                totals["malformed"] += malformed
                batches += 1
                print(
                    f"batch {batches}: {inserted} inserted, {suppressed} suppressed, "
                    f"{malformed} malformed"
                )
        finally:
            client.close()

    print(
        f"stopped after {batches} batches: {totals['inserted']} inserted, "
        f"{totals['suppressed']} suppressed by the dedup index, "
        f"{totals['malformed']} stored with a parse error"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Land Debezium change events in bronze.*")
    parser.add_argument("--max-batches", type=int, default=None)
    args = parser.parse_args()
    run(args.max_batches)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the hermetic tests**

Run: `pytest tests/test_consumer.py -v`
Expected: 4 passed.

- [ ] **Step 5: Write the restart test**

Append to `tests/test_bronze_load.py`:

```python
@pytest.mark.integration
def test_a_rewound_consumer_group_does_not_duplicate_bronze(conn):
    """The restart guarantee, end to end and against a real broker.

    Rewinding the group is the strongest possible version of a restart: it replays messages
    that certainly landed. If bronze grows, the dedup key is wrong.
    """
    pytest.importorskip("confluent_kafka")
    from fleet_telemetry.ingest import consumer

    def bronze_count() -> int:
        with conn.cursor() as cur:
            cur.execute(
                "select (select count(*) from bronze.raw_ping_events) "
                "     + (select count(*) from bronze.raw_cdc_entities) "
                "     + (select count(*) from bronze.raw_job_events)"
            )
            return cur.fetchone()[0]

    consumer.run(max_batches=3)
    after_first = bronze_count()
    assert after_first > 0, "no events consumed; is the connector registered and the simulator up?"

    # Rewind by hand -- the same thing `rpk group seek bronze-loader --to start` does.
    from confluent_kafka import Consumer, TopicPartition

    broker = config.kafka()
    client = Consumer(
        {
            "bootstrap.servers": broker.bootstrap_servers,
            "group.id": broker.consumer_group,
            "enable.auto.commit": False,
        }
    )
    client.commit(
        offsets=[TopicPartition(topic, 0, 0) for topic in sorted(consumer.envelope.TOPIC_TABLES)],
        asynchronous=False,
    )
    client.close()

    consumer.run(max_batches=3)
    assert bronze_count() == after_first, "a replay added rows; the dedup key is not doing its job"
```

- [ ] **Step 6: Run it end to end**

```bash
docker compose -f docker/docker-compose.yml up -d
python -m fleet_telemetry.ingest.connector --register
python -m fleet_telemetry.ingest.consumer --max-batches 20
pytest tests/test_bronze_load.py -v -m integration
```
Expected: batches report inserted counts; the restart test passes with a suppressed count equal to the replayed rows.

- [ ] **Step 7: Commit**

```bash
ruff check . && ruff format --check .
git add src/fleet_telemetry/ingest/consumer.py tests/test_consumer.py tests/test_bronze_load.py
git commit -m "feat(cdc): bronze consumer committing offsets only after the database commit"
```

---

## Task 10: Close the source contract

**Files:**
- Modify: `dbt/models/staging/_sources.yml`, `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: the bronze tables from Task 1.

- [ ] **Step 1: Replace the stale header and add `poll_rows`**

In `dbt/models/staging/_sources.yml`, replace lines 6-9 (which say "Tables land in phase 1" and defer the tests) with:

```yaml
# Tables land in phase 2, created by src/fleet_telemetry/load/schema.py. The `tests:` keys below
# arrive in the same change, as that header promised: dbt build runs source tests eagerly, so a
# not_null test against a table that does not exist fails the build rather than reporting
# anything useful.
#
# EVERY SCALAR COLUMN HERE IS TEXT, INCLUDING THE OBVIOUSLY NUMERIC ONES. They are generated
# columns, and a cast inside GENERATED ALWAYS is evaluated on INSERT -- so a device sending a
# ping_id of "banana" would raise and take the whole batch with it, making Bronze reject exactly
# the malformed evidence it exists to keep. Silver casts, where a bad value is a failing test
# rather than lost data.
```

Add the tests to each table. On `raw_ping_events`:

```yaml
        columns:
          - name: ping_id
            description: >
              Client-generated UUID, the idempotency key. A retrying device resends the same
              ping_id, so deduplication is exact rather than heuristic. Text here, not uuid --
              see the header.
          - name: _kafka_offset
            description: >
              The broker's coordinate for this message, and Bronze's deduplication key. The
              application already removed device retries, so a duplicate reaching Bronze can
              only be a redelivery -- which this identifies exactly and ping_id does not.
            tests:
              - not_null
          - name: _ingested_at
            description: When Bronze wrote the row.
            tests:
              - not_null
        tests:
          - dbt_utils.unique_combination_of_columns:
              combination_of_columns: ["_kafka_partition", "_kafka_offset"]
```

> If `dbt_utils` is not already a dependency, use a plain `unique` test on `_kafka_offset` instead and note in the comment that it holds only while the topic has one partition. Adding a package for one test is not worth a new `packages.yml` at this point.

Add `poll_rows` as a documented dead end:

```yaml
      - name: poll_rows
        description: >
          Output of the naive batch poller (src/fleet_telemetry/ingest/poller.py), kept so the
          two ingestion paths can be diffed against one simulator run. NOT a source for any
          Silver model, deliberately: it holds current-state row images, so it has no op code
          and no before-image, and it is missing every change that happened between two polls
          plus every delete that ever occurred. The absence is the finding -- see
          docs/learn/02-ingestion.md.
        columns:
          - name: source_table
            description: Which OLTP table the row image came from.
          - name: row_image
            description: The row exactly as the poller selected it. Current state, not a change.
          - name: watermark_value
            description: >
              The watermark value that caused this row to be selected. Kept so a leaked
              watermark is diagnosable after the fact rather than merely suspected.
```

- [ ] **Step 2: Verify dbt still parses**

Run: `dbt parse --project-dir dbt --profiles-dir dbt`
Expected: success.

- [ ] **Step 3: Verify the source tests run and pass**

Run: `dbt build --project-dir dbt --profiles-dir dbt`
Expected: source tests execute against the real bronze tables and pass.

- [ ] **Step 4: Make CI able to run them**

In `.github/workflows/ci.yml`, add after the "Create medallion schemas" step (line 79-85):

```yaml
      # Bronze is owned by the Python loader, not by dbt, so the tables do not exist until this
      # runs -- and the source tests added in phase 2 would fail the build without them.
      - name: Apply the bronze schema
        run: python -m fleet_telemetry.load.schema
```

- [ ] **Step 5: Verify against the CI target locally**

Run: `dbt build --project-dir dbt --profiles-dir dbt --target ci` with `WAREHOUSE_PORT=55432` set (the local stack stands in for the service container).
Expected: success.

- [ ] **Step 6: Commit**

```bash
git add dbt/models/staging/_sources.yml .github/workflows/ci.yml
git commit -m "feat(bronze): close the source contract and let CI build against real bronze tables"
```

---

## Task 11: The comparison

**Files:**
- Modify: `src/fleet_telemetry/ingest/compare.py`

**Interfaces:**
- Produces: `compare.cdc_vs_poller(oltp_conn, wh_conn) -> dict[str, Any]`, `compare.main() -> int`

- [ ] **Step 1: Add the CDC side**

Append to `src/fleet_telemetry/ingest/compare.py`:

```python
import argparse
import json

from psycopg import connect

from fleet_telemetry import config


def cdc_vs_poller(oltp_conn: Connection, wh_conn: Connection) -> dict[str, Any]:
    """What each path captured over the same window. Every number from a query."""
    out: dict[str, Any] = {}
    with wh_conn.cursor(row_factory=dict_row) as cur:
        # Changes per entity, per path. The poller reports rows it observed; CDC reports
        # committed changes. They are not the same quantity, and the gap is the point.
        cur.execute(
            """
            select source_table,
                   count(*)                                as cdc_events,
                   count(*) filter (where op = 'c')        as creates,
                   count(*) filter (where op = 'u')        as updates,
                   count(*) filter (where op = 'd')        as deletes,
                   count(*) filter (where op = 'r')        as snapshot_reads
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
        out["deletes_seen_by_poller"] = 0  # structurally, not empirically -- see poller.py

        # Changes CDC saw for a key that the poller recorded only once. Each is a state the
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

    print(json.dumps(findings, indent=2, default=str))
    if args.json:
        Path(args.json).write_text(json.dumps(findings, indent=2, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

Add `from pathlib import Path` to the imports at the top of the module.

- [ ] **Step 2: Generate a comparable run**

```bash
docker compose -f docker/docker-compose.yml down -v
docker compose -f docker/docker-compose.yml up -d
python -m fleet_telemetry.ingest.connector --register
python -m fleet_telemetry.load.schema
```

Then, in three terminals: the consumer (`python -m fleet_telemetry.ingest.consumer`), the poller (`python -m fleet_telemetry.ingest.poller --interval 10`), and a script that exercises the mutable entities through the API — reassign a vehicle twice within one poll interval, and delete one:

```bash
curl -X PATCH http://127.0.0.1:8000/vehicles/1 -H "Authorization: Bearer local-dev-token" -H "Content-Type: application/json" -d '{"current_driver_id": 5}'
curl -X PATCH http://127.0.0.1:8000/vehicles/1 -H "Authorization: Bearer local-dev-token" -H "Content-Type: application/json" -d '{"current_driver_id": 1}'
curl -X DELETE http://127.0.0.1:8000/vehicles/40 -H "Authorization: Bearer local-dev-token"
```

- [ ] **Step 3: Run the comparison**

Run: `python -m fleet_telemetry.ingest.compare --json compare.json`
Expected: `deletes_seen_by_cdc` ≥ 1, `deletes_seen_by_poller` 0, and vehicle 1 appearing in `changes_the_poller_collapsed`.

- [ ] **Step 4: Commit**

```bash
ruff check . && ruff format --check .
git add src/fleet_telemetry/ingest/compare.py
git commit -m "feat(phase-2): query-derived comparison of what each ingestion path captured"
```

---

## Task 12: The guide, and the docs that point at it

**Files:**
- Create: `docs/learn/02-ingestion.md`
- Modify: `README.md`, `CLAUDE.md`

- [ ] **Step 1: Write `docs/learn/02-ingestion.md`**

Follow the shape of `docs/learn/01-source-system.md`. Required content:

1. **What phase 2 answers**, and why the poller comes first (spec `:370-376`).
2. **The poller**, and its three failures with the numbers you measured in Task 4 Step 5 and Task 11 Step 3 — not the illustrative ones in this plan. Every number from a query, per the rule in `src/fleet_telemetry/profile_source.py:16-18`.
3. **The diff table**: rows per path per table, deletes per path, changes collapsed. Pasted from `compare.json`.
4. **What CDC cost**, honestly: two containers, a replication slot that must be drained, an initial snapshot of ~172,800 ping rows, and a broker that is not durable storage.
5. **Why bronze exists downstream of a broker** (spec `:257-258`).
6. **What broke**, written before it was fixed — per `docs/learn/README.md` and `CLAUDE.md`. At minimum the two-listener split from Task 5 if it bit, and whether `CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE` was right first time.
7. **The three explain-back questions**, verbatim from spec `:384-386`, answered from memory:
   - Why can a batch poller never detect a delete?
   - What does at-least-once mean for your Bronze table, and which column saves you?
   - What happens to a replication slot if the consumer stops for a week?

- [ ] **Step 2: Update the README**

- `README.md:75-76` — expand the `ingest/` and `load/` lines to name the modules.
- Add a "Running the ingestion" section beside the existing "Running the source system": `docker compose up -d`, `python -m fleet_telemetry.ingest.connector --register`, `python -m fleet_telemetry.ingest.poller`, `python -m fleet_telemetry.ingest.consumer`, `python -m fleet_telemetry.ingest.compare`.
- Note that `[warehouse]` is now required: `pip install -e ".[app,warehouse,dev]"`.

- [ ] **Step 3: Update CLAUDE.md**

- Build phases table: mark phase 2 complete.
- Module boundaries table: name the new modules under `src/fleet_telemetry/ingest/` and `load/`.
- Key Patterns: add the offset-ordering rule ("commit Kafka offsets only after the database commit; reversing them is silent data loss") and the no-casting-generated-columns rule ("Bronze must not contain a cast that can reject a row").
- Build & Test Commands: add the connector registration step and the `[warehouse]` extra.

- [ ] **Step 4: Full verification**

```bash
ruff check . && ruff format --check .
pytest
dbt build --project-dir dbt --profiles-dir dbt
```
Expected: all green.

- [ ] **Step 5: Commit and open the PR**

```bash
git add docs/learn/02-ingestion.md README.md CLAUDE.md
git commit -m "docs(phase-2): the ingestion guide and the poller-vs-CDC diff"
git push -u origin phase-2/cdc
```

---

## Verification

**Acceptance criteria, mapped to the task statement:**

| Requirement | How it is verified |
| --- | --- |
| Events stored verbatim, including malformed ones | `tests/test_bronze_load.py::test_a_malformed_payload_is_stored_rather_than_rejected`, `::test_a_ping_id_that_is_not_a_uuid_still_lands`; `tests/test_envelope.py` (7 cases including tombstone and non-UTF-8) |
| Consumer offsets survive a restart; a restart does not duplicate or skip | `tests/test_bronze_load.py::test_a_rewound_consumer_group_does_not_duplicate_bronze` — replays committed messages and asserts the row count is unchanged |
| Deduplication where at-least-once requires it | `::test_the_same_kafka_offset_cannot_land_twice`, `::test_write_reports_inserted_and_suppressed`; the per-batch `suppressed` count printed by the consumer |
| Both streams land | `compare.cdc_vs_poller` reports rows in `raw_ping_events` and per-op counts in `raw_cdc_entities`; `tests/test_envelope.py::test_the_three_streams_route_to_three_tables` |
| Entity changes carry before/after plus an op code | `::test_generated_columns_project_the_payload`; the `NO ExtractNewRecordState SMT` decision in `connector.py` |
| Bronze is the durable record | `docs/learn/02-ingestion.md` §5; the connector's `snapshot.mode=initial` means bronze holds history the topic will eventually drop |
| **spec `:378-379` deliverable** — both paths populated, written comparison | `bronze.poll_rows` + the three `raw_*` tables; `docs/learn/02-ingestion.md` §3 |

**End-to-end run, from nothing:**

```bash
docker compose -f docker/docker-compose.yml down -v
pip install -e ".[app,warehouse,dev]"
docker compose -f docker/docker-compose.yml up -d
python -m fleet_telemetry.config                       # confirm nothing unexpected defaulted
python -m fleet_telemetry.load.schema
python -m fleet_telemetry.ingest.connector --register
python -m fleet_telemetry.ingest.consumer --max-batches 20
python -m fleet_telemetry.ingest.poller --cycles 3 --interval 10
python -m fleet_telemetry.ingest.compare
pytest
dbt build --project-dir dbt --profiles-dir dbt
```

**Checks that catch the failure modes this design is built around:**

```bash
# The replication slot is being drained. wal_status must stay 'reserved'; 'extended' or 'lost'
# means the consumer has fallen behind far enough for Postgres to start worrying about disk.
psql "postgresql://fleet:fleet@127.0.0.1:55433/fleet" \
  -c "select slot_name, active, wal_status, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as retained from pg_replication_slots"

# Nothing malformed is hiding. Non-zero is not a failure -- it is the evidence -- but it should
# be a number you have looked at.
psql "postgresql://telemetry:telemetry@127.0.0.1:55432/telemetry" \
  -c "select count(*) filter (where parse_error is not null) as malformed, count(*) from bronze.raw_ping_events"

# The dedup index is real, not aspirational.
psql "postgresql://telemetry:telemetry@127.0.0.1:55432/telemetry" \
  -c "select indexname, indexdef from pg_indexes where schemaname = 'bronze' and indexname like '%kafka_uk'"

# Stop the consumer, leave it stopped for ten minutes, and watch `retained` grow. That is the
# answer to the third explain-back question, observed rather than recited.
```

**Teardown, when the slot is no longer wanted** — this is the one that fills a disk if forgotten:

```bash
python -m fleet_telemetry.ingest.connector --delete
psql "postgresql://fleet:fleet@127.0.0.1:55433/fleet" -c "select pg_drop_replication_slot('fleet_debezium')"
```
