# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

An end-to-end data platform built to learn data engineering by owning **both** halves of the
pipeline: the application that produces events, and the warehouse that consumes them. The domain
is vehicle telemetry for informal transport in The Gambia. Nobody will use the application — it
exists to emit realistically-shaped events.

**The bar is comprehension, not completion.** A working pipeline nobody can explain is a failed
outcome here. This shapes how to work in the repo: the extensive comments explaining *why* are
the deliverable, not decoration. Do not strip them, and match their density when adding code.

**The one hard problem:** events arrive late, out of order, and in bursts; the warehouse must
still be correct. Event time vs processing time, watermarks, lateness bounds, restating
aggregates. It is verifiable because the simulator knows the truth it generated, so output can be
checked against ground truth rather than merely looking plausible.

Read `docs/learn/README.md` (the curriculum) and
`docs/superpowers/specs/2026-08-07-telemetry-platform-design.md` (the design) before making
architectural decisions. `docs/archive/` is an abandoned aviation project — not current.

## Build & Test Commands

```bash
pip install -e ".[app,warehouse,dev]"   # or: uv sync --extra app --extra warehouse --extra dev
pre-commit install                   # ruff + secret scanning; without this the hooks never run
docker compose -f docker/docker-compose.yml up -d
python -m fleet_telemetry.config     # shows what resolved and what fell back to a default
python -m fleet_telemetry.ingest.connector --register   # phase 2: Debezium onto the WAL
pytest
```

Extras arrive with the phase that needs them: `[warehouse]` (dbt, Kafka client) landed in phase
2, `[airflow]` in phase 4. `[dev]` alone cannot run the suite — it imports the application and
the loader, so `[app]` and `[warehouse]` are both required.

### Ingestion (phase 2)

```bash
python -m fleet_telemetry.load.schema                    # bronze DDL, idempotent
python -m fleet_telemetry.ingest.connector --register|--status|--delete
python -m fleet_telemetry.ingest.consumer [--max-batches N]   # CDC  -> bronze.raw_*
python -m fleet_telemetry.ingest.poller [--cycles N] [--interval S]  # -> bronze.poll_rows
python -m fleet_telemetry.ingest.compare [--json out.json]    # the phase 2 diff
```

Deleting the connector does not drop its replication slot, deliberately, so it can be recreated
and resume. Drop it explicitly with `select pg_drop_replication_slot('fleet_debezium')` when
finished, or Postgres retains WAL until the disk fills.

### Transformation (phase 3)

```bash
python -m fleet_telemetry.transform.run      # sql/silver/*.sql -> silver_manual.*
python -m fleet_telemetry.truth              # truth schema DDL, idempotent
python -m simulator --vehicles 40 --hours 6 --truth   # record intent while emitting
```

Runs the hand-written Silver scripts in **filename order**, with no dependency resolution — that
absence is the phase 3 step 1 lesson, and the findings are in `docs/silver-by-hand.md`. Target
schema is `silver_manual`, never `silver`: dbt owns `silver` and would drop same-named tables on its
first run, destroying the artifact and the dbt-vs-hand diff with it. `tests/test_transform.py` pins
both the schema name and the script order.

**Silver now exists twice, and both survive on purpose.** `sql/silver/` builds `silver_manual` and
is the kept phase-3-step-1 artifact — the same treatment `poller.py` got in phase 2. The nine dbt
models in `dbt/models/staging/` build `silver` and are what everything downstream reads. Model names
are identical across the two schemas so the diff is a one-liner; `rejected_rows` is the one
exception, since staging requires the `stg_` prefix. `tests/test_dbt_silver.py` asserts all nine
pairs match in both directions, and the findings are in `docs/silver-in-dbt.md`.

**All nine scripts are ported — no schema is half-owned.** `ping_quality` and `vehicle_day` land in
`silver` too, keeping their unprefixed names since they are aggregates over the `stg_` models rather
than stagings of a source. Note the tension that buys: the design spec's layer table gives Gold every
judgement call, and p50/p99, bucketing by `device_ts`, and the inner join on the vehicle dimension are
all judgements. Whole-layer-in-one-place and a complete diff won over a clean boundary; it resolves
when the dimensional model turns these two into mart inputs. `gold/` and `marts/` stay empty until
their own tickets.

**`vehicle_day` is the only model whose grain assertion can fail on a live hazard** — it joins three
relations, so a duplicate upstream doubles every count, speed and distance at once. The other eight
are `DISTINCT ON` or a single-relation `GROUP BY`, where Postgres guarantees the grain and the test is
a regression guard. And no grain assertion catches rows the inner join *drops*, which is why that
wrongness is documented in the model rather than tested for.

```bash
python -m dbt.cli.main snapshot --project-dir dbt --profiles-dir dbt   # -> gold.snap_vehicles / snap_drivers
```

**`dbt build` runs snapshots too** — a snapshot is just another node in the DAG, so the polling
interval this project's snapshots actually get is not "whenever `dbt snapshot` is run" but "every
explicit `dbt snapshot` plus every `dbt build` anyone happens to run for any other reason." That
makes the snapshot's effective interval irregular and dependent on who else is working in the repo,
which is exactly the property `docs/type-2-dimensions.md` measures against CDC.

### Tests

```bash
pytest                               # everything; needs the oltp container
pytest -m "not integration"          # pure logic only, no docker
pytest -m integration                # only the tests that need the database
pytest tests/test_world.py           # one file
pytest tests/test_config.py::test_env_var_beats_default   # one test
pytest -k lateness                   # by name substring
```

`tests/test_app.py` is entirely `integration` (module-level `pytestmark`). `test_config.py` and
`test_world.py` are hermetic.

**`pytest` and `python -m pytest` were not equivalent, and CI ran the one that failed.** Only
`src/fleet_telemetry` is packaged, so `app` and `simulator` are importable only with the repo root
on `sys.path` — `python -m pytest` puts it there, bare `pytest` does not. Three modules importing
`simulator` therefore collected for anyone typing `python -m pytest` and raised
`ModuleNotFoundError` in CI: **every CI run from 2026-08-10 to 2026-08-28 failed this way** while
local runs looked green. Fixed by `pythonpath = ["."]` in `[tool.pytest.ini_options]`. Do not
"fix" it instead by adding `app`/`simulator` to the wheel — they are the source system, not
library code.

### Lint & format

```bash
ruff check .
ruff format .
ruff format --check .                # what CI runs
```

Before pushing: `ruff check . && pytest`.

### Running the source system

Three processes. On Windows PowerShell set the env var separately — there is no inline prefix.

```bash
docker compose -f docker/docker-compose.yml up -d oltp
FLEET_ALLOW_SERVER_TS_OVERRIDE=true uvicorn app.main:app --port 8000
python -m simulator --vehicles 40 --hours 6
python -m fleet_telemetry.profile_source
```

`http://127.0.0.1:8000/docs` is the generated API browser.

Simulator flags: `--vehicles`, `--hours`/`--minutes`, `--interval`, `--seed`, `--api`, `--live`,
`--reset`, `--truth`. Without `--reset`, runs append and sequence numbers resume from the database —
that is deliberate (see Key Patterns). `--truth` is the only flag that needs the warehouse, and is
off by default for that reason.

### dbt

```bash
dbt parse --project-dir dbt --profiles-dir dbt
dbt build --project-dir dbt --profiles-dir dbt      # runs models AND every test
dbt build --project-dir dbt --profiles-dir dbt --target ci
```

The `--project-dir`/`--profiles-dir` flags are required; the profile is not in `~/.dbt`.

**dbt Fusion cannot build this project, and the way it reaches you is the VS Code extension.**
Fusion does not support the Postgres adapter and fails with *"The 'postgres' adapter is not yet
supported by dbt Fusion"* — which reads like a missing dependency rather than the wrong
executable. This warehouse is Postgres and is not going to stop being Postgres.

**It is not a PATH problem, despite looking exactly like one.** Measured 2026-08-25:
`~/.local/bin` is not on PATH at all, and bare `dbt` resolves to pip's `dbt-core` 1.12.0 in both
PowerShell and Git Bash. The Fusion binary is `~/.local/bin/dbt.exe` — 411 MB of Rust, against
dbt-core's 108 KB Python shim — installed alongside `dbt-wizard.exe` by the `dbtLabsInc.dbt`
extension, which invokes it **by absolute path**. So no PATH edit reaches it; only
`dbt.dbtPath` does, which `.vscode/settings.json` now sets to the dbt-core shim.

That extension is worth watching for a second reason: it twice wrote a duplicate `profile:` key
into `dbt/dbt_project.yml`, which makes `dbt parse` emit `DuplicateYAMLKeysDeprecation`. If you
see a `profile:` line you did not write, `git checkout -- dbt/dbt_project.yml`.

Use `python -m dbt.cli.main ...` regardless. It names the interpreter, so it cannot be
mis-resolved by anything. CI is unaffected: it installs only `.[warehouse]`.

## Architecture

```text
simulator ──HTTP──> FastAPI ──> OLTP Postgres
                                     │
                              Debezium CDC
                                     │
                                 Redpanda
                                     ▼
                     bronze.*  (raw change events, append-only)
                                     ▼
                     silver.stg_*    (typed, deduplicated)
                                     ▼
                     gold.dim_*/fact_*
                                     ▼
                     marts.*
```

**Two ingestion paths, deliberately.** High-volume append-only pings teach throughput and
partitioning; CDC on mutable entities teaches Slowly Changing Dimensions authentically.

**Division of labour: Extract/Load in Python, Transform in SQL, orchestration separate.** Python
only moves bytes. Every business rule is a dbt model, so it stays diffable and testable. Do not
put transformation logic in `src/fleet_telemetry/`.

**Two databases that must never be one.** `oltp` (port 55433) is what the app writes — plain
Postgres 16, normalised, current-state only. `warehouse` (port 55432) is what analytics reads —
PostGIS + TimescaleDB, dimensional, keeps history. Pointing a BI tool at the operational database
is the specific anti-pattern this project exists to replace.

### Module boundaries

| Path | Owns |
| --- | --- |
| `docker/oltp/init.sql` | Six OLTP tables: depots, drivers, vehicles, jobs, job_events, pings |
| `docker/warehouse/init.sql` | PostGIS + TimescaleDB extensions, the four medallion schemas |
| `app/` | The source system. Seven FastAPI endpoints; `main.py`, `models.py` (Pydantic), `db.py` (pool) |
| `simulator/` | `world.py` (physics, no I/O), `run.py` (clock walk + HTTP) |
| `src/fleet_telemetry/config.py` | The only place that knows where settings come from |
| `src/fleet_telemetry/profile_source.py` | Source-system profiler — the phase 1 deliverable |
| `src/fleet_telemetry/truth.py` | The `truth` schema, the intended-ping row shape, the batched writer, and `TruthRecorder`. Not in `load/`, which is scoped to bronze. The only thing that gives the simulator warehouse *write* access |
| `src/fleet_telemetry/ingest/` | `poller.py` (naive path), `connector.py` (Debezium registrar), `envelope.py` (routing/decode, no Kafka import), `consumer.py` (the CDC loop), `compare.py` (the phase 2 diff) |
| `src/fleet_telemetry/load/` | `schema.py` (all bronze DDL), `writer.py` (the only writer of `bronze.raw_*` — the poller owns `poll_rows` and `poll_watermarks`, whose rows have no Kafka coordinate) |
| `src/fleet_telemetry/transform/` | `run.py` — executes `sql/silver/*.sql` in filename order. No business logic; every rule is in the SQL |
| `sql/silver/` | Silver as hand-written SQL (phase 3 step 1), targeting `silver_manual`. Built to be superseded by `dbt/`, and kept afterwards like `poller.py` was |
| `docker/debezium/` | Connector config. Credentials are `${...}` placeholders filled by `connector.py` from `config.py` |
| `dbt/` | Silver, Gold, marts — all business logic. `models/staging/` → `silver` (eleven models, views: eight `stg_*`, `stg_rejected_rows`, `ping_quality`, `vehicle_day`); `macros/generate_schema_name.sql` makes layer names absolute, and deleting it moves every model silently; `tests/` holds one grain assertion per model plus the geometry, reconciliation and dedup-unit assertions. `models/gold/` → `gold` (two Type 2 dimensions, `dim_vehicle` and `dim_driver`, table-materialised, built from `stg_vehicle_versions`/`stg_driver_versions`); `snapshots/` holds `snap_vehicles` and `snap_drivers` — dbt snapshots polling the same current-state tables the batch poller reads, kept as the CDC dimensions' comparison baseline. `marts/` is still empty |
| `dags/` | Airflow DAGs (phase 4) |

Bronze tables are owned by the Python loader, not dbt — declared in
`dbt/models/staging/_sources.yml` as sources so Silver references them via `source()`.

### Build phases

Each phase starts with the obvious hand-rolled version, lets its limits bite, and only then
introduces the real tool — a tool introduced before its problem is a tool you cannot explain.

| Phase | Question | Naive first | Then |
| --- | --- | --- | --- |
| 1 | What data exists, what shape, how often? | — | measure it |
| 2 | How do I get it out without loss or duplication? | batch poller | Debezium CDC |
| 3 | How do I make raw records trustworthy? | SQL scripts | dbt |
| 4 | How do I make it run without me? | shell script + timer | Airflow |
| 5 | How does anyone actually use it? | — | marts + a consuming surface |

Phases 1 and 2 are complete. **Phase 3 is the project** — it holds the lateness problem. If time
runs short, cut Phase 5, then Phase 4; never Phase 3.

## Key Patterns

**Three timestamps, one of them lies.** `device_ts` is client-controlled and NOT trustworthy — a
wrong clock reports a wrong time and no validation can detect it. `server_ts` is ours and is
trustworthy. `server_ts − device_ts` is the lateness. Measured clean baseline: p50 34 s, p99 67 s.
That number is the floor for any watermark; a bound below it would quarantine correct data before
any pathology exists.

**Reject the impossible, record the merely suspicious.** Latitude 900 → 422; it cannot be
corrected later and would poison every aggregate. A device clock three hours fast → accept and
store; it is evidence *about the device*, and discarding it destroys the only signal that
something is wrong. This distinction is a data-modelling decision, not a validation detail.

**Idempotency uses a key the client controls.** `ping_id` is a device-generated UUID reused on
retry, with `ON CONFLICT (ping_id) DO NOTHING`. Duplicates are reported, not hidden — the
duplicate rate is a signal about network conditions. Deduplicating on `(vehicle_id, device_ts)`
looks equivalent and is not: two genuine readings can share a timestamp when a clock is coarse.

**A gap is not a stop.** `sequence_no` is monotonic per vehicle. A gap proves data was lost; an
unchanging position with no gap proves the vehicle stopped. Anything that lets the counter rewind
destroys that signal — which is why `run.py` resumes each counter from the database rather than
starting at zero, and why the app deliberately has *no* unique constraint on
`(vehicle_id, sequence_no)`.

**The privileged backfill path is explicit and off by default.** Writing historical `server_ts`
requires `FLEET_ALLOW_SERVER_TS_OVERRIDE=true`; otherwise the endpoint returns 403. A silent
override would destroy the only trustworthy timestamp in the system. Keep any new privileged path
to the same rule: explicit, disabled by default, impossible to trigger by accident.

**The simulator goes through the API, never the database.** Phase 2's CDC reads what the
*application* committed. A simulator that wrote directly would be testing a pipeline that does not
exist. The static bearer token exists for this reason, not for security.

**`DELETE /vehicles/{id}` is a hard delete on purpose.** A real fleet system would soft-delete.
This one does not, because a deleted row has no `updated_at` and a `WHERE updated_at > watermark`
poller can never see it — the cleanest possible demonstration of why CDC exists.

**Config resolves env var → `.env` → built-in default**, and records which. Defaults mirror
`docker-compose.yml` exactly; if one side changes, change both. A defaulted password is fine
locally and dangerous anywhere else, so `describe()` prints the distinction. Secrets are never
printed — passwords render as presence plus length, DSNs redacted, and two tests assert it.
Loaders take optional `env`/`root` arguments so tests stay hermetic instead of mutating
`os.environ`.

**`dbt/macros/generate_schema_name.sql` is load-bearing, and deleting it fails silently.** dbt's
built-in macro *concatenates* `target.schema` with `+schema`, so with `schema: silver` in
`profiles.yml` and `+schema: silver` in `dbt_project.yml`, models resolve to **`silver_silver`** —
measured. `dbt build` reports success either way: creating a view in the wrong schema is not an
error, the models are correct, their tests pass against them, and `silver` is simply empty. The
override makes layer names absolute. `tests/test_dbt_silver.py::test_silver_is_not_silver_silver`
asserts it from the database, because nothing about a green build reports the destination.

**A grain assertion does not prove a deduplication rule.** It proves the output holds one row per
key, which is true whether the rule keeps the earliest or the latest observation. Bronze holds no
duplicate `ping_id`s, so `DISTINCT ON` discards nothing and both rules emit identical rows — and
reversing the `ORDER BY` in `stg_pings` fails the unit test while the grain assertion still passes.
Rules that real data cannot exercise need a fixture that contains the case, which is what
`dbt/models/staging/_unit_tests.yml` is for. Note dbt materialises a unit test as a relation, so its
name is bound by Postgres's 63-character identifier limit.

**dbt vars encode policy.** `incremental_lookback_days` MUST exceed `lateness_bound_hours` — a
shorter lookback silently loses events that arrived within the accepted bound while the pipeline
reports success. Tests are `+severity: error` project-wide because grain assertions are what stop
fan-out from silently inflating every downstream number.

**Bronze is not optional.** A Kafka topic has a retention window, not a memory. Bronze is the
durable record everything downstream is rebuilt from.

**Commit Kafka offsets only after the database commit.** poll → insert → Postgres COMMIT →
`consumer.commit()`. Reversing the last two — or leaving `enable.auto.commit` at its default of
`true`, which effectively does — turns a crash into silent data loss: the loader reports success
and the offsets sit past data that never landed. The window between the two commits produces
duplicates instead, which the unique index on `(_kafka_partition, _kafka_offset)` absorbs. That
coordinate is the dedup key, **not `ping_id`**: the app already removed device retries, so a
duplicate reaching Bronze can only be a redelivery, and `ping_id` is null for deletes.

**Bronze must not contain a cast that can reject a row.** Every scalar generated column in
`bronze.*` is `text`, including the obviously numeric ones. A cast inside `GENERATED ALWAYS` is
evaluated on INSERT, so `(payload#>>'{after,ping_id}')::uuid` fails the whole batch the first
time a device sends `"banana"` — Bronze rejecting exactly the malformed evidence it exists to
keep. Postgres 16 has no `TRY_CAST`. Silver casts, where a bad value is a failing test.

**Before-images are off by default, and their absence is not empty — it is fabricated.** The
mutable tables are `REPLICA IDENTITY FULL`; without it Postgres logs only the primary key of the
old row, and Debezium fills the rest with *type defaults*, so a delete yields a plausible row
that never existed (`plate: ""`, `capacity: 0`, `created_at: 1970-01-01`). `pings` and
`job_events` stay at the default deliberately: append-only, so no before-image exists to
describe, and `pings` is where the extra WAL would cost something.

**A batch size must bound the statement, not the unit of work.** `poll_once` drains each table
rather than stopping at `LIMIT`; otherwise "everything since the watermark" silently becomes "up
to 1000 rows", the poller is permanently catching up, and every poll still reports success.

**Write down what broke before fixing it.** The comments recording past failures — the Windows
port collision that presented as an authentication failure, the batch that stamped the previous
flush's timestamp and backdated 169,480 rows, the sequence counters that collided on the second
run — are worth more than the code around them. Add to them.

**A Type 2 dimension can be built from after-images only.** Each version's `valid_from` is its own
`source_ts_ms` and its `valid_to` is simply the next version's `valid_from`, both obtainable with a
`lead()` window function over ordinary create/update/snapshot rows — no version needs to read a
before-image body to know when it started or ended. Only a delete needs the before-image at all,
and it contributes nothing but its own commit timestamp, to close out the version it ends. That
narrow contract is what keeps `known-issues.md` §1's fabricated before-image defaults (`plate: ""`,
`capacity: 0`, `created_at: 1970-01-01`) out of Gold entirely — the dimension never asks the
question whose answer would be invented. Its necessary companion: a delete must still break the
suppression chain rather than being silently dropped alongside genuine no-op updates, because ids
**are** reused here (`known-issues.md` §3 measures the simulator's fixed test ids colliding across
runs) — ten resurrections across two vehicle ids on this volume, and without that rule five of six
lifecycles vanish silently, collapsed into whichever one is still standing.

**Ground truth is typed where Bronze is text, and the inversion is the point.** Bronze is all
`text` because a cast inside `GENERATED ALWAYS` runs on INSERT, so one device sending `"banana"`
would take a whole batch down — Bronze rejecting the malformed evidence it exists to keep.
`truth.intended_pings` is written by our own code three lines before the insert, so a failing cast
there is a bug we want loudly and immediately. Never "align" the two: doing so moves every such
failure out of the simulator, where it is a bug report, and into the diff, where it is an
unexplained missing row. `src/fleet_telemetry/truth.py` is deliberately not in `load/`, which
`load/schema.py` scopes to bronze.

**The truth diff is bounded by a per-run arrival frontier, and the obvious bound is quietly wrong.**
Truth is always ahead of Silver by one pipeline traversal, so an unbounded diff always fails on
in-flight data. But `max(stg_pings.device_ts)` is worse than useless: it spans all of Silver
including pre-truth history, and a backfill run generates `device_ts` in the *past*
(`start = end - span`), so the bound can sit beyond the run's own window and admit rows that have
not arrived. `--reset` cannot rescue it — Bronze is append-only. The frontier is therefore derived
per `run_id` from rows that actually matched, with a strict `<` because every ping in a tick shares
one `device_ts` and a 500-ping flush lands mid-tick.

**The insert into `truth.intended_pings` binds parameters by name, not position.** `latitude` and
`longitude` are adjacent columns of the same type, so reordering a positional tuple would exchange
them with no error, no type mismatch and no failing cast — the same silent break that moved fleet
total distance by +2.14% with nine of nine scripts reporting ok. `_COLUMNS` is pinned to
`IntendedPing._fields` by a hermetic test.

## Naming Conventions

- **Python**: `snake_case` throughout; modules are singular nouns (`config.py`, `world.py`).
  Every module starts with a docstring explaining *why*, not what. `from __future__ import
  annotations` at the top of each file. Type hints on signatures; `dataclass(frozen=True)` for
  config shapes.
- **Private helpers** are `_`-prefixed (`_get`, `_patch`, `_database`, `_rows`, `_one`).
- **SQL in Python** is lowercase in inline statements (`insert into pings ...`); DDL in
  `docker/*/init.sql` is uppercase keywords. Follow whichever file you are in.
- **Warehouse layers** map to schemas: `bronze` → `silver` → `gold` → `marts`. dbt directories are
  `staging/` (→ `silver`), `gold/`, `marts/`.
- **dbt models**: `stg_*` in staging, `dim_*`/`fact_*` in gold. Bronze tables are `raw_*`.
- **Env vars**: `{PREFIX}_{FIELD}` — `OLTP_HOST`, `WAREHOUSE_PORT`, `FLEET_API_TOKEN`. Note
  `database` maps to `_DB`, not `_DATABASE` (see `env_var_for`).
- **Docs**: `docs/learn/NN-topic.md` for curriculum, `docs/superpowers/specs/YYYY-MM-DD-name.md`
  for specs.
- **Branches**: one per phase or slice — `phase-2/batch-poller`. Branch from `main`; keep `main`
  green, it is what a newcomer clones.

## Environment & CI

- **Platform**: Windows 11, PowerShell primary (Bash also available). Python 3.13 required.
  Docker Desktop.
- **Ports are deliberately odd**: warehouse `55432`, oltp `55433`. 5432 is held by a locally
  installed PostgreSQL, and Windows lets a container bind a port a host service already holds —
  both listen, clients silently reach the *host* server, and the symptom is "password
  authentication failed", which reads as a credentials bug. That cost a day. Container-side stays
  5432.
- **Use `127.0.0.1`, not `localhost`**. Docker Desktop on Windows publishes on both stacks but
  does not forward IPv6; `localhost` resolves to `::1` first and dbt does not fall back to IPv4,
  failing with "could not receive data from server".
- **Tooling versions**: keep `ruff` in step with the `ruff-pre-commit` rev in
  `.pre-commit-config.yaml`, or the hook and a local `ruff check` will disagree about which rules
  exist. Airflow is pinned `>=3.0,<4.0` — the 2.x → 3.x task API differs.
- **CI** (`.github/workflows/ci.yml`) runs three jobs on push to `main` and every PR:
  - `lint` — `ruff check .` and `ruff format --check .`
  - `test` — `pip install -e ".[dev]"` then `pytest`
  - `dbt` — `dbt parse` then `dbt build --target ci` against a `postgis/postgis:16-3.4` service
    container on port 5432, with the four medallion schemas created first
- **CI has no credentials and must stay that way.** Every test runs offline; the suite asserts
  this by passing explicit env mappings rather than reading the process environment.
- **TimescaleDB is absent in CI** (PostGIS image only), so hypertable creation must be guarded or
  `dbt build` will fail there while passing locally.
- `wal_level=logical` is already set on the oltp container, in phase 1, because changing it
  requires a database restart. An unconsumed replication slot makes Postgres retain WAL forever
  and eventually fills the disk.

## Data & Analysis

- **Datasets of record**: the `oltp` database is the source of truth for phase 1; `bronze.*` in
  the warehouse becomes the durable record from phase 2 onward. Neither is in git.
- **Analysis output**: `docs/source-system-reference.md` holds the profiler's findings — every
  number in it must be produced by a query, not estimated. Regenerate with
  `python -m fleet_telemetry.profile_source` rather than hand-editing numbers.
- **Runtime data stays out of git**: `data/bronze/`, dbt `target/`/`logs/`, and Airflow artefacts
  are already in `.gitignore`. Never commit `.env`; `.env.example` documents what exists.
- **Profiler findings are checked against ground truth.** The simulator's own counts are the
  reference — a profile that disagrees means the measurement is wrong. Two real simulator bugs were
  found this way, which is why the profiler is the phase 1 deliverable rather than an afterthought.
