# fleet-telemetry-platform

An end-to-end data platform, built to learn data engineering by owning **both** halves of the
pipeline: the application that produces events, and the warehouse that consumes them.

The domain is vehicle telemetry for informal transport in The Gambia — vehicles, jobs, GPS
pings. The domain is **grounding, not a product.** Nobody will use the application; it exists
to emit realistically-shaped events.

Current status: **phase 1 of 5 complete.** The source system is built and running — application,
simulator and OLTP database all come up with one command and produce data continuously. The
warehouse stands up but is empty: nothing connects the two yet, which is phase 2's job.

## The one hard problem

> **Events arrive late, out of order, and in bursts. The warehouse must still be correct.**

Everything else serves that. It is the hardest common problem in production data engineering
— event time vs processing time, watermarks, lateness bounds, restating aggregates after the
fact — and it is native to the domain rather than bolted on: patchy connectivity means a
reconnecting device dumps an hour of backlog at once, so a stale ping arriving after a fresher
one is the normal case.

Crucially, **it is verifiable.** The simulator knows the truth it generated, so pipeline
output can be checked against ground truth rather than merely looking plausible.

## Read these first

| Path | What it is |
| --- | --- |
| [docs/learn/](docs/learn/README.md) | **Start here.** The curriculum: five phases, how to work through them, and the vocabulary |
| [docs/superpowers/specs/2026-08-07-telemetry-platform-design.md](docs/superpowers/specs/2026-08-07-telemetry-platform-design.md) | The design behind it. Stack, source schema, pathology catalogue, lateness policy |
| [docs/archive/](docs/archive/) | The abandoned aviation project and why it failed. Not current |

This is a **beginner learning project**, and the bar is comprehension rather than completion: a
working pipeline nobody can explain is a failed outcome here.

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

Two ingestion paths, deliberately — high-volume append-only pings teach throughput and
partitioning; CDC on mutable entities teaches Slowly Changing Dimensions authentically.

Division of labour: **Extract/Load in Python, Transform in SQL, orchestration separate.**
Python only moves bytes; every business rule is a dbt model, so it stays diffable and
testable.

## Layout

```text
docker/            compose stack + the image the app and simulator share
  oltp/init.sql    the source schema -- read this first, it is heavily commented
app/               the fleet application (FastAPI)
simulator/
  world.py         pure physics: where is a vehicle at time T
  run.py           the clock: backfill a window, or --forever at real speed
src/fleet_telemetry/
  config.py        settings resolution (env -> .env -> built-in default)
  profile_source.py  phase 1: measure the source system
  ingest/
    poller.py      phase 2 step 1: the naive batch poller, built to lose
    connector.py   registers Debezium over the Connect REST API
    envelope.py    topic -> bronze table, and a decoder that never drops a message
    consumer.py    the CDC loop; offsets commit only after the database does
    compare.py     what each ingestion path captured -- the phase 2 deliverable
  load/
    schema.py      all bronze DDL, owned here rather than by dbt
    writer.py      the only thing that writes bronze.*
dbt/               Silver, Gold, marts -- all business logic lives here
dags/              Airflow DAGs (phase 4)
tests/
```

## Setup

There are **no credentials to obtain.** This project calls no external APIs — every setting has
a built-in default matching the compose file, so a fresh clone runs with no `.env`.

You need Docker and Python 3.13.

```bash
pip install -e ".[app,warehouse,dev]"   # or: uv sync --extra app --extra warehouse --extra dev
pre-commit install                   # ruff, secret scanning; without this the hooks never run
docker compose -f docker/docker-compose.yml up -d
python -m fleet_telemetry.config     # shows what resolved, and what fell back to defaults
pytest
```

If all five succeed you have a working environment. Then start with
[docs/learn/](docs/learn/README.md).

**Extras arrive with the phase that needs them.** `[warehouse]` (dbt, Kafka client) landed in
phase 2 and is in the install above; `[airflow]` arrives in phase 4. `[dev]` alone is not enough
to run the tests — the suite imports both the application and the loader, so `[app]` and
`[warehouse]` are required too.

Six containers come up:

| Service | What it is | Port |
| --- | --- | --- |
| `oltp` | the source system | 55433 |
| `warehouse` | PostGIS + TimescaleDB; `bronze.*` from phase 2 | 55432 |
| `redpanda` | the broker, Kafka wire protocol | 19092 |
| `connect` | Kafka Connect running Debezium | 8083 |
| `api` | the fleet application | 8000 |
| `simulator` | stands in for real vehicles; runs continuously | — |

The database ports are deliberately odd — see the comments in
[docker-compose.yml](docker/docker-compose.yml) for the Windows port-collision reason.

## The world runs on its own

`up -d` starts the simulator in `--forever` mode and it does not stop: 10 vehicles, one ping
each per 5 seconds, paced against the real clock. Nothing schedules it, and phase 4's Airflow
never will — it *represents the world*, and minibuses keep driving whether or not your pipeline
is up. Airflow will orchestrate the things that consume this data.

```bash
docker compose -f docker/docker-compose.yml logs -f simulator     # watch it
docker compose -f docker/docker-compose.yml stop simulator        # pause the world
docker compose -f docker/docker-compose.yml start simulator       # resume
```

## Running the ingestion (phase 2)

Two paths into `bronze.*`, deliberately. The poller is the naive version and is meant to lose;
the diff between what each captured is the phase 2 deliverable —
see [docs/learn/02-ingestion.md](docs/learn/02-ingestion.md).

```bash
python -m fleet_telemetry.load.schema             # create bronze.* (idempotent)
python -m fleet_telemetry.ingest.connector --register   # register Debezium

python -m fleet_telemetry.ingest.consumer         # CDC -> bronze.raw_*, until interrupted
python -m fleet_telemetry.ingest.poller           # poller -> bronze.poll_rows
python -m fleet_telemetry.ingest.compare          # what each one captured
```

The consumer and the poller both create the bronze schema on startup, so the first command is
only needed if you want the tables before either runs.

**Watch the replication slot.** An unconsumed slot makes Postgres retain WAL forever and
eventually fills the disk — a real and popular way to take an instance down:

```bash
psql "postgresql://fleet:fleet@127.0.0.1:55433/fleet" -c \
  "select slot_name, active, wal_status,
          pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as retained
     from pg_replication_slots"
```

Deleting the connector does **not** drop its slot, on purpose, so a connector can be recreated
and resume. If you are finished with it, drop it deliberately:

```bash
python -m fleet_telemetry.ingest.connector --delete
psql "postgresql://fleet:fleet@127.0.0.1:55433/fleet" \
  -c "select pg_drop_replication_slot('fleet_debezium')"
```

That default writes **~173,000 rows/day, roughly 43 MB with indexes** — small enough to leave
running. The design's full fleet is 40 vehicles, four times that and over a gigabyte a week, so
it is opt-in: set `SIM_VEHICLES=40` in `.env` when throughput is the thing you want to test.

Two things to know before you change these. Only the first N active vehicles by id report, so
the rest simply stop producing rows — in the data that is indistinguishable from a device that
failed. And changing `SIM_INTERVAL` makes new data incomparable with the phase 1 baseline, which
was measured at 5 s.

Want the databases only, and to run the app by hand?

```bash
docker compose -f docker/docker-compose.yml up -d oltp warehouse
uvicorn app.main:app --port 8000
```

### Generating history instead

`--forever` produces data at real speed, so a month of history takes a month. To generate a
window in a few minutes, the API has to accept a client-supplied `server_ts` — a privileged
backfill path that is **off by default**, because `server_ts` is the only timestamp in the
system no client can influence and that is precisely why it can be trusted.

```bash
docker compose -f docker/docker-compose.yml stop simulator
FLEET_ALLOW_SERVER_TS_OVERRIDE=true uvicorn app.main:app --port 8000   # separate terminal
python -m simulator --hours 6 --vehicles 40
```

The two modes are otherwise identical — same world, same job lifecycle, same entity mutations —
so their output is comparable. The one difference shows up in the data: backfilled rows carry a
synthetic 1–8 s transmission delay, while live rows carry whatever really happened (a few
milliseconds locally).

## Working on this together

Phases are **sequential** — you cannot transform data you have not yet ingested — so two people
cannot simply take a phase each. Two splits that do work:

- **Split within a phase.** Phase 2 is a batch poller and a CDC pipeline, built separately and
  then compared against the same run. Genuinely parallel, and the comparison is the lesson.
- **One builds, one explains.** The second person writes the `docs/learn/` guide and the tests.
  This is not the lesser job: writing the explanation is how you discover what you did not
  actually understand, so **alternate it every phase** rather than letting one person own it.

The explain-back questions at the end of each guide work far better asked by another person than
read off a page. Answer them out loud to each other before calling a phase done.

Whoever is not driving should still run the code. Reading a pipeline teaches you much less than
watching it break.

### Mechanics

Branch from `main`, one branch per phase or slice: `phase-2/batch-poller`. Keep `main` green — it
is what a newcomer clones, and it was wrong for two weeks before anyone noticed.

Before pushing: `ruff check . && pytest`. The pre-commit hooks cover the same ground, which is
why `pre-commit install` is in the setup list rather than optional — it was configured but never
installed for the first two weeks, so none of the checks ran for anybody.

## Configuration

Resolution order is **environment variable → `.env` → built-in default**. Override anything
via `.env` (git-ignored); see `.env.example` for what exists and why.

Defaults are a deliberate change from this project's predecessor, which returned `None` for a
missing value. There is no sensible default for someone else's API secret, but there is one
for local Postgres. The trade-off is that **a defaulted password is fine locally and dangerous
anywhere else**, so every value records whether it was set or fell back, and
`config.describe()` prints the distinction.

Secrets are never printed — passwords render as presence and length, DSNs render redacted,
and two tests assert no secret reaches `describe()` or stdout.

## Why Bronze is not optional here

A Kafka topic has a retention window, not a memory. Once an event ages out it is gone, and no
amount of reprocessing brings it back — Bronze is the durable record everything downstream is
rebuilt from.

The same reasoning killed the predecessor project and is worth carrying forward: if you do not
own a copy of the raw event, your pipeline's ceiling is set by someone else.

## Build phases

Five phases, following the stages every pipeline has. Each one starts with the obvious
hand-rolled version, lets its limits bite, and only then introduces the real tool — because a
tool introduced before its problem is a tool you cannot explain.

| Phase | Question it answers | Naive first | Then |
| --- | --- | --- | --- |
| 1 | What data exists, what shape, how often? | — | measure it |
| 2 | How do I get it out without loss or duplication? | batch poller | Debezium CDC |
| 3 | How do I make raw records trustworthy? | SQL scripts | dbt |
| 4 | How do I make it run without me? | shell script + timer | Airflow |
| 5 | How does anyone actually use it? | — | marts + a consuming surface |

**Phase 3 is the project** — it holds the lateness problem. If time runs short, cut Phase 5, then
Phase 4; never Phase 3.

Each phase ends with explain-back questions. If you cannot answer them from memory, the phase is
not finished, however well the code runs.
