# fleet-telemetry-platform

An end-to-end data platform, built to learn data engineering by owning **both** halves of the
pipeline: the application that produces events, and the warehouse that consumes them.

The domain is vehicle telemetry for informal transport in The Gambia — vehicles, jobs, GPS
pings. The domain is **grounding, not a product.** Nobody will use the application; it exists
to emit realistically-shaped events.

Current status: **scaffolding.** Warehouse and dbt project stand up; the application,
simulator and CDC pipeline are not built yet. Start with the design spec.

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
docker/            warehouse: postgres + postgis + timescaledb
src/fleet_telemetry/
  config.py        settings resolution (env -> .env -> built-in default)
  ingest/          Bronze: Kafka CDC -> raw events
  load/            Bronze -> bronze.* tables
dbt/               Silver, Gold, marts -- all business logic lives here
dags/              Airflow DAGs (phase 6)
tests/
```

## Setup

There are **no credentials to obtain.** This project calls no external APIs — every setting has
a built-in default matching the compose file, so a fresh clone runs with no `.env`.

You need Docker and Python 3.13.

```bash
pip install -e ".[app,dev]"          # or: uv sync --extra app --extra dev
pre-commit install                   # ruff, secret scanning; without this the hooks never run
docker compose -f docker/docker-compose.yml up -d
python -m fleet_telemetry.config     # shows what resolved, and what fell back to defaults
pytest
```

If all five succeed you have a working environment. Then start with
[docs/learn/](docs/learn/README.md).

**Extras arrive with the phase that needs them**, so the install above is deliberately not
everything: `[warehouse]` (dbt, Kafka client) lands in phase 2, `[airflow]` in phase 4. `[dev]`
alone is not enough to run the tests — the suite imports the application, so `[app]` is required
too.

Two containers come up: `oltp` (the source system, port 55433) and `warehouse`
(PostGIS + TimescaleDB, port 55432). The ports are deliberately odd — see the comments in
[docker-compose.yml](docker/docker-compose.yml) for the Windows port-collision reason.

## Working on this together

Phases are **sequential** — you cannot transform data you have not yet ingested — so two people
cannot simply take a phase each. What works:

- Split *within* a phase. Phase 2, for example, is a batch poller and a CDC pipeline that are
  built separately and then compared.
- One builds, the other writes the `docs/learn/` guide and the tests for it. The guide is not
  documentation-after-the-fact here; explaining the thing is how you find out whether you
  understood it.

Branch from `main`, one branch per phase or per slice: `phase-2/batch-poller`. Keep `main` green
— it is what a newcomer clones.

Before pushing: `ruff check . && pytest`. The pre-commit hooks cover the same ground, which is
why `pre-commit install` is in the setup list rather than optional.

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
