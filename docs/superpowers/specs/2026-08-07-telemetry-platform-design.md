# Fleet Telemetry Platform — Design

Date: 2026-08-07
Status: draft, pending approval
Supersedes: `2026-08-03-platform-design.md` (aviation conflict analytics), abandoned — see §2.

---

## 1. Purpose

Learn data engineering end to end by building both halves of a pipeline: the operational system
that produces events, and the analytics platform that consumes them.

The domain is vehicle telemetry for informal transport in The Gambia — vehicles, jobs, and GPS
pings. The domain is **grounding, not a product**. Nobody will use the application. It exists to
emit events with realistic structure, and it should be built to the minimum standard that achieves
that.

The learning target is the pipeline. Every decision below is judged on what it teaches, not on
whether it would ship.

## 2. Why the previous project was abandoned

Recorded because the failure drove this design.

The aviation conflict project died of **data supply**, four times over:

| Attempt | Outcome |
|---|---|
| ACLED conflict events | Account authenticates; every `/api/*/read` returns 403. Entitlement, not a request defect |
| OpenSky over conflict zones | Donbas, Yemen, Sudan: **0 aircraft**. ADI is `0/0` — the effect saturated before observation began |
| OpenSky over The Gambia | **0 aircraft** over the country; 2 across all West Africa (2.8M km²) vs Switzerland's 1,856/Mkm². Not absent traffic — absent *receivers* |
| Global Fishing Watch | Would have required another token and another wait |

One root cause: **no control over the data supply.** Someone else's coverage decisions and
entitlement reviews set the ceiling, and changing the question just re-rolled the same dice.

Owning the producer removes that class of failure permanently. It also unlocks four things no
public API can teach: change data capture, authentic Slowly Changing Dimensions, controlled
volume, and **deliberately injected pathology**.

## 3. The central problem

One hard problem, which everything else serves:

> **Events arrive late, out of order, and in bursts. The warehouse must still be correct.**

Chosen for three reasons:

1. It is the hardest common problem in production data engineering — event time vs processing
   time, watermarks, lateness bounds, and restating aggregates after the fact.
2. It is **native to the domain**. Patchy connectivity means a reconnecting device dumps an hour
   of backlog at once, so a stale ping arriving after a fresher one is the normal case, not an
   edge case. The realism is free.
3. **It is verifiable.** The simulator knows the truth it generated, so pipeline output can be
   checked against ground truth.

Point 3 is the direct correction of the previous project's fatal flaw: `baseline_flight_count`
estimated a counterfactual nothing could verify, so a wrong answer would have looked clean and
plausible. Here, wrong answers are detectable.

## 4. Scope decision

**The application is scaffolding. The simulator does the work.**

The standard failure mode of "build the producer too" is that the producer eats the project —
six weeks of auth flows and CRUD screens, no pipeline. Guardrails:

- No user accounts, no sessions, no UI. A static bearer token is sufficient authentication.
- The API surface is roughly six endpoints. If it grows past ten, something has gone wrong.
- **No real users.** Waiting on adoption would recreate exactly the external dependency §2 exists
  to escape.
- Load comes from the simulator, which is reproducible, controllable and instant.

Corollary worth stating plainly: **simulated data is clean unless deliberately made dirty.** If
the simulator only ever emits well-formed events, Phase 4 has nothing to work on and this becomes
a CRUD app with a warehouse attached. Pathology injection is therefore a first-class feature with
its own tests (§7), not a garnish added at the end.

## 5. Stack

| Layer | Choice | Rationale |
|---|---|---|
| Source app | FastAPI + Python 3.13 | Same language as everything else; minimal ceremony |
| **OLTP database** | Postgres 16, **separate container** | See below |
| CDC | Debezium | The standard. Requires `wal_level=logical` on the OLTP instance |
| Broker | **Redpanda** (Kafka API) | Single container, no ZooKeeper/KRaft setup. Wire-compatible, so all code and tooling is Kafka's — swappable for real Kafka with no application change |
| Warehouse | Postgres 16 + PostGIS + TimescaleDB | Already built and working |
| Transform | dbt (`dbt-postgres`) | Already scaffolded |
| Orchestration | Apache Airflow 3 | Already planned |
| Simulator | Python | Reproducible; seeded RNG |
| Quality | pytest, ruff, pre-commit, GitHub Actions | Already configured |

**The OLTP database must not be the warehouse.** Same reasoning that separates Airflow's metadata
database in the existing design: an application bug must not be able to corrupt analytics, the two
have different backup and uptime needs, and pointing analytics at the operational database is
precisely the anti-pattern the warehouse exists to eliminate. Separate container, separate volume,
separate credentials.

Division of labour is unchanged: **EL in Python, T in SQL, orchestration separate.**

## 6. The source system

### Tables

| Table | Mutability | Teaches |
|---|---|---|
| `drivers` | mutable | SCD Type 2 |
| `vehicles` | mutable | SCD Type 2; reassignment between drivers |
| `depots` | slowly changing | Spatial dimension |
| `jobs` | status state machine | Event sourcing; late-arriving state |
| `job_events` | append-only | Immutable transition log |
| `pings` | append-only, high volume | Throughput, partitioning, lateness |

### Three timestamps, and why each exists

This is the most load-bearing detail in the design. Every ping carries:

| Column | Source | Trustworthy? |
|---|---|---|
| `device_ts` | Device clock, when the reading was taken | **No** — device clocks drift and are sometimes badly wrong |
| `server_ts` | API receipt time | Yes |
| `_ingested_at` | When Bronze wrote the row | Yes |

`server_ts − device_ts` is the observed lateness, and it is what every watermark decision keys
off. But it conflates two different things — genuine transmission delay and **device clock skew** —
and they need different treatment. A device three hours fast is not producing events from the
future; it is misreporting event time, and using `device_ts` raw would place its trips in the wrong
hour. Estimating per-device skew and correcting for it is a Phase 4 problem, and one of the more
valuable things here.

### Ping identity and sequence numbers

Each device generates, per ping:

- `ping_id` — client-generated UUID. The idempotency key. Retries resend the same `ping_id`, so
  deduplication is exact rather than heuristic.
- `sequence_no` — monotonic per device.

The sequence number is not redundant with the timestamp, and it earns its place:

> **A gap in the sequence proves data loss. A flat position with no gap proves the vehicle stopped.**

That single distinction is the reliable, non-heuristic answer to "no data or no activity?" — the
question that produced `receiver_coverage` in the previous design and then killed the domain
outright. Here it is answerable from the data itself rather than inferred.

### API surface

Six endpoints, roughly: create job, update job status, assign vehicle, post ping batch, plus two
reads for the simulator to verify against. Batch ping submission is deliberate — it is how real
devices behave when reconnecting, and it is what produces burst arrival.

## 7. The simulator

Drives the API at volume. Seeded RNG, so any run is reproducible from its seed — which is what
makes pipeline tests deterministic.

### Volume target

100 vehicles × 1 ping / 5 s × 12 h/day ≈ **864,000 pings/day**; 30 simulated days ≈ **26M rows**.

Enough that partitioning, index choice and query plans stop being theoretical. TimescaleDB has to
actually earn its hypertable.

### Pathologies

Each is an independently toggleable flag, so a test can enable one in isolation and assert the
pipeline survives it.

| Pathology | Mechanism | What it breaks if unhandled |
|---|---|---|
| Reconnect burst | Device buffers offline, submits an hour at once | Out-of-order arrival; aggregates for closed windows change |
| Clock skew | Per-device constant offset, occasionally large | Events land in the wrong hour, or in the future |
| Retry storm | Same `ping_id` submitted 2–5 times | Double-counted rows; inflated utilisation |
| Silent vehicle | Device stops without a status change | "No data" read as "no activity" |
| Sequence gap | Pings dropped in transit | Undetected loss; silent undercount |
| Schema drift | A new optional field appears mid-run | Rigid parsers fail; Bronze must tolerate it |
| Out-of-order status | Job marked delivered before picked up | State machine assumptions violated |

Ground truth is written alongside — the simulator logs what it *intended* to emit, so pipeline
output can be diffed against reality. This is the verification the previous project could not have.

## 8. Data flow

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

Bronze stays load-bearing for the same reason as before: **an unconsumed event is gone.** A Kafka
topic has a retention window, not a memory.

### Layer boundaries

Unchanged from the previous design, because the rule was sound: **Silver fixes what nobody can
reasonably disagree about; Gold holds every judgment call.**

| Layer | Owns |
|---|---|
| Bronze | Verbatim change events, including malformed ones. Never edited |
| Silver | Deduplication on `ping_id`, typing, unit normalisation, geometry construction |
| Gold | Clock-skew correction, lateness policy, trip reconstruction, utilisation rules |
| Marts | Analysis-ready aggregates |

Clock-skew correction sits in **Gold**, not Silver, and the placement is deliberate: it is an
estimate with a method that will be revised, and revising it must not require reprocessing 26M raw
rows.

### SCD Type 2 from CDC, not from snapshots

dbt's `snapshot` materialisation polls a source table on a schedule and therefore **misses any
change that happens between two runs** — if a driver moves depot and moves back within an hour,
a snapshot sees nothing. CDC captures every committed change, so building the Type 2 dimensions
from the change stream is strictly more accurate.

Worth building both once and diffing them. The discrepancy is the lesson.

## 9. Handling lateness

The core of Phase 4, and where most of the learning is.

**Lateness bound.** Accept events up to a declared horizon late (starting default: 6 hours).
Beyond it, route to a quarantine table rather than dropping — silent discards are how undercounts
become invisible.

**Incremental models with lookback.** dbt incremental models rebuild a trailing window rather than
only new rows, so late arrivals within the bound are picked up:

```sql
where event_date >= (select max(event_date) - interval '3 days' from {{ this }})
```

The lookback must exceed the lateness bound, or late data lands outside the rebuild window and is
silently lost. This is a foot-gun worth documenting next to the config.

**Restatement must be visible.** When late data changes a previously published aggregate, the
change is recorded, not silently applied. A mart that quietly revises last Tuesday's number is
indistinguishable from one that is wrong.

**A mart about the pipeline itself.** `mart_pipeline_health` — lateness percentiles, duplicate
rate, sequence-gap rate, quarantine volume, restatement count. Observability as a first-class
output rather than a dashboard bolted on later.

## 10. Marts

| Mart | Grain | Question |
|---|---|---|
| `mart_vehicle_utilisation` | (vehicle, day) | Active hours vs available hours |
| `mart_job_performance` | (job) | Actual vs estimated duration; on-time rate |
| `mart_corridor_flow` | (corridor, hour) | Spatial demand — exercises PostGIS |
| `mart_pipeline_health` | (day) | Lateness, duplicates, gaps, restatements |

## 11. Testing

Carried over wholesale, because it was the strongest part of the previous design.

| Scope | Approach |
|---|---|
| Python | pytest — API handlers, simulator determinism, skew estimation |
| dbt generic | `not_null`, `unique`, `relationships`, `accepted_values` |
| **Grain assertions** | One singular test per fact and mart, unique on its declared grain |
| **Ground-truth diff** | Simulator's intended output vs the mart. The verification the last project lacked |
| **Pathology tests** | One test per §7 row: enable it alone, assert correctness holds |
| CI | ruff, pytest, `dbt build` against a throwaway Postgres service |

Grain assertions stay `severity: error`. Fan-out silently inflates every downstream number and is
invisible without an explicit uniqueness test.

## 12. Build phases

Vertical slices. Something works end to end at Phase 2.

| Phase | Deliverable | Blocked by |
|---|---|---|
| **0** | FastAPI app, OLTP schema, simulator emitting clean events | — |
| 1 | Redpanda + Debezium; raw CDC landing in `bronze.*` | 0 |
| 2 | dbt Silver for pings + grain tests. **First end-to-end slice** | 1 |
| 3 | SCD Type 2 dimensions from the change stream | 2 |
| 4 | **Lateness**: pathology injection, watermarks, skew correction, restatement | 3 |
| 5 | Gold facts and marts, including `mart_pipeline_health` | 4 |
| 6 | Airflow orchestration; CI green | 5 |

Phases 0–3 are getting to the start line. **Phase 4 is the project.** If time runs short, cut
scope from 5 and 6, never from 4.

## 13. Migration from the aviation project

| Asset | Action |
|---|---|
| `docker/` warehouse, PostGIS, Timescale | **Keep** — unchanged |
| `dbt/` project, medallion schemas, grain-test discipline | **Keep** — repoint models |
| `src/aviation_conflict/config.py` | **Keep** — rename package, same resolution logic |
| CI, ruff, pre-commit, `.gitignore` | **Keep** |
| `docs/opensky-api-reference.md` | **Archive** to `docs/archive/` — good work, superseded |
| `2026-08-03-platform-design.md` | **Archive** — same |
| `scripts/explore_*.py`, `data/samples/` | **Delete** |
| Repository name | **Rename** — `aviation-conflict-analytics` will mislead every future reader, including us |

Most of the engineering survives. What is discarded is the domain, not the platform — which is
some evidence the layering was right.

## 14. Open items

| Item | Blocks |
|---|---|
| Confirm Debezium + Postgres 16 logical replication config (`wal_level`, publications, slots) | Phase 1 |
| Choose the lateness bound and lookback window; document the relationship between them | Phase 4 |
| Decide the clock-skew estimator (per-device median offset is the obvious start) | Phase 4 |
| Pick the new repository name | — |
| Confirm 26M rows is comfortable on the target machine; reduce vehicle count if not | Phase 0 |
