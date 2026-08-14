# Study map

What to read to understand this project, and how to check that you do.

Not a phase guide — those teach the project. This points **outward**, to the books and primary docs
behind it, and **inward**, to the place in this repo where each idea has already been measured.

**The ordering is deliberate and it is not the order of the concepts.** It is the order in which
reading pays off, given what is already built and what comes next.

---

## The four ideas everything else hangs off

Most people meet these as definitions and hit the failures years later. Here they arrive with
evidence attached, which is the only real advantage this project has.

| Idea | Already measured here |
| --- | --- |
| **Event time ≠ processing time** | `device_ts` vs `server_ts` — p50 34 s / p99 67 s in backfill, 0.003 s live, and 10 rows of *negative* lateness ([source-system-reference §3](../source-system-reference.md), [silver-by-hand §3](../silver-by-hand.md)) |
| **Grain** — what exactly one row represents | `count(*) = count(distinct ping_id)` asserted on seven tables in `tests/test_transform.py`; fan-out is what breaks it |
| **Idempotency** — the same input twice must not change the answer | `ON CONFLICT (ping_id)` at the app, `(_kafka_partition, _kafka_offset)` at Bronze, two identical checksums in Silver |
| **Reading state ≠ reading changes** | `deletes_seen_by_poller: 0` — structurally, at any polling frequency, forever ([02-ingestion](02-ingestion.md), the diff) |

Internalise those four and phases 4–5 are variations on them.

---

## Concept → evidence here → what to read

### Streaming and time

**Study this first.** Phase 3 is the project, and this is what phase 3 is made of.

| Concept | Evidence in this repo | Read |
| --- | --- | --- |
| Watermarks, and why they leak | `next_watermark` in `ingest/poller.py`, and vehicle 3 becoming permanently invisible ([known-issues §4](../known-issues.md)) | **Streaming Systems** (Akidau, Chernyak, Lax) ch 1–3 — or the free "Streaming 101 / 102" articles by the same author, which are the same material |
| Late, out-of-order, bursty arrival | `job_events` where a delivery precedes its own pickup; `write_lag_seconds` in `30_stg_job_events.sql` | Streaming Systems ch 2 |
| Lateness bounds, quarantine, restatement | `lateness_bound_hours: 6` and `incremental_lookback_days: 3` in `dbt_project.yml`, and why one MUST exceed the other | Streaming Systems ch 2, then dbt's incremental-models docs |
| Monotonic vs wall clock | the container clock stepping back ~2.7 s every ~30 s — it broke simulator pacing, leaked a watermark, and produced negative lateness | any OS reference on `CLOCK_MONOTONIC`; it is a short read and it pays for itself three times over in this repo alone |

### Change data capture

| Concept | Evidence in this repo | Read |
| --- | --- | --- |
| WAL, logical decoding, replication slots, publications | `wal_level=logical` set in phase 1 because it needs a restart; slot `fleet_debezium`; `plugin.name=pgoutput` | PostgreSQL docs — *Logical Replication* and *Write-Ahead Logging*. Primary source, and genuinely readable |
| `REPLICA IDENTITY`, and fabricated before-images | before-images arriving as type defaults: `plate: ""`, `capacity: 0`, `created_at: 1970-01-01` ([known-issues §1](../known-issues.md)) | Debezium Postgres connector docs, the before-image section |
| At-least-once delivery, dedup keys | `enable.auto.commit: False`, and the unique index that absorbs a redelivery | **Designing Data-Intensive Applications** (Kleppmann) ch 11 |
| Transaction boundaries decide the *direction* of failure | poll → insert → **database commit** → offset commit, in `consumer.py`. Reversing the last two turns a crash into silent loss | **Kleppmann ch 7.** The single most valuable chapter for this project |

### Warehouse modelling

Nothing here is built yet — `dbt/models/gold/` is empty. Read only as far as #13 needs.

| Concept | Status | Read |
| --- | --- | --- |
| Dimensional modelling, facts and dimensions | not built | **The Data Warehouse Toolkit** (Kimball & Ross) ch 1–3. Skim; do not read cover to cover |
| SCD Type 2 | the change stream that makes it possible exists in `bronze.raw_cdc_entities` | Kimball ch 5, then dbt's snapshots docs — to see what this project deliberately does *not* do, and why CDC beats polled snapshots |
| Right-censoring / survivorship | 37 of 143 jobs still open at the phase 1 boundary; noted in `23_stg_jobs.sql` | one good article on survival analysis is enough |

### Data quality and engineering practice

| Concept | Evidence in this repo |
| --- | --- |
| Reject the impossible, record the merely suspicious | `latitude: 900` → 422; a device clock three hours fast → accepted and stored |
| Keep the evidence, including evidence of your own failure | `envelope.decode` never raises and never returns nothing; `parse_error` and `raw_payload` columns; `silver_manual.rejected_rows` |
| A cast that can reject a row belongs downstream of the durable record | every scalar in `bronze.*` is `text`, including `sequence_no` |
| Hermetic vs integration tests, and test isolation | `-m "not integration"`; and three separate failures caused by tests depending on state they do not own ([known-issues §3, §3b, §3c](../known-issues.md)) |
| Naive-first | `poller.py` and `sql/silver/` both exist to be superseded, and both are kept |

---

## Tools, in the order they pay off

1. **SQL, deeply.** Not a tool so much as *the* tool. This project already uses window functions
   (`lag`), `DISTINCT ON`, `percentile_cont`, `jsonb` operators, generated columns and CTEs. Learn
   `PARTITION BY` and window frames properly — Gold is almost entirely window functions. Postgres
   docs on window functions, then write some.
2. **dbt.** The next issue. Read *sources → refs → tests → materializations → incremental models*,
   in that order. It will take an afternoon, because `sql/silver/` is the hand-built version of
   exactly what it does and [silver-by-hand.md](../silver-by-hand.md) already lists the four things
   it gives you.
3. **PostgreSQL as a system**, not a dialect: `EXPLAIN`, indexes, MVCC, transaction isolation, and
   `now()` vs `clock_timestamp()` — the last one has already caused a watermark leak here.
4. **Kafka's model** — partitions, offsets, consumer groups, retention, and ordering being *per
   partition*. Learn the model, not the broker internals. `partitions: 1` in the connector config is
   a correctness decision that only makes sense once you know that.
5. **Docker Compose** — working knowledge is enough, plus one thing that has cost this project two
   bugs: `docker-entrypoint-initdb.d` runs **only on an empty data directory**, so DDL there is not
   a migration.
6. **Airflow** — phase 4. Do not touch it until there is a pipeline to schedule.
7. **PostGIS** — as needed. `ST_MakePoint` argument order, SRID 4326, and `geography` vs `geometry`;
   the swap that changed a distance by 2.14% is in [silver-by-hand §5](../silver-by-hand.md).

### Deliberately not on this list

Spark, Flink, Snowflake, BigQuery, Iceberg, Airbyte, Kafka internals, Kimball cover-to-cover. All
useful eventually. None of them explains anything currently confusing, and a tool learned before its
problem is a tool you cannot explain — which is the rule this whole repository is built on.

---

## What to do first

**Three sittings, in this order.** They cover the one hard problem and both correctness arguments:

1. Streaming Systems ch 1–3 (or Streaming 101/102) — event time, watermarks, lateness
2. Kleppmann ch 7 — transactions, and why commit *ordering* is the whole design
3. Kleppmann ch 11 — stream processing, at-least-once, log-based messaging

Then the checklist below. Anything you cannot answer names your next reading more accurately than
this document can.

---

## Explain-back checklist

Every question already written into the guides, in one place. **Answer from memory.** Where an
answer is given, it is given because the question is a trap.

### Phase 1 — the source system ([01-source-system.md](01-source-system.md))

- [ ] Why do pings carry three timestamps? Which can you trust, and why not the others?
- [ ] What does a gap in `sequence_no` prove that a repeated position does not?
- [ ] Why is `latitude: 900` rejected but a device clock three hours fast accepted?
- [ ] Your OLTP has 22 million ping rows and full history of every driver change — true or false?
- [ ] Why is deduplicating on `ping_id` correct where `(vehicle_id, device_ts)` is not?
- [ ] The simulator made 12 vehicle changes and the database shows 8. Where did the other four go,
      and what query would find them?

### Phase 2 — ingestion ([02-ingestion.md](02-ingestion.md))

- [ ] Why can a batch poller never detect a delete?
- [ ] What does at-least-once mean for your Bronze table, and which column saves you?
- [ ] What happens to a replication slot if the consumer stops for a week?

### Phase 2 — the code ([02-ingestion-code-tour.md](02-ingestion-code-tour.md))

- [ ] Why does `pings` watermark on `server_ts` while `vehicles` watermarks on `updated_at`?
- [ ] What breaks if `BATCH` bounds the poll instead of the statement?
- [ ] Why must `consumer.commit()` come after `conn.commit()`, and what does the reverse look like
      from outside?
- [ ] Why does `envelope.decode` store a message it could not parse instead of raising?
- [ ] Why is `deletes_seen_by_poller` a hardcoded `0` in a module whose rule is that every number
      comes from a query?

### Phase 3 step 1 — Silver by hand ([silver-by-hand.md](../silver-by-hand.md))

- [ ] Why could no query tell you whether the `ping_id` deduplication rule was correct?
- [ ] After a script fails halfway, which tables are stale — and what tells you?
- [ ] The lat/lon swap changed a headline number by 2%. Why is that worse than 10×?
- [ ] `server_ts` is the trustworthy timestamp. In what sense is it not?

**Scoring, such as it is.** Eighteen questions. If you can answer the phase 1 and phase 2 sets
cold, those phases are finished in the sense this project means. If you cannot, the code still runs
and the phase is not done — which is the standard set in
[the curriculum README](README.md).

---

## One honest gap

There is hands-on evidence here for ingestion, typing and deduplication. There is **none yet** for
what phases 3–5 are actually made of: incrementality, restatement, SCD Type 2, orchestration and
serving.

Reading ahead on those is worth it only for the next two issues — lateness (#15) and dimensional
modelling (#13). The rest will stick better after hitting the wall it solves, which is the pattern
that has worked four times so far in this repo: the contaminated percentile, the clock-stepping
pacing bug, the watermark leak firing in ordinary use, and a 2% distance error that passed every
check.
