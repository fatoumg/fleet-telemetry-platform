# Silver in dbt

What changed when the hand-written Silver scripts became dbt models, and what the tests caught that
no query against the data could. The deliverable of
[issue #12](https://github.com/fatoumg/fleet-telemetry-platform/issues/12), which replaces the
scripts described in [`silver-by-hand.md`](silver-by-hand.md).

Every number here came from a query or a build against a running system on 2026-08-19. Nothing is
estimated.

**The headline is not that the port works.** It is that two of the four absences
[silver-by-hand §7](silver-by-hand.md#7-what-this-actually-buys-dbt) charged to the hand-written
layer are now closed with evidence, one trap was found *before* a single model existed, and a live
bug in the ported SQL was found by reading the data it rejected.

---

## 1. What was built

Seven models in `dbt/models/staging/`, materialised as **views** in `silver`:

```text
bronze.raw_ping_events ──> stg_pings ──────────┐
bronze.raw_job_events ───> stg_job_events ─────┤
                                               ├──> stg_rejected_rows
bronze.raw_cdc_entities ─┬> stg_vehicles ──────┘
                         ├> stg_depots
                         ├> stg_drivers
                         └> stg_jobs
```

`ping_quality` and `vehicle_day` are **not** ported. The design spec's layer table
([§9](superpowers/specs/2026-08-07-telemetry-platform-design.md)) defines Silver as *"deduplication
on `ping_id`, typing, unit normalisation, geometry construction"*; those two are percentile
aggregates and a three-way join, which is Gold's work. They stay hand-written in `silver_manual`
until that ticket.

A clean build:

```text
Found 7 models, 65 data tests, 4 sources, 478 macros, 1 unit test
Done. PASS=73 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=73
```

### It matches the layer it replaces, exactly

Both `EXCEPT` directions, per `tests/test_dbt_silver.py`:

| Model | `silver_manual` | `silver` | only in manual | only in dbt |
| --- | --- | --- | --- | --- |
| `stg_pings` | 173,046 | 173,046 | 0 | 0 |
| `stg_vehicles` | 40 | 40 | 0 | 0 |
| `stg_depots` | 8 | 8 | 0 | 0 |
| `stg_drivers` | 40 | 40 | 0 | 0 |
| `stg_jobs` | 151 | 151 | 0 | 0 |
| `stg_job_events` | 579 | 579 | 0 | 0 |
| `stg_rejected_rows` | 36 | 36 | 0 | 0 |

### The diff is harder than it looks, and the first version of it was wrong

`silver_manual` holds **tables**, materialised when `transform.run` last executed. `silver` holds
**views**, evaluated when you query them. Any Bronze row arriving in between appears on one side
only, and the simulator ingests continuously.

The first version of this test bounded `stg_pings` and `stg_job_events` on `max(bronze_offset)` and
assumed the entity models were static. It failed the first time the full stack was up — `stg_jobs`
at 151 vs 152 — because the simulator creates **jobs** continuously too, not just pings.

Bounding does not generalise, and the reason is worth stating. For an append-only stream a prefix
bounded by offset is stable, so the diff can be asserted while ingestion continues. For a
current-state model it is not: filtering the view's output to `bronze_offset <= bound` **drops** an
entity whose latest version is newer, where the snapshot holds that entity's **earlier** version.
There is no bounded query that makes the comparison valid — the model cannot be rewound.

So the two strategies are a property of the source, not a preference:

| Models | Strategy |
| --- | --- |
| `stg_pings`, `stg_job_events` | **Bounded** — assert on a stable offset-bounded prefix, even while ingesting |
| the four entity models, `stg_rejected_rows` | **Frontier** — assert only if Bronze has not advanced since the snapshot; skip with an explicit message if it has |

The frontier check is deliberately *not* "compare row counts and skip if they differ" — that would
mask a genuine port bug that changed the row count as a skip. Comparing the offset frontier means a
stale snapshot skips while a stable frontier with differing content still fails. For the reject bin
the frontier is grouped by topic, because four topics land in `raw_cdc_entities` and their offsets
are independent sequences, so a single `max()` is not a bound.

Verified both ways: with the simulator running, `stg_jobs` and `stg_rejected_rows` skip; with it
stopped and both layers rebuilt, **9 passed, 0 skipped**.

---

## 2. The trap that was already set, and would have gone green

**Found before any model existed, and it cost nothing because it was looked for.**

`dbt/profiles.yml` sets `schema: silver`. `dbt_project.yml` sets `+schema: silver` on `staging`.
With no `generate_schema_name` override — and `dbt/macros/` held only a `.gitkeep` — dbt's built-in
macro applies, and it **concatenates**:

```jinja
{%- else -%} {{ default_schema }}_{{ custom_schema_name | trim }}
```

Measured, with `stg_depots` written and the macro absent:

```text
MODEL stg_depots -> schema: silver_silver | relation: "telemetry"."silver_silver"."stg_depots"
```

And after adding `dbt/macros/generate_schema_name.sql`:

```text
MODEL stg_depots -> schema: silver | relation: "telemetry"."silver"."stg_depots"
```

**`dbt build` succeeds either way.** The models would have been correct, every test would have
passed against them, and `silver` would have stayed empty. Three things would have broken quietly:

- The `silver_manual`-vs-`silver` diff promised at [silver-by-hand.md:314](silver-by-hand.md) would
  have compared against a relation that did not exist — or, once someone created the schema by
  hand to make the error go away, against an empty one, reporting zero differences.
- CI pre-creates `bronze`, `silver`, `gold`, `marts` and nothing else
  (`.github/workflows/ci.yml:83-89`). dbt would have created a fifth schema nobody declared, in the
  job whose purpose is proving the SQL runs where it should.
- `gold` and `marts` would have become `silver_gold` and `silver_marts` the moment those layers
  arrived.

**This is the project's recurring failure class, not a dbt quirk.** The tool reports success and the
warehouse disagrees. Same shape as `load/schema.py`'s `CREATE TABLE IF NOT EXISTS` being idempotent
against *absence, not divergence*, which let the module and the database disagree about a column
while printing "tables present" and exiting 0
([§4a](silver-by-hand.md#4a-the-loader-reported-success-while-the-database-diverged)). Same shape as
the four `REPLICA IDENTITY FULL` statements that sat unexecuted for five days
([known-issues §1](known-issues.md)).

Why it had gone unnoticed: the comment at `dbt_project.yml:17-18` asserted the **inverse** — that
*"`+schema` with a custom `generate_schema_name` macro would prefix them"*. The default is what
prefixes; a custom macro is what makes names absolute. And no model had ever been built, so nothing
had exercised it. The source tests added in phase 2 never could: a source's schema comes verbatim
from `_sources.yml` and never passes through `generate_schema_name`. Confirmed — the baseline
manifest reported `source schemas: ['bronze']`, `model schemas: []`.

Now asserted from the database by `test_silver_is_not_silver_silver`.

---

## 3. The deduplication rule is finally decidable

[§2](silver-by-hand.md#2-the-deduplication-removes-nothing-and-no-query-can-tell-you-whether-it-is-right)
measured that `DISTINCT ON (ping_id)` discards **zero** rows, so the model's output is identical
whether the rule is right, reversed, or absent. Reading the table cannot distinguish them. Reading
the query cannot either.

A grain assertion **does not close that gap**, and this is the part worth understanding. It proves
the output holds one row per `ping_id` — which is true under both rules.

`dbt/models/staging/_unit_tests.yml` supplies two synthetic Bronze rows sharing one `ping_id` at
offsets 10 (`op='c'`, the streamed create) and 99 (`op='r'`, a re-snapshot), and asserts offset 10
survives. Measured, with the `ORDER BY` reversed to `_kafka_partition DESC, _kafka_offset DESC`:

```text
stg_pings_dedup_keeps_earliest_observation ............ FAIL
assert_stg_pings_unique_on_ping_id .................... PASS
assert_stg_pings_geometry_matches_its_coordinates ..... PASS
```

**The grain assertion passes on a silently wrong rule.** That contrast is the argument for this
ticket, and it is now a measurement rather than a claim.

Two things the fixture had to work around, both non-obvious:

- **Postgres's 63-character identifier limit.** dbt materialises a unit test as a relation, so a
  self-describing 64-character name failed with `Relation name ... is longer than 63 characters` —
  which reads like a dbt bug rather than a naming rule.
- **Numeric scale.** `EXTRACT(EPOCH FROM interval)` returns `numeric` at scale 6 and dbt compares
  rendered values, so an expected `34.0` fails against an actual `34.000000`. Fixed in the fixture,
  never the model: the model must stay byte-identical for the `EXCEPT` diff to mean anything.

The same rule governs `stg_job_events`, and it is **not** pinned by a unit test. Neither is the
latest-wins rule in the four entity models — and `drivers` was measured at 0 of 40 rows changed
since creation, so that rule has never had to choose between two versions either. Same blind spot,
same argument, not bought by this ticket. Flagged in each model.

---

## 4. What the grain assertions actually buy — and what they don't

One singular test per model, seven in total, all at `+severity: error`. Being honest about them:

**Six of the seven cannot fail on a correct model.** `stg_pings`, `stg_vehicles`, `stg_depots`,
`stg_drivers`, `stg_jobs` and `stg_job_events` are all `SELECT DISTINCT ON (key)`, and Postgres
guarantees one row per `DISTINCT ON` key. Those assertions are **regression guards on the declared
key** — they fire if someone changes the `DISTINCT ON` expression or adds a join — not live hazard
detection.

**`stg_rejected_rows` is the exception**, and it is the one that earns its keep. Three `UNION ALL`
branches plus a `VALUES` join, so a duplicate is genuinely reachable: an overlapping predicate
between two branches, or a `source_table` appearing twice in the join list. Either would make the
reconciliation tests **fail in the direction that looks correct** — the sum would balance while rows
were double-counted as rejected.

**The model where fan-out is a live hazard is not in this layer.** `50_vehicle_day.sql:96-101` joins
three tables, its own header says *"No test here asserts the row count, the grain, or that the join
did not drop anything"*, and it remains hand-written and untested. Add one row to `stg_vehicles` for
a `vehicle_id` and every distance, speed and ping count in that table doubles with no error. That
assertion is the Gold ticket's, and it is the most valuable one in the project.

So: the uniform assertion is the right shape — it makes the declared grain a checked claim rather
than a comment — but seven green checks here should not be read as seven caught bugs.

### Reconciliation, which does do arithmetic

| Stream | Bronze | Silver | Rejects | Balances |
| --- | --- | --- | --- | --- |
| `raw_ping_events` | 173,062 | 173,046 | 16 | ✅ exactly |
| `raw_job_events` | 599 | 579 | 20 | ✅ exactly |

Bounded on `max(bronze_offset)`, and the bounds were confirmed non-null (173,061 and 598) — a null
bound would make the comparison null and the test pass vacuously. **This volume is a better test bed
than the clean one §1 was written against, where rejects were 0** and the arithmetic held trivially.

These two tests are also the **drift detector**. The reject predicate is written twice — once as each
model's `usable` filter, once in `stg_rejected_rows` — and no abstraction prevents them disagreeing.
A macro would have prevented it; asserting the property instead is the deliberate trade, and it is
the habit this project already follows.

**There is no equivalent test for the four entity models, and the absence is a decision.** They
deduplicate to current state, so most Bronze rows are neither modelled nor rejected but
**superseded**. `in = out + rejects` is simply false for them. Asserting it would require inventing a
supersession term, and a reconciliation whose residual is "everything I could not account for"
reconciles nothing. The honest version belongs with the Type 2 work, where every version is retained
and the arithmetic closes again.

---

## 5. A live bug in the SQL, found by reading what it rejected

`silver_manual.rejected_rows` held **36 rows**, where [§1](silver-by-hand.md) recorded 0. Looking at
one:

```text
op | _kafka_offset | payload -> 'after' IS NULL | payload_head
d  |        172808 | false                      | {"op": "d", "after": null, "before": {...}}
```

Two findings, from three lines of output.

### 5a. `payload -> 'after' IS NULL` is dead code

Debezium emits `"after": null` — the key is **present** with a JSON null value. `->` returns jsonb
`'null'`, which **is not SQL NULL**. So the predicate evaluates to `false` on every delete, and
`WHEN payload -> 'after' IS NULL THEN 'no after-image, op=' || op` never fires.

Every delete falls through to the `ELSE` arm. All 36 are labelled `'ping_id is null'` or
`'job_event_id is null'` when the truth is `'no after-image, op=d'`.

What is **not** affected: the *set* of rejected rows. `ping_id IS NOT NULL` does the exclusion —
`#>>` through a JSON null *does* yield SQL NULL — so membership is correct and the reconciliation
balances. Only the human-readable reason is wrong. The correct predicate is
`jsonb_typeof(payload -> 'after') = 'null'`.

Ported **verbatim on purpose**, so the diff stays a comparison of logic rather than of two different
filters. Fixing it will move 36 rows in that diff, which is a decision rather than a cleanup.

### 5b. The `u`-or-`d` prediction came true

`_sources.yml:50-54` said only `c` and `r` were expected on the append-only streams, and that a `u`
or a `d` arriving *"is itself a finding rather than something to handle"*. It arrived: **16 pings and
20 job_events, all `op='d'`** — most likely `simulator --reset` deleting OLTP rows, which CDC
faithfully captured.

Their before-images confirm the documented fabricated-defaults pathology **exactly**: `latitude 0.0`,
`longitude 0.0`, `device_ts 1970-01-01T00:00:00Z`. `pings` is deliberately left at the default
`REPLICA IDENTITY`, so Debezium fills the before-image with type defaults and `ping_id` is the only
real field in it. A row that never existed, described plausibly.

Post-exclusion, `bronze_op` in Silver is only what the contract allows — `stg_pings`: `r` 172,800 /
`c` 246; `stg_job_events`: `r` 535 / `c` 44 — which is what makes `accepted_values` at `error`
severity safe on the models while it would fail against the sources.

---

## 6. What is still absent

| Absence | Status |
| --- | --- |
| **No dependency graph** | **Closed.** `ref()` builds it. `stg_rejected_rows` can no longer run before the table it writes into exists, and renaming a file changes nothing — the `00_` prefix fragility its own header complained about is gone |
| **No tests** | **Closed.** 65 data tests, 7 grain assertions, 2 reconciliations, 2 geometry assertions, 1 unit test, all `+severity: error` |
| **No lineage** | **Closed.** A failing model fails its descendants and still builds its siblings, instead of leaving one table committed, one rolled back to stale contents, and seven never attempted with nothing recording which was which |
| **No state** | **Open.** Every model is a view, so every downstream query re-runs `DISTINCT ON` over all of Bronze. Fine at 173k rows; the target is ~22.2M over 30 days. `incremental_lookback_days: 3` already sits in `dbt_project.yml` waiting, and it MUST exceed `lateness_bound_hours: 6` |

Also deliberately absent, each recorded in place rather than left to be read as an oversight:

- **No `relationships` tests.** `DELETE /vehicles/{id}` is a hard delete on purpose and the phase-2
  diff proved two vehicle lifecycles really were deleted, so `stg_jobs.vehicle_id` →
  `stg_vehicles.vehicle_id` would fail on **correct** data at `error` severity. Whether a missing
  dimension member is dropped or marked unknown is a presentation judgement, and judgements are
  Gold's.
- **No `not_null` on `stg_job_events.from_status`** — null for a job's first event by design, 163 of
  579 rows. On `speed_kmh`/`heading_deg` — nullable in the OLTP, and a nullable column that happens
  to be full today is a promise nobody made. On `lateness_seconds` — it can legitimately be negative.
- **No macro for the four near-identical entity models.** The duplication is real; it is not on the
  list of things that has cost anything. A tool introduced before its problem is a tool you cannot
  explain, and that covers Jinja as much as Airflow. The macro's moment is the Gold ticket, where
  Type 2 history triples that scaffolding in all four at once.

### One thing noticed in passing, for the lateness ticket

`max(write_lag_seconds)` on `stg_job_events` is **21,601.69 s = 6.0005 hours**, against
`lateness_bound_hours: 6` = 21,600 s. The simulator's deliberate out-of-order emission reaches the
bound and steps 1.7 s past it. Not this ticket's problem; a measured example to test a bound
against.

---

## 7. Numbers worth remembering

| | |
| --- | --- |
| Models, data tests, unit tests | 7 / 65 / 1 — `PASS=73 ERROR=0` |
| Schema the first model resolved to, before the macro | **`silver_silver`**, with a green build |
| Reversed dedup rule: unit test vs grain assertion | **FAIL** vs **PASS** |
| `EXCEPT` diff, all seven pairs, both directions | **0** |
| Ping reconciliation | 173,062 = 173,046 + 16, exactly |
| Job-event reconciliation | 599 = 579 + 20, exactly |
| Rejected rows, and how many are mislabelled | 36, and **all 36** |
| Grain assertions that can fail on a correct model | **1 of 7** (`stg_rejected_rows`) |
| Deletes on append-only streams, as predicted | 16 pings + 20 job_events |

---

## Explain-back

Answer from memory. If you cannot, this phase is not finished, however green the build.

1. **Why does a grain assertion not prove the deduplication rule is correct?**
   Because it asserts the output has one row per `ping_id`, which is true whether the rule keeps the
   earliest or the latest observation. Bronze holds no duplicates, so `DISTINCT ON` discards nothing
   and both rules produce identical output. Only a fixture containing a duplicate distinguishes
   them — measured: reversed rule, unit test fails, grain assertion passes.

2. **What would `+schema: silver` have done without `macros/generate_schema_name.sql`, and why would
   the build have been green?**
   dbt's built-in macro concatenates `target.schema` with the custom schema, so models resolved to
   `silver_silver`. The build succeeds because creating a view in the wrong schema is not an error —
   the models are correct, their tests pass against them, and `silver` is simply empty. Nothing
   about a passing build reports the destination.

3. **Why is there no reconciliation test for `stg_vehicles`?**
   Because it deduplicates to current state, so most Bronze rows are superseded rather than modelled
   or rejected, and `in = out + rejects` is false for it. Asserting it would need a supersession
   term, which makes the residual "everything I could not account for". The check closes properly
   once Type 2 history retains every version.

4. **All 36 rejected rows carry a wrong reason. Why is the reject *set* still correct?**
   Because the exclusion is done by the key check, not the after-image check. `payload -> 'after' IS
   NULL` is false for a Debezium delete — `"after": null` is a JSON null, and `->` returns jsonb
   null, not SQL NULL — but `#>> '{after,ping_id}'` through that JSON null does yield SQL NULL. So
   the right rows are rejected for the wrong stated reason.

---

## Next

- The ideas: `docs/learn/03-transformation.md` (issue #16, not written yet)
- `ping_quality` and `vehicle_day` into Gold, where `vehicle_day` finally gets the grain assertion
  that can actually fail
- Incremental materialisation and the lateness policy: the `+severity: error` tests here are what
  make restatement safe to attempt
- Defects found along the way: [`known-issues.md`](known-issues.md)
