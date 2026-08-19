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

**All nine scripts** are ported, into `dbt/models/staging/`, materialised as **views** in `silver`.
Nothing in this layer is hand-written-only; `gold` and `marts` exist and stay empty until their own
tickets.

```text
bronze.raw_ping_events ──> stg_pings ──────────┬──> ping_quality
bronze.raw_job_events ───> stg_job_events ──┐  │
                                            │  ├──> vehicle_day
bronze.raw_cdc_entities ─┬> stg_vehicles ───┼──┤
                         ├> stg_depots ─────┼──┘
                         ├> stg_drivers     │
                         └> stg_jobs        │
                                            └──> stg_rejected_rows
                             (stg_pings also feeds stg_rejected_rows)
```

`ping_quality` and `vehicle_day` land in `silver` alongside the models they summarise, and that is
worth recording as a decision rather than presenting as the obvious reading. The design spec's layer
table ([§9](superpowers/specs/2026-08-07-telemetry-platform-design.md)) defines Silver as
*"deduplication on `ping_id`, typing, unit normalisation, geometry construction"* and gives Gold
*every* judgement call — and choosing p50/p99 as the lateness summary, bucketing by `device_ts`
rather than `server_ts`, and inner-joining the vehicle dimension are all choices. The tension is
deliberate: the whole layer being in one place and diffable against `silver_manual` beats a clean
layer boundary today, and it resolves when the dimensional model lands and these two become inputs
to `mart_pipeline_health` and `mart_vehicle_utilisation` rather than the end of the line.

They carry no `stg_` prefix in either schema, because they are aggregates over the `stg_` models
rather than stagings of a source — and keeping the hand-written names keeps their diff a one-liner
too.

A clean build:

```text
Found 9 models, 67 data tests, 4 sources, 478 macros, 1 unit test
Done. PASS=77 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=77
```

### It matches the layer it replaces, exactly

Both `EXCEPT` directions, per `tests/test_dbt_silver.py`:

| Model | `silver_manual` | `silver` | only in manual | only in dbt |
| --- | --- | --- | --- | --- |
| `stg_pings` | 190,484 | 190,484 | 0 | 0 |
| `stg_vehicles` | 40 | 40 | 0 | 0 |
| `stg_depots` | 8 | 8 | 0 | 0 |
| `stg_drivers` | 40 | 40 | 0 | 0 |
| `stg_jobs` | 290 | 290 | 0 | 0 |
| `stg_job_events` | 1,128 | 1,128 | 0 | 0 |
| `stg_rejected_rows` | 86 | 86 | 0 | 0 |
| `ping_quality` | 50 | 50 | 0 | 0 |
| `vehicle_day` | 50 | 50 | 0 | 0 |

The two aggregates also cross-check each other, which is worth more than either diff alone: both
declare the grain `(vehicle_id, event_date)`, both derive from `stg_pings`, and both account for
**all 190,484** of its rows. So `vehicle_day`'s inner join on `stg_vehicles` currently drops
**nothing** — the documented wrongness in §6 is latent in this volume, not active. That is a
measurement, not a reprieve: it drops rows the moment a vehicle with pings is deleted, and the
phase-2 diff proved those deletes happen.

Row counts grew during the work — Bronze went from 173k to 205k ping rows as the stack ran — so every
number in this document comes from one consistent snapshot taken after the final rebuild, not from
whenever each section was written.

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
measured that `DISTINCT ON (ping_id)` discarded **zero** rows, so the model's output was identical
whether the rule was right, reversed, or absent. Reading the table could not distinguish them.
Reading the query could not either.

### And then, during this work, real duplicates arrived

Measured: **190,504** usable Bronze rows, **190,484** distinct `ping_id`s. The rule removed **20**.

```text
ping_id                                copies  observations
017bd8af-0265-46a0-b08c-a2e6a1039af2        2  c@197396  c@197416
12d84b39-f677-4af7-b5d6-6f5d90b976e1        2  c@197401  c@197421
140252c6-49e0-4a10-8597-7fedf898b7fb        2  c@197398  c@197418
```

**Both copies carry `op='c'`,** and every pair is exactly 20 offsets apart. That is not the case the
model's own header anticipated — a create plus a later snapshot re-read. It is a **producer-side
redelivery of a single batch of 20 messages**, which Bronze's unique index on
`(_kafka_partition, _kafka_offset)` cannot absorb, because the copies landed at *different* offsets.
The at-least-once delivery this pipeline was designed around finally happened.

It also settles a question the hand-written comment left open. `10_stg_pings.sql` argued that
*"'Prefer `op='c'`' and 'lowest offset' pick the same row — today"*. In the case that actually
occurred, **"prefer `op='c'`" cannot choose at all** — both copies are creates. Lowest-offset is the
only rule that resolves it, which is a stronger argument for the chosen rule than the original one.

**This does not retire the unit test — it vindicates writing it early.** A grain assertion still
cannot settle the rule, because one row per `ping_id` is true under either ordering. And an assertion
added *after* the divergence cannot tell you which side of it was right.

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

One singular test per model, nine in total, all at `+severity: error`. Being honest about them:

**Seven of the nine cannot fail on a correct model.** The six `stg_*` models are `SELECT DISTINCT ON
(key)`, and `ping_quality` is a `GROUP BY` over one relation — Postgres guarantees the grain in both
shapes. Those assertions are **regression guards on the declared key**: they fire if someone changes
the `DISTINCT ON` expression, adds a column to the `GROUP BY`, or joins something in. Not live hazard
detection.

Two earn their keep, and they are the two that touch more than one relation:

**`vehicle_day` is the one this ticket was really about.** It joins three relations. Add a second row
to `stg_vehicles` for one `vehicle_id` and every ping count, speed and distance in that table doubles
— no error, no warning, and every number a human would sanity-check moves *together* and therefore
still looks plausible. Its hand-written header said the problem out loud: *"No test here asserts the
row count, the grain, or that the join did not drop anything. Writing one by hand would mean writing
a query, running it, and reading the answer myself, every time, for every model."* That labour is now
`assert_vehicle_day_unique_on_vehicle_and_date.sql`, on every build.

**`stg_rejected_rows` is the other.** Three `UNION ALL` branches plus a `VALUES` join, so a duplicate
is reachable: an overlapping predicate between two branches, or a `source_table` appearing twice in
the join list. Either would make the reconciliation tests **fail in the direction that looks
correct** — the sum would balance while rows were double-counted as rejected. So its grain assertion
protects those tests, not just itself.

**What a grain assertion still cannot do, and `vehicle_day` is the clearest case.** It catches rows
*added* by a join. It does not catch rows *dropped* by one, because dropping preserves uniqueness
perfectly. The inner join on `stg_vehicles` silently discards a deleted vehicle's entire day, and no
green check anywhere in this layer objects — only a reconciliation against `stg_pings` would, and
that comparison exists in this document (§1) rather than as a test.

So: the uniform assertion is the right shape — it makes every declared grain a checked claim rather
than a comment — but nine green checks are not nine caught bugs.

### Reconciliation, which does do arithmetic

    bronze = silver + rejects + duplicates_removed

| Stream | Bronze | Silver | Rejects | Duplicates | Balances |
| --- | --- | --- | --- | --- | --- |
| `raw_ping_events` | 190,520 | 190,484 | 16 | 20 | ✅ exactly |
| `raw_job_events` | 1,188 | 1,128 | 60 | 0 | ✅ exactly |

Every term is bounded on `max(bronze_offset)`, and the bounds were confirmed non-null — a null bound
would make every comparison null and the test pass vacuously.

**Bounding the rejects, not just Bronze, turned out to matter and is now demonstrated.**
`stg_rejected_rows` holds **70** `raw_job_events` rejects in total but only **60** at or below the
bound. Counting all of them against a bounded Bronze gives 1,128 + 70 = 1,198 ≠ 1,188 — the sum
breaks on correct data, because rejects that arrived after the window are compared against a Bronze
count that excludes them.

**The duplicate term was missing, and that was a real bug in my own test.** The first version
asserted `bronze = silver + rejects`, which held for exactly as long as deduplication did no work.
The moment 20 duplicates arrived it failed on a correctly-functioning pipeline, short by exactly 20 —
and the tempting repair is to loosen it. A reconciliation that breaks the first time deduplication
does its job is worse than none.

The same defect was sitting in the hand-written `tests/test_transform.py`, unnoticed for the same
reason and now fixed alongside. Both had been green since the day they were written, which is worth
noticing: **a reconciliation with a missing term is indistinguishable from a correct one until the
term becomes non-zero.**

The duplicate count is computed **from Bronze, not from the model**, or the assertion would be
circular: the reject predicate is the exact complement of the `usable` filter, so
`bronze − rejects ≡ usable`, and deriving duplicates that way would assert nothing.

These tests are also the **drift detector** for the reject predicate, which is written twice — once
as each model's `usable` filter, once in `stg_rejected_rows` — with no abstraction preventing them
disagreeing. A macro would have prevented it; asserting the property instead is the deliberate trade.

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

`silver_manual.rejected_rows` held rows at all — **86** in the final snapshot — where
[§1](silver-by-hand.md) recorded 0. Looking at
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
filters. Fixing it will move every reject row in that diff, which is a decision rather than a cleanup.

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
`c` 17,684, and nothing else — which is what makes `accepted_values` at `error` severity safe on the
models while it would fail against the sources. The deletes reach Bronze and stop there.

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
| Models, data tests, unit tests | 9 / 67 / 1 — `PASS=77 ERROR=0` |
| Schema the first model resolved to, before the macro | **`silver_silver`**, with a green build |
| Reversed dedup rule: unit test vs grain assertion | **FAIL** vs **PASS** |
| `EXCEPT` diff, all nine pairs, both directions | **0** |
| Ping reconciliation | 190,520 = 190,484 + 16 rejects + 20 duplicates, exactly |
| Job-event reconciliation | 599 = 579 + 20 rejects + 0 duplicates, exactly |
| Duplicate `ping_id`s the rule removed | **20**, both copies `op='c'`, pairs 20 offsets apart |
| Reconciliation terms that were missing until a duplicate appeared | **1**, in two separate tests |
| Rejected rows, and how many are mislabelled | 86, and **all 86** |
| Grain assertions that can fail on a correct model | **2 of 9** (`vehicle_day`, `stg_rejected_rows`) |
| Pings accounted for by both aggregates | 173,046 = 173,046 — the inner join drops nothing *today* |
| Fleet total distance, `vehicle_day` | 12,952.5 km |
| Deletes on append-only streams, as predicted | 16 pings + 70 job_events, every one `op='d'` |

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

4. **Real duplicate `ping_id`s finally appeared. Why did that break two reconciliation tests that
   had always been green — and why is that a bug in the tests rather than the pipeline?**
   Both asserted `bronze = silver + rejects`, which omits the rows deduplication removes. That term
   was zero for as long as the rule did no work, so the arithmetic balanced and the omission was
   invisible. The moment 20 duplicates arrived the sum was short by exactly 20, on a pipeline
   behaving correctly. A reconciliation with a missing term is indistinguishable from a correct one
   until the term becomes non-zero.

5. **Both copies of every duplicate carried `op='c'`. Why does that matter for the tie-break?**
   Because the model's header offered "prefer `op='c'`" as an equivalent rule, and against two
   creates it cannot choose at all. Lowest offset is the only rule that resolves the case that
   actually occurred — a producer-side redelivery, not the create-plus-re-snapshot the comment
   anticipated.

6. **All rejected rows carry a wrong reason. Why is the reject *set* still correct?**
   Because the exclusion is done by the key check, not the after-image check. `payload -> 'after' IS
   NULL` is false for a Debezium delete — `"after": null` is a JSON null, and `->` returns jsonb
   null, not SQL NULL — but `#>> '{after,ping_id}'` through that JSON null does yield SQL NULL. So
   the right rows are rejected for the wrong stated reason.

---

## Next

- The ideas: `docs/learn/03-transformation.md` (issue #16, not written yet)
- `vehicle_day`'s inner join on `stg_vehicles` → `LEFT JOIN` plus an "unknown" dimension member. It
  is the obvious next change and was deliberately not made here: altering it would have changed the
  numbers in the same commit that moved the file, and a port whose output differs from its source
  cannot be verified against it
- Gold: SCD Type 2 from the change stream, and clock-skew correction. That is where the
  `REPLICA IDENTITY FULL` before-image caveat becomes load-bearing, and where a macro for the four
  entity models finally earns its place
- Incremental materialisation and the lateness policy: the `+severity: error` tests here are what
  make restatement safe to attempt
- Defects found along the way: [`known-issues.md`](known-issues.md)
