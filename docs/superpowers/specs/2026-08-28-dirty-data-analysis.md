# Requirements analysis — issue #14, "Make the simulator emit dirty data"

Written 2026-08-28, before any implementation. This is the reasoning behind the three-way slicing
of issue #14 and the corrections to its acceptance criteria. The first slice is planned in
`docs/superpowers/plans/2026-08-28-truth-harness.md`.

## Context

Phase 3 is the project. It holds the one hard problem: events arrive late, out of order, and in
bursts, and the warehouse must still be correct. Right now there is nothing to be correct *about* —
the simulator emits clean data. Measured baseline (`docs/source-system-reference.md`): lateness p50
34 s / p99 67 s / max 68 s, all positive; **0** sequence gaps, **0** repeats. That baseline was
recorded precisely so any anomaly seen later is provably the injection rather than a pre-existing
bug.

The intended outcome is a simulator that can be told to misbehave in one specific way at a time,
plus a written record of what it *meant* to emit — so pipeline output can be diffed against truth
instead of merely looking plausible. Without the second half, correctness claims are unfalsifiable,
which CLAUDE.md calls a failed outcome.

Two decisions taken during analysis:

- **Schema drift is out of scope**, split to its own issue. It is an API-contract change, not a
  data-generation change: Pydantic (`app/models.py:40-60`) rejects an unknown or renamed field with
  a 422 before it can reach OLTP, so "rigid parsers fail" is untestable without loosening the
  contract. Six pathologies remain.
- **Ground truth lands in a warehouse table read as a dbt source**, not a JSONL file — so the diff
  is a SQL join and a dbt test rather than a bespoke Python comparison.

## Summary

Give the simulator six independently-toggleable pathology flags (reconnect burst, clock skew, retry
storm, silent vehicle, sequence gap, out-of-order job status), and have it record every intended
emission to a new `truth` schema in the warehouse so pipeline output is diffable against ground
truth.

**Modules affected:** `simulator/` (the injection), a new `src/fleet_telemetry/truth.py` (the DDL),
`docker/warehouse/init.sql` (the schema), `dbt/models/staging/_sources.yml` plus a new singular test
in `dbt/tests/` (the diff), and `.github/workflows/ci.yml` (CI creates schemas by hand). `app/` needs
**no change** — every field a pathology touches is already client-controlled and deliberately
unvalidated.

**Slicing — three independently mergeable branches; #14 becomes an epic:**

| Slice | Content | Why this order |
| --- | --- | --- |
| **A** | `truth` schema + table + dbt source + the diff, proven against the **clean** simulator where it must come out exactly zero | A harness validated while you still know the answer is zero is the only way to later tell an injection from a bug |
| **B** | The four ping-stream flags: burst, skew, gap, silent | All four live in `take_reading`/`flush`; one test pattern covers them |
| **C** | Retry storm + out-of-order job status + the false-doc fix below | Different code path (`advance_jobs`, `Api.post_pings`), different observables |

## Acceptance criteria gaps

### 1. The retry storm cannot produce "double-counted rows" in this architecture

The issue's table says a retry storm breaks "double-counted rows". It cannot. The application
already absorbs device retries — `on conflict (ping_id) do nothing` (`app/main.py:170`) — so a
retried ping never reaches Bronze at all. Its **only** observable is the API's duplicate counter,
already plumbed through `Api.pings_sent`/`pings_inserted` (`simulator/run.py:149-150`) and printed at
`simulator/run.py:591-592` — a counter that is structurally always 0 today.

This matches CLAUDE.md's own rule: *"the app already removed device retries, so a duplicate reaching
Bronze can only be a redelivery."* The acceptance criterion has to be rewritten as **"the API-level
duplicate rate becomes non-zero and is reported"**, not "the warehouse deduplicates." Real Bronze
duplicates already exist from a different cause — 20 producer-side redeliveries, 20 offsets apart,
documented at `dbt/models/staging/stg_pings.sql:81-92`.

### 2. "Silent vehicle" and "sequence gap" must be defined as opposites

The issue lists them as separate rows but does not say what separates them. The project's own rule
does — *"A gap is not a stop"*:

- **Silent vehicle**: skip the reading entirely and **do not** advance `sequence_no`. Downstream
  sees absence with no gap — nothing proves loss.
- **Sequence gap**: take the reading (advancing `sequence_no` at `simulator/run.py:227`) and then
  **drop** it. The counter advances, rows are missing — loss is provable.

Without that split the two flags are indistinguishable and the pair teaches nothing.

There is a second trap. `simulator/world.py:178-185` deliberately keeps *parked* vehicles pinging,
with the reasoning that if idle vehicles went silent, "stopped" and "disappeared" would be identical
in the data — and `tests/test_world.py:79-89` pins it. The silent-vehicle flag must therefore model
**communication** silence, not **idleness** silence, and that existing test must stay green.

### 3. Nothing downstream can handle any of this yet

`lateness_bound_hours: 6` and `incremental_lookback_days: 3` sit in `dbt/dbt_project.yml`
**entirely unconsumed** — a repo-wide grep for `{{ var(` finds only definitions and prose. There are
no incremental models, no quarantine table, no restatement log, no `mart_pipeline_health`.

So the dirty data will be *visible* and not *handled*. That is correct and intended — it is what
makes phase 3 bite — but the story must not be mistaken for "phase 3 done." Explicit non-goal:
*handling the pathologies is out of scope; these tickets only create them and prove they are
detectable.*

### 4. Edge cases the story does not cover

- **Do flags compose?** "Independently toggleable so a test can enable one in isolation" says
  nothing about two at once. Recommendation: composition permitted but untested, and the truth
  table records *which* flag caused each divergence.
- **Boolean or rate?** "Toggleable flag" implies boolean, but a burst needs a frequency and a
  duration, and skew needs a magnitude. Every flag needs a default intensity or the flag is
  meaningless.
- **How is a pathology run kept out of the baseline?** `--reset` (`simulator/run.py:129-135`) deletes
  only `pings`. `jobs`, `job_events` and vehicle mutations from prior runs survive, so a dirty run
  permanently pollutes the reference database unless reset is widened.
- **Same-seed reruns.** `ping_id` is an unseeded `uuid.uuid4()` (`simulator/run.py:218`), so the
  truth table needs a `run_id` to distinguish two runs of the same seed. Note the docstring at
  `simulator/world.py:129-131` claims "byte-identical output" for the same seed, which is already
  false for this reason.

### 5. A false claim in the repo, to fix as part of slice C

Three places already assert that out-of-order job status **exists**:

- `dbt/models/staging/_sources.yml:140-142` — *"the simulator deliberately emits deliveries that
  arrive before their own pickup"*
- `dbt/models/staging/stg_job_events.sql:29-32` — the same claim
- the assertion header at `dbt/tests/assert_stg_job_events_unique_on_job_event_id.sql:3-8`

It does not. `advance_jobs` (`simulator/run.py:231-247`) emits `created`, `assigned` and `picked_up`
with an **identical** `occurred_at`, and `delivered` strictly later, in tick order. The large
`write_lag_seconds` those files point at (21,601.7 s, per `dbt/models/staging/_models.yml:398`) comes
from backfill mode's simulated-past `occurred_at` against a real-clock `created_at` — not from
out-of-order arrival. Slice C makes the claim true and corrects the wording either way.

## Technical approach

### Where the injection lives

`take_reading(vehicle, world, now)` (`simulator/run.py:209`) has **no parameter for policy**, and the
sim functions take flat positionals. There is no config dataclass — an `argparse.Namespace` is
threaded through by hand.

Proposal: a new `simulator/pathology.py` holding a `frozen=True` dataclass with one field per flag
plus its intensity, constructed once in `main` and passed into `take_reading`, `flush` and
`advance_jobs`. New CLI flags go after `--reset` (`simulator/run.py:501`), with mutual-exclusion
checks beside the existing ones at `simulator/run.py:504-510`.

**Each pathology gets its own RNG.** This is the single most important design point.
`self.rng = random.Random(seed)` (`simulator/world.py:138`) is one shared stream drawn on by world
construction, dwell draws, the transmission delay (`simulator/run.py:300`) and mutation decisions.
Every consumer draws in call order, so **any** new draw anywhere shifts every subsequent value and
invalidates seed-pinned expectations across the board. Give each pathology
`random.Random(f"{seed}:{name}")`. Then enabling one flag cannot perturb another, and a clean run's
stream is bit-for-bit what it is today — which is what "independently toggleable" has to mean
mechanically.

### Pathology by pathology

| Flag | Mechanism | Where |
| --- | --- | --- |
| **Reconnect burst** | Vehicle goes "offline" for N ticks; readings go to a per-vehicle held list instead of the main buffer, `sequence_no` still advancing (a burst is not loss). On reconnect the held readings join the next flush: old `device_ts`, current `server_ts`. | `flush` closure, `simulator/run.py:293-304` |
| **Clock skew** | Per-vehicle constant offset, drawn once, added to `device_ts` **only**. | `simulator/run.py:221` |
| **Retry storm** | Re-POST a batch (or a subset) a second time. | `Api.post_pings`, `simulator/run.py:152` |
| **Silent vehicle** | Skip the reading; do **not** advance `sequence_no`. | the per-vehicle loop, `simulator/run.py:308-309` |
| **Sequence gap** | Take the reading (advancing the counter), discard it. | same loop |
| **Out-of-order job status** | Perturb the `occurred_at` handed to `move_job` so `delivered` precedes `picked_up`. | `advance_jobs`, `simulator/run.py:231-247` |

Three consequences worth stating explicitly, because each looks like a bug:

1. **A reconnect burst needs no batch splitting.** `server_ts_override` is one value for the
   **entire** POST (`app/models.py:74`, applied at `app/main.py:139-162`), which initially looks
   fatal for per-ping lateness. It is not, for this pathology: held-back readings *should* share the
   current batch's late `server_ts` — old `device_ts` plus new `server_ts` **is** out-of-order
   arrival. But it does mean per-ping lateness *variation* is unreachable without multiple POSTs, so
   no design should assume it.

2. **`buffer_max_device_ts` must be renamed and re-scoped.** Today it is assigned `now`
   unconditionally on every append (`simulator/run.py:310`) and feeds
   `server_ts = buffer_max_device_ts + rng.randint(1, 8)` (`simulator/run.py:296-301`). Once a batch
   can contain backdated readings, "max device_ts" and "truth now" diverge, and a reconnect batch
   would have its `server_ts` dragged backwards into the past. It must track **truth time** — call it
   `batch_truth_ts`. The regression note at `simulator/run.py:282-291` (a batch that stamped the
   previous flush's timestamp and backdated 169,480 rows) is the same class of bug.

3. **A skewed ping is internally inconsistent, on purpose.** Position is a pure function of `now`,
   not of `device_ts` (`simulator/world.py:94-103`), and `take_reading` computes lat/lon from the
   `now` it was handed. So skewing the timestamp does not move the vehicle — which is correct,
   because a wrong clock doesn't move a van. This needs a comment or someone will "fix" it. Forward
   skew yields `server_ts < device_ts`; `ping_quality.negative_lateness`
   (`dbt/models/staging/ping_quality.sql:88`) already counts that and is deliberately **not** a
   test, so CI stays green.

**No API change is required.** Every field the six flags touch is already accepted by design:
`device_ts` has no bound at all (a +3 h future timestamp returns 202 — `tests/test_app.py:185-192`);
`sequence_no` may skip, repeat or rewind, with the unique constraint deliberately withheld
(`docker/oltp/init.sql:236-238`); `occurred_at` is client-controlled and unvalidated
(`app/models.py:95`), and `JOB_TRANSITIONS` (`app/main.py:240-246`) checks the status graph only,
never timestamp monotonicity. `FLEET_ALLOW_SERVER_TS_OVERRIDE` stays the only privileged path,
unchanged.

### The truth table (slice A)

- **`truth` schema** — added to `docker/warehouse/init.sql` alongside `bronze`/`silver`/`gold`/`marts`
  (that file owns schemas and defines no tables, by its own rule at lines 1-5). CI creates schemas
  with `psql` rather than running this file, so `.github/workflows/ci.yml` needs the same addition or
  `dbt parse` fails on an unknown source.
- **`truth.intended_pings`** — one row per intended emission, declared in a **new**
  `src/fleet_telemetry/truth.py` following the shape of `src/fleet_telemetry/load/schema.py`. Not
  added *to* `schema.py` — that module's docstring scopes it to bronze, and `apply()` hardcodes
  `bronze` as the only schema it creates.
- Columns: `run_id`, `seed`, `vehicle_id`, `sequence_no`, `ping_id`, `intended_device_ts`,
  `truth_ts`, `emitted`, `pathology`, and the payload fields.
- **These columns are typed, not text** — the opposite of Bronze's rule. Bronze is all `text` because
  it must never reject the malformed evidence it exists to keep. The truth table is written by our
  own code, so a cast failure there is a bug we want loudly. State the distinction in the module
  docstring or someone will "fix" it to match.
- `ping_id` must be recorded: it is an unseeded UUID and the only join key into `silver.stg_pings`.

Integration points:

- **The simulator gains a warehouse connection.** It currently talks to OLTP directly for
  `fetch_depots` / `fetch_vehicle_ids` / `reset_pings`, so direct DB access has precedent — but the
  warehouse is new. `config.warehouse()` already exists; no new config shape needed. Writes must be
  batched via `executemany`, never per row.
- **dbt source** — a second entry in `dbt/models/staging/_sources.yml`. `poll_rows` is precedent for
  declaring a table as a source with no model reading it (lines 164-167).
- **The diff** — a singular test modelled on
  `dbt/tests/assert_ping_rows_are_modelled_or_rejected.sql`. Two properties of that file must be
  copied: every term bounded by the same window (lines 48-52), and the note that it proves **nothing
  in CI**, where the tables are empty and every comparison is null (lines 57-59).

### Tests

- **`tests/test_simulator_backfill.py` is new and needed regardless.** `simulate()` — the backfill
  path, the one carrying `TRANSMISSION_DELAY_SECONDS` and the `server_ts_override` logic, and where
  every ping-stream flag lands — has **zero test coverage** today.
  `tests/test_simulator_forever.py` covers only `next_tick` and `simulate_forever`.
- Reuse its `StubApi(Api)` harness (`tests/test_simulator_forever.py:101-132`).
- One test per flag: enable it alone, assert the specific metric moves **and no other does**. That
  second half is what the per-pathology RNG buys.

## Potential risks

**High — the shared RNG stream.** Covered above. Handled by per-pathology `Random` instances; get
this wrong and every seed-pinned expectation in the repo shifts silently.

**High — destroying the measured baseline.** `docs/source-system-reference.md` is the reference the
whole project measures against, and a dirty run against the same OLTP database contaminates it
irreversibly. `--reset` only truncates `pings`. There is precedent for exactly this damage: the
integration suite once "dragged the profiled lateness minimum to -208,270,649 seconds and invented
three sequence gaps covering 895,683 phantom pings" (`tests/test_app.py:47-54`). Mitigations: keep
pathology tests hermetic via `StubApi`; any integration test stays inside the
`sequence_no >= 900_000` teardown range (`tests/test_app.py:35-37`); widen `--reset` or use a
separate database for dirty runs.

**Medium — injecting into untested code.** `simulate()` has no tests. Write the harness first.

**Medium — the one place a pathology can vanish silently.** `vehicle_day` joins three relations with
an **INNER** join on `stg_vehicles` (`dbt/models/staging/vehicle_day.sql:118`), documented as wrong.
A silent vehicle whose vehicle row was deleted disappears from the model with **no test firing** — no
grain assertion catches rows an inner join drops. The truth diff is the only thing that would notice,
which is an argument for slice A landing first.

**Low — dbt tests going red.** Walked through and mostly fine by design: duplicates are absorbed at
the app so Bronze sees none; `negative_lateness` is deliberately not a test; gap metrics carry only
`not_null`; every grain assertion is `DISTINCT ON` or a single-relation `GROUP BY`. The one to watch
is `write_lag_seconds`, already measured at 21,601.7 s — 6.0005 hours, *just past*
`lateness_bound_hours: 6` (`dbt/models/staging/_models.yml:398`). Injecting out-of-order job status
pushes it further, and tests are `+severity: error` project-wide.

**Low — security and access.** No new privileged path: every pathology works inside fields the API
already accepts from an untrusted client, and `FLEET_ALLOW_SERVER_TS_OVERRIDE` is untouched. The one
widening is the simulator gaining **write** access to the warehouse, which it has never had.

**Performance / data volume.** Truth rows are 1:1 with pings. Batch the inserts, index on
`(run_id, ...)` and on `ping_id` for the join, and decide a retention story before 22M rows
accumulate. No migration risk — every new object is additive and no existing table changes shape.

## Open questions

1. **Truth table scope** — pings only, or also `job_events` and entity mutations? Pings-only for
   slice A; out-of-order job status (slice C) then needs its own decision about whether it is
   diffable or merely observable.
2. **Flag intensity** — boolean, or does each flag take a rate/magnitude? Proposal: boolean flags
   with documented default intensities, overridable.
3. **`run_id` semantics** — settled by slice A: one per simulator process.
4. **Retry storm acceptance criterion** — given it cannot reach Bronze (gap 1), is "the API duplicate
   counter becomes non-zero and is reported" sufficient, or should the story instead target the
   producer-side redelivery path that *does* create Bronze duplicates?
5. **Where flags are configured** — CLI only, or also env/config so `docker/docker-compose.yml` can
   run a dirty `--forever` service? CLI-only is simpler and matches every existing flag.
6. **Baseline protection** — is widening `--reset` to clear `jobs`/`job_events`/vehicle mutations
   acceptable, or should dirty runs get their own database?
7. **The false doc claim** (gap 5) — fixed inside slice C, or its own issue? Recommendation: inside
   slice C; it is three files and the claim becomes true in the same branch.
8. **Sequencing against phase 3 proper** — does watermark/quarantine work start before or after these
   flags exist? The vars are already in `dbt_project.yml` unconsumed, so either order is possible,
   but building a bound against clean data would repeat the mistake these tickets exist to prevent.
