# The ground-truth harness

What was built for slice A of [issue #14](https://github.com/fatoumg/fleet-telemetry-platform/issues/14),
what it measured, and what it deliberately does not prove.

Every number here came from a query or from a command's own output. Measured 2026-08-28.

## Why this exists before the pathologies

Phase 3's premise is that the simulator knows the truth it generated, so pipeline output can be
diffed against reality rather than inspected for plausibility. Until this ticket, nothing
materialised that truth: `simulator/` performed no file or table writes at all, and its only outputs
were HTTP POSTs and a stdout summary.

That left a gap bigger than it looks. **Every existing assertion in this project proves an internal
property** — a grain holds, a geometry matches its own coordinates, a reconciliation balances against
Bronze. All of them can pass while the pipeline quietly drops or mangles data, because Bronze is the
earliest thing they can see and Bronze is downstream of the loss. The only ground-truth comparison
ever performed before this was manual, once, on a closed window
([docs/silver-by-hand.md](silver-by-hand.md), section 2).

The harness was therefore built and proven against the **clean** simulator, where the answer is
known to be zero, before any pathology flag exists. A diff first exercised against deliberately
dirty data cannot distinguish an injected pathology from a bug in itself.

## What it is

| Piece | Where |
| --- | --- |
| `truth` schema | [docker/warehouse/init.sql](../docker/warehouse/init.sql) for a fresh clone, and `apply()` for volumes that already exist |
| `truth.intended_pings` | [src/fleet_telemetry/truth.py](../src/fleet_telemetry/truth.py) — DDL, row shape, batched writer, `TruthRecorder` |
| `--truth` flag | [simulator/run.py](../simulator/run.py) — opt-in, off by default |
| dbt source | [dbt/models/staging/_sources.yml](../dbt/models/staging/_sources.yml) — the project's second source, and its first non-`bronze` one |
| The diff | [dbt/tests/assert_intended_pings_reached_silver.sql](../dbt/tests/assert_intended_pings_reached_silver.sql) |
| The grain | [dbt/tests/assert_intended_pings_unique_on_run_vehicle_and_sequence.sql](../dbt/tests/assert_intended_pings_unique_on_run_vehicle_and_sequence.sql) |

## Measured: the clean run

```
python -m simulator --live --minutes 10 --vehicles 20 --truth
```

| | |
| --- | --- |
| Pings sent | **2,400** (2,400 inserted, 0 duplicate) |
| Truth rows | **2,400** written of 2,400 recorded |
| `run_id` | `d0087d40-539c-456b-83ad-844f91d1f1cf` |
| Arrival frontier | `2026-08-28 13:17:05.595713+00` |
| Truth rows matched in `silver.stg_pings` | **2,400** — all of them |
| Truth rows **actually compared by the test** | **2,380** |
| Diff result | **0 rows** |

### The 20-row difference is the design working, not a shortfall

2,400 rows arrived; 2,380 were compared. The 20 excluded are exactly one tick — 20 vehicles × 1
tick — held back by the diff's strict `<` against the frontier.

That is the whole reason the inequality is strict rather than `<=`. Every ping in a tick carries the
same `device_ts`, and a batch flush happens at `PINGS_PER_BATCH = 500` regardless of tick
boundaries, so a flush routinely lands mid-tick and the frontier tick is only half arrived.
Excluding it costs one tick of coverage and removes the entire class of false failure. The measured
20 is confirmation, not a coincidence.

### Non-vacuity is the number that matters

`COMPARED BY TEST: 2,380` is the number worth recording, because a test that returns zero rows
having compared zero rows proves nothing at all. Both new tests pass in CI and on a fresh clone for
exactly that reason — the table is empty there, every frontier is null, and the test is green
without asserting anything. That is stated in the test header too, following the precedent of
`assert_ping_rows_are_modelled_or_rejected.sql`.

## Measured: proving the test can fail

A test never seen red is not known to work. Two probes, both fully reversible — deliberately **not**
by deleting a Bronze row, since Bronze is append-only and its Kafka offsets are already committed,
so the deletion could not be undone.

**Probe A — a recorded intent that never arrived.** Inserted one phantom truth row (fresh `ping_id`,
same `run_id`, `intended_device_ts` below the frontier, `emitted = true`):

```
1 of 1 FAIL 1 assert_intended_pings_reached_silver
  Got 1 result, configured to fail if != 0
```

Caught by the `silver_ping_id IS NULL` predicate. Deleted the row; green again.

**Probe B — a value that disagrees.** Took a truth row that *did* match and moved its latitude by
+1.0 degree (13.4549 → 14.4549):

```
1 of 1 FAIL 1 assert_intended_pings_reached_silver
  Got 1 result, configured to fail if != 0
```

Caught by the field-equality predicate. Restored the value; green again.

The two probes exercise the two distinct failure predicates. A missing row and a wrong value are
different bugs, and a diff that only detected one would be half a test.

## The bound took two attempts, and the wrong one is the interesting part

Truth is always **ahead** of Silver: a recorded intent must cross the API, the OLTP, Debezium, Kafka
and Bronze before a Silver view can see it. So an unbounded diff always fails, and every failure is
in-flight data rather than loss — the same flapping that
[assert_ping_rows_are_modelled_or_rejected.sql](../dbt/tests/assert_ping_rows_are_modelled_or_rejected.sql)
records, where a hand-run comparison showed a 60-row gap that was entirely the seconds between two
queries.

The obvious fix is to bound on `max(stg_pings.device_ts)`. **It is wrong here, and quietly so:**

- That maximum is taken over **all** of Silver, including every ping from runs predating this table.
- A backfill run walks its clock from `now - span` to `now`, so its window can end *before* the
  historical maximum.
- In that case the bound admits every truth row — including the ones still in flight — and the test
  fails on a correct pipeline.
- `--reset` cannot rescue it. Bronze is append-only, so that flag deletes OLTP `pings` only and
  Silver keeps every historical row forever.

The bound is therefore a **per-run arrival frontier**: `max(device_ts)` over the Silver rows whose
`ping_id` matches one of *that run's* truth rows. A run with nothing landed yet has a null frontier
and contributes nothing, rather than failing. This is the same BOUNDED-vs-FRONTIER distinction
`tests/test_dbt_silver.py` draws, arrived at for the same reason.

## What this does not prove

**That Silver holds no rows truth never recorded.** The reverse direction is not assertable while
pre-truth history exists — every historical ping would be reported as a phantom. Asserting it and
then adding exclusions until it passed would produce a test that cannot fail, which is worse than no
test. When the reverse direction matters it needs a warehouse whose Bronze was empty when truth
recording began.

**That the deduplication rule chose the right row.** `stg_pings` keeps the earliest Bronze
observation, and the fields compared here are identical between duplicate copies, so a reversed rule
would still pass this diff. `dbt/models/staging/_unit_tests.yml` is what settles that, and the two
tests do not substitute for each other.

**Anything at all in CI.** The simulator never runs there, so the table is empty, every frontier is
null and the diff returns no rows. What CI catches is that the SQL parses and the columns exist.

## Two inversions worth knowing before editing this

**Typed columns, against Bronze's rule.** Bronze is all `text` because a cast inside
`GENERATED ALWAYS` runs on INSERT, so one device sending `"banana"` would take a whole batch down —
Bronze rejecting precisely the malformed evidence it exists to keep. Nothing analogous applies to
`truth.intended_pings`: the values are built by our own code three lines before the insert, so a
failing cast is a bug we want loudly. "Aligning" the two would move every such failure out of the
simulator, where it is a bug report, and into the diff, where it is an unexplained missing row.
`tests/test_truth_load.py` asserts the types from `information_schema` so that change fails.

**A grain constraint, against `pings`' rule.** `pings` deliberately withholds its
`(vehicle_id, sequence_no)` unique constraint so the warehouse can detect a misbehaving device
rather than the application rejecting the evidence. `truth.intended_pings` has the equivalent
constraint, because the difference is who wrote the row: a duplicate ping is evidence about a
device, whereas a duplicate truth row could only be our own recorder writing twice — and it would
inflate the denominator of every ground-truth comparison, in the direction that makes the pipeline
look worse than it is.

## Design decisions recorded here rather than in a comment

**Recorded at reading time, not flush time.** `TruthRecorder.record()` is called immediately after
`take_reading()`, before the ping joins the batch. That is a slice B requirement arriving early: a
sequence-gap row is one the simulator forms the intent to send and then drops, so it never reaches a
flush and a flush-time recorder could not see it. The `emitted` column exists for it and is `true`
on every row today.

**Truth is flushed before the POST, and in a `finally`.** A recorded intent with no ping is a
finding the diff reports; a ping with no recorded intent is a phantom no test can attribute. So the
cheaper failure is chosen deliberately. The `finally` matters because a `--forever` run killed with
SIGTERM is the normal way such a run ends, and its truth rows are worth as much as a clean run's.

**The sink is injected.** Same seam as `config.py`'s `env` mapping, for the same reason: without it
the only way to test the simulator's recording would be to stand up a warehouse, so the tests that
matter most would be the ones least often run. `tests/test_simulator_backfill.py` is hermetic
because of it.

**Parameters bind by name, not position.** `latitude` and `longitude` are adjacent columns of the
same type, so reordering a positional tuple would exchange them with no error, no type mismatch and
no failing cast — the same silent break that once moved fleet total distance by +2.14% with nine of
nine scripts reporting ok. `_COLUMNS` is pinned to `IntendedPing._fields` by a hermetic test.

## Still open

- **No retention policy.** Truth rows are 1:1 with pings. This run wrote 2,400; the design target is
  ~22.2M over 30 days. Two indexes exist; nothing prunes.
- **Pings only.** `job_events` and entity mutations are not recorded, so out-of-order job status
  (slice C) has no ground-truth counterpart yet and would be observable rather than diffable.
- **The reverse direction**, as above — it needs a warehouse with no pre-truth history.
- **`--reset` still only truncates `pings`.** Jobs, job events and vehicle mutations from prior runs
  survive, so a dirty run in slice B will pollute the reference database unless that is widened.
- The remaining open questions are in
  [docs/superpowers/specs/2026-08-28-dirty-data-analysis.md](superpowers/specs/2026-08-28-dirty-data-analysis.md).
