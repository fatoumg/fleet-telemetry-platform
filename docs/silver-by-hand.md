# Silver by hand

What happened when the Silver layer was written as plain SQL scripts run in order, with no
framework — and what broke when a column changed. The deliverable of
[issue #11](https://github.com/fatoumg/fleet-telemetry-platform/issues/11), which exists to be
replaced by dbt in #12.

Every number here came from a query against a running system on 2026-08-13. Nothing is estimated.

**The layer works.** That is not the interesting part. The interesting part is that four separate
things went wrong or could not be checked, and the layer reported success for all four.

---

## 1. What was built

Nine scripts in [`sql/silver/`](../sql/silver/), three levels deep, run in filename order by
[`transform/run.py`](../src/fleet_telemetry/transform/run.py) into a **`silver_manual`** schema —
not `silver`, which belongs to dbt.

```text
level 1   10_stg_pings ─────────────────┬──> 40_ping_quality
level 2   20_stg_vehicles ──┬───────────┴──> 50_vehicle_day
          21_stg_depots ────┤
          22_stg_drivers ───┤
          23_stg_jobs ──────┘
          30_stg_job_events
```

A clean run, cold:

```text
  00_rejected_rows.sql         ok      0.04s
  10_stg_pings.sql             ok      7.28s
  20_stg_vehicles.sql          ok      0.02s
  21_stg_depots.sql            ok      0.01s
  22_stg_drivers.sql           ok      0.01s
  23_stg_jobs.sql              ok      0.01s
  30_stg_job_events.sql        ok      0.04s
  40_ping_quality.sql          ok      0.69s
  50_vehicle_day.sql           ok      1.91s
```

### It is correct, as far as correctness can be checked by looking

| Check | Result |
| --- | --- |
| Ground truth, closed window (`server_ts < 12:00:00Z`) | OLTP **342,992** = `stg_pings` **342,992** |
| `job_events` end to end | OLTP **1,435** = `stg_job_events` **1,435** |
| Grain, `stg_pings` | 351,432 rows, 351,432 distinct `ping_id` |
| Grain, the other six tables | holds on every one |
| Idempotent | two runs, identical count **and** identical checksum |
| Rejected rows | **0** |
| Casts | `uuid`, `integer`, `bigint`, `timestamptz`, `double precision`, `geometry` |

The ground-truth comparison has to be made on a **closed window**. The simulator runs
continuously at ~2 pings/second, so an unbounded `count(*)` on each side is two measurements of a
moving target: the first attempt showed a 60-row gap that was entirely the seconds between the two
queries. A test that compares live counts fails for reasons unrelated to correctness, and is then
"fixed" by loosening it, which is how a real reconciliation check becomes decorative.

---

## 2. The deduplication removes nothing, and no query can tell you whether it is right

Silver's first specified job is *deduplicate on `ping_id`*. Measured:

```text
 rows   | distinct_ids | ping_ids appearing under >1 op code
--------+--------------+------------------------------------
 350742 |       350742 |                                  0
```

**`DISTINCT ON (ping_id)` currently discards zero rows.** `pings` is append-only in the OLTP —
inserted with `ON CONFLICT (ping_id) DO NOTHING`, never updated, never deleted — so a `ping_id`
reaches Bronze twice only if the connector is re-registered against a fresh offset and re-snapshots
a row it already streamed. That has not happened here.

So the output of `10_stg_pings.sql` is byte-identical whether the deduplication rule is right,
wrong, or absent. Reading the table cannot distinguish those. Reading the *query* cannot either —
`ORDER BY ping_id, _kafka_partition, _kafka_offset` looks equally plausible reversed, and reversed
it would silently prefer a later re-snapshot over the original streamed create.

Only an assertion on the grain separates them, and it has to exist **before** the divergence rather
than after. That is the most direct argument for #12 this exercise produced, and it arrived without
anything breaking.

---

## 3. Negative lateness exists, and phase 1 said it did not

`40_ping_quality.sql` counts rows where `server_ts < device_ts` because a previous simulator bug
produced 169,480 of them. It was expected to return zero. It returned **10**:

```text
 vehicle_id | sequence_no |           device_ts           |           server_ts           | lateness
------------+-------------+-------------------------------+-------------------------------+----------
          1 |        6515 | 2026-08-10 15:29:25.815247+00 | 2026-08-10 15:29:25.089448+00 |  -0.7258
          2 |        6515 | 2026-08-10 15:29:25.815247+00 | 2026-08-10 15:29:25.089461+00 |  -0.7258
          3 |        6515 | 2026-08-10 15:29:25.815247+00 | 2026-08-10 15:29:25.089463+00 |  -0.7258
        ... all ten vehicles, one tick, identical device_ts
```

Ten rows out of 353,062 — **0.0028%**. All ten are the same tick: every vehicle in the fleet shares
one `device_ts` because the simulator stamps one timestamp per tick, and `server_ts` for all ten
landed 0.726 s *earlier*.

**Cause.** The container wall clock stepped backwards between the simulator computing the tick
timestamp and the API stamping `server_ts` — the WSL2 resync already measured at ~2.7 s every ~30 s
([source-system-reference §8](source-system-reference.md#8-continuous-mode-and-a-clock-that-cannot-be-trusted)).
The phase 1 baseline reported *"all positive, no negatives"*, and that was true: it measured
**backfill** mode, where `device_ts` is generated hours in the past and no clock step is large
enough to invert the sign.

**Why it matters beyond a curiosity.** The project's central claim is that `server_ts` is the
trustworthy timestamp. That needs qualifying: `server_ts` is trustworthy in the sense that *we*
produced it and no client can forge it — but it is not monotonic on this host. Any phase-3 lateness
policy that assumes `server_ts − device_ts >= 0`, or that derives a watermark from `max(server_ts)`
without a clamp, has a case it does not handle. `poller.py`'s `next_watermark` already clamps for
exactly this reason; the lateness work in #15 will need the same guard, and now has a measured
example to test against.

---

## 4. The loud break: rename a column upstream

Renamed `bronze.raw_ping_events.sequence_no` to `seq_no`.

### 4a. The loader reported success while the database diverged

Before touching the database, `sequence_no` was renamed in `load/schema.py` — the module that
declares it — and the loader was run:

```text
$ python -m fleet_telemetry.load.schema
applying bronze schema to postgresql://telemetry:***@127.0.0.1:55432/telemetry
bronze tables present: raw_ping_events, raw_cdc_entities, raw_job_events, poll_rows, poll_watermarks

$ select column_name from information_schema.columns
    where table_name = 'raw_ping_events' and column_name in ('sequence_no','seq_no');
 sequence_no
```

**Exit 0, "tables present", and the column was not renamed.** Every statement in that module is
`CREATE TABLE IF NOT EXISTS`, which is idempotent against **absence, not divergence** — the table
exists, so the whole definition is skipped. The file that claims to own the bronze schema and the
schema itself now disagree, and nothing in the project reports it. Same failure class as
[known-issues §1](known-issues.md), where four `REPLICA IDENTITY FULL` statements sat unexecuted
for five days.

### 4b. Then the real rename, and the run

```text
  00_rejected_rows.sql         ok      0.01s
  10_stg_pings.sql             FAILED

UndefinedColumn: column "sequence_no" does not exist
LINE 76:     sequence_no::bigint                             AS seque...
             ^

committed before the failure : ['00_rejected_rows.sql']
rolled back                  : 10_stg_pings.sql
never attempted              : ['20_stg_vehicles.sql', '21_stg_depots.sql', '22_stg_drivers.sql',
                                '23_stg_jobs.sql', '30_stg_job_events.sql', '40_ping_quality.sql',
                                '50_vehicle_day.sql']
```

Exit code 1. The error is clear, correctly located, and names the column. **That is not the
problem.** The problem is the state it leaves:

```text
 stg_pings | ping_quality | rejects | pings_counted |         silver_newest         | bronze_now
-----------+--------------+---------+---------------+-------------------------------+------------
    351742 |          110 |       0 |        351742 | 2026-08-13 13:08:39.836243+00 |     352102
```

Every table is present. Every table is queryable. `ping_quality` holds 110 rows summing to exactly
351,742 pings — **perfectly self-consistent with a `stg_pings` that is now 360 rows behind Bronze
and falling further behind every second.** No error is stored anywhere in the database. A dashboard
reading `vehicle_day` sees plausible numbers and no indication that the layer stopped updating.

One inconsistency was committed and left invisible: `00_rejected_rows.sql` succeeded, so its
`TRUNCATE` committed while `stg_pings` kept the previous run's rows. It happened to be harmless
because the reject table was already empty — but with any malformed row present, the rejects would
have been discarded while the rows they explain stayed in place.

### 4c. Finding what else referenced the column

There is no lineage, so the tool is `grep`:

```text
$ grep -rn "sequence_no" sql/
sql/silver/10_stg_pings.sql:76        <- the break
sql/silver/40_ping_quality.sql:24    <- a comment
sql/silver/40_ping_quality.sql:26    <- a comment
sql/silver/40_ping_quality.sql:27    <- a comment
sql/silver/40_ping_quality.sql:36    <- a comment
sql/silver/40_ping_quality.sql:50    <- silver_manual.stg_pings.sequence_no -- NOT affected
sql/silver/40_ping_quality.sql:52    <- same
sql/silver/40_ping_quality.sql:53    <- same
```

**Eight lines in `sql/`, and exactly one is the break.** Four are comments. Three read a *Silver*
column that happens to share the name and is unaffected, because `10_stg_pings.sql` aliases it. Grep
cannot tell layers apart.

Widen it and it gets worse — 13 files repo-wide, including `app/`, `simulator/` and
`docker/oltp/init.sql`, all referring to the **OLTP** column of the same name, which is a different
column in a different database. And one more that matters:

`dbt/models/staging/_sources.yml` still declared `sequence_no` on `raw_ping_events` throughout. That
declaration was false for the whole experiment, and nothing checked it — `dbt build` would only
notice once a model referenced the column.

A dependency graph would have named one node and its descendants. Reverted with an explicit `ALTER`
plus `git checkout`, then verified the database and `schema.py` agree again.

---

## 5. The silent break: change what a column means, not its name

Swapped the two arguments to `ST_MakePoint` in `10_stg_pings.sql` — longitude-first to
latitude-first. One token moved, in the upstream-most model.

```text
  00_rejected_rows.sql         ok      0.01s
  10_stg_pings.sql             ok      2.74s
  ... all nine ...
  50_vehicle_day.sql           ok      1.33s

runner exit code: 0
```

Nine of nine `ok`. Exit 0. Identical row counts in every table. Then:

| | before | after | change |
| --- | --- | --- | --- |
| Fleet total distance | 34,154.8 km | **34,885.9 km** | **+2.14%** |
| Mean per vehicle-day | 310.5 km | 317.1 km | +2.13% |
| `vehicle_day` rows | 110 | 110 | — |

Held constant on identical historical days, which is the controlled comparison — same `pings`, same
`distinct_positions`, different distance:

```text
 event_date | pings | distinct_positions | distance_metres (before -> after)
------------+-------+--------------------+----------------------------------
 2026-08-10 |  5325 |               4569 | 511,465.0 -> 522,367.7   (+2.13%)
 2026-08-11 |  3931 |               3465 | 595,558.9 -> 607,185.6   (+1.95%)
 2026-08-12 |  6184 |               5443 | 530,273.7 -> 539,874.5   (+1.81%)
```

**The 2% is the whole finding.** A 10× error gets caught in review. A 2% error ships, and then
somebody reconciles a report against it a month later. The reason it is 2% and not 10× is geometric:
The Gambia sits at ~13°N, ~16°W, and swapping the arguments mirrors it to ~16°S, ~13°E. Distances
*between nearby points* are nearly preserved across that mirror, so every aggregate stays plausible.

And the corruption is invisible in the model that contains it:

```text
 lat_min | lat_max | lon_min | lon_max      <- stg_pings, after the break
---------+---------+---------+---------
  13.271 |  13.567 | -16.682 | -14.217      <- still perfectly correct
```

The `latitude` and `longitude` columns are untouched and right. Only the derived `position`
geometry is wrong, and the only way to see it is to ask:

```text
 ST_AsText(position)
-----------------------------
 POINT(13.441566 -16.123891)     <- the South Atlantic
 POINT(-16.123891 13.441566)     <- The Gambia (after revert)
```

Nobody reads a geometry by eye. There is no test. There is no error. The number is wrong by an
amount chosen to be believable.

---

## 6. One more, free: a cast can take the whole layer down

`_sources.yml` promises that *"Silver casts, where a bad value is a failing test rather than lost
data"*. That promise assumes a framework that can fail one model and carry on. There isn't one here:
Postgres 16 has no `TRY_CAST`, so `'banana'::uuid` aborts the script, rolls its table back to the
previous run, and every script after it never runs — the §4b state, triggered by one row.

No defence was built against it, deliberately. Nothing can currently produce such a value: Debezium
renders every field in `after` from an already-typed OLTP column, so `ping_id` is a `uuid` and
`sequence_no` is a `bigint` before they are ever serialised. The `"banana"` in
`tests/test_bronze_load.py` is written directly to Bronze to prove Bronze *tolerates* it, which is a
different claim from Bronze *receiving* it.

Building the validation layer now would be defending against a value that cannot arrive — which is
the thing this whole phase argues against. Recorded instead.

---

## 7. What this actually buys dbt

Four absences, each now with evidence rather than an assertion.

| Absence | What it cost, measured |
| --- | --- |
| **No dependency graph** | Grep returned 8 lines in `sql/`; one was the break. `CREATE TABLE AS` registers no catalog dependency, so Postgres let `10`'s `DROP TABLE` succeed while `40` still held rows derived from it. Had these been views, the database itself would have refused |
| **No tests** | The lat/lon swap passed nine of nine scripts with exit 0 and moved a headline number 2.14%. The deduplication rule cannot be validated by any query against current data |
| **No lineage** | After the failure, no artifact anywhere records which tables are fresh and which are stale. `ping_quality` was self-consistent with a stale parent |
| **No state** | Every run rebuilds all 353k rows. Fine at 10 s; the target is ~22.2M rows over 30 days |

The naive layer is kept, not deleted — the same treatment `poller.py` got in phase 2. `silver_manual`
sits beside `silver`, the model names are identical, and so #12 inherits its own check for free:

```sql
SELECT * FROM silver_manual.stg_pings EXCEPT SELECT * FROM silver.stg_pings;
```

Where those disagree, one of the two is wrong.

---

## Numbers worth remembering

| | |
| --- | --- |
| Ground truth, closed window | 342,992 = 342,992, exactly |
| Rows the `ping_id` deduplication removed | **0** of 350,742 |
| Negative-lateness rows, continuous mode | 10 (0.0028%), one tick, −0.726 s |
| Loud break: scripts committed / rolled back / never run | 1 / 1 / 7 |
| Loud break: grep hits in `sql/` vs actual breaks | 8 vs **1** |
| Silent break: exit code | **0** |
| Silent break: change in fleet total distance | +2.14% (34,154.8 → 34,885.9 km) |
| Full layer rebuild, 353k rows | ~10 s |

---

## Explain-back

Answer from memory. If you cannot, this phase is not finished, however well the scripts run.

1. **Why could no query tell you whether the `ping_id` deduplication rule was correct?**
   Because Bronze held no duplicate `ping_id`s, so `DISTINCT ON` discarded nothing and the output was
   identical under a right rule, a wrong rule, and no rule. Only an assertion on the declared grain
   distinguishes them, and it has to exist before the data diverges.

2. **After a script fails halfway, which tables are stale — and what tells you?**
   Tables from scripts before the failure are fresh; the failing script's table rolled back to its
   *previous* contents; tables after it were never touched. Nothing tells you. Every one is present,
   queryable and self-consistent, which is why the runner prints the three lists — that output is
   the only record that exists.

3. **The lat/lon swap changed a headline number by 2%. Why is that worse than 10×?**
   Because 10× gets caught. The Gambia at 13°N mirrors to 16°S, and distances between nearby points
   are nearly preserved across the equator, so every aggregate stays believable. The columns the bug
   lives next to — `latitude`, `longitude` — stay correct; only the derived geometry is wrong.

4. **`server_ts` is the trustworthy timestamp. In what sense is it not?**
   No client can forge it, which is what "trustworthy" was claiming. But it is not monotonic: the
   host clock steps backwards, and 10 rows carry a `server_ts` earlier than their own `device_ts`.
   Anything assuming lateness ≥ 0, or taking a watermark from `max(server_ts)` without a clamp, has
   an unhandled case.

---

## Next

- The ideas: `docs/learn/03-transformation.md` (issue #16, not written yet)
- The same logic in dbt, with the tests this layer lacks: issue #12
- Defects found along the way: [`known-issues.md`](known-issues.md)
