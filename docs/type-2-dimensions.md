# Type 2 dimensions

What a CDC-built history captures that a periodic snapshot of current state cannot, measured
against the two paths this project now builds side by side. The deliverable of
[issue #13](https://github.com/fatoumg/fleet-telemetry-platform/issues/13): `gold.dim_vehicle`
and `gold.dim_driver`, built from `stg_*_versions` in dbt's models; `gold.snap_vehicles` and
`gold.snap_drivers`, built by `dbt snapshot` polling the same current-state tables the
batch poller reads.

Every number here came from a query against a running system on 2026-08-25. Nothing is estimated.

---

## 1. The result, in one measured sentence

Reassign vehicle 20 twice, 80 milliseconds apart, and CDC's `dim_vehicle` records all three depots
it passed through — 5, then 2, then 8 — while the polling-based `snap_vehicles` records only the
first and the last, because depot 2 was already gone from current state before anything could poll
it.

---

## 2. The window, and why the decisive case had to be constructed

**Poll times** (each a `dbt snapshot` run against `gold.snap_vehicles` / `gold.snap_drivers`):

| # | Timestamp | Note |
| --- | --- | --- |
| 1 | 2026-08-24T18:01:55Z | |
| 2 | 2026-08-24T18:17:13Z | 0 changes |
| 3 | 2026-08-25T10:00:39Z | |
| 4 | 2026-08-25T10:10:47Z | |

**The decisive case was constructed, not observed.** The simulator reassigns a vehicle to a new
depot roughly twice a day across the whole fleet — measured at ~8 vehicle changes over the 16
hours this window spans, each to a *different* vehicle. Waiting for one vehicle to be reassigned
twice between two consecutive snapshot runs by chance, at that rate, is luck, not method: the
expected number of vehicles hit twice in one ~10-hour gap is close to zero. So the case the
acceptance criteria actually name — one vehicle, two reassignments, one snapshot interval — was
built deliberately, through the API, the same rule the simulator itself follows (CLAUDE.md: *"The
simulator goes through the API, never the database"*):

```text
PATCH /vehicles/20   home_depot_id -> 2
PATCH /vehicles/20   home_depot_id -> 8      (80 ms later)
```

at 2026-08-25T10:06:13Z — between poll 3 and poll 4 above. The OLTP's `vehicles` table showed only
depot 8 immediately after; depot 2 existed in current state for less time than it takes to issue a
third query against it.

---

## 3. What the snapshot missed, and what it asserted instead

`gold.dim_vehicle`, vehicle 20:

| home_depot_id | valid_from | valid_to | is_current | source_op |
| --- | --- | --- | --- | --- |
| 5 | 2026-08-12 16:42:30.878+00 | 2026-08-25 10:06:13.874+00 | f | r |
| 2 | 2026-08-25 10:06:13.874+00 | 2026-08-25 10:06:13.895+00 | f | u |
| 8 | 2026-08-25 10:06:13.895+00 | *(null)* | t | u |

`gold.snap_vehicles`, vehicle 20:

| home_depot_id | dbt_valid_from | dbt_valid_to |
| --- | --- | --- |
| 5 | 2026-08-07 13:06:29.666059+00 | 2026-08-25 10:06:13.893619+00 |
| 8 | 2026-08-25 10:06:13.893619+00 | *(null)* |

Three CDC versions against two snapshot rows. One version — depot 2, real for 21 milliseconds —
is missing from the snapshot entirely.

That undercount is the easy way to describe it, and it understates what actually happened. The
snapshot does not merely have a gap where depot 2 should be; it **asserts something false** in the
row it does have. Reading `snap_vehicles` alone, vehicle 20 was at depot 5 continuously until
2026-08-25 10:06:13.893 — which is a specific, checkable claim, and it is wrong for the final 21
milliseconds of that span, during which the vehicle was actually at depot 2. A snapshot interval
does not fail open into "unknown here"; it fails closed into "definitely still the previous value,"
and that closed failure is indistinguishable from truth to anything reading it.

---

## 4. What polling cannot do at all, not even with a shorter interval

Four vehicles appear in `gold.dim_vehicle` that `gold.snap_vehicles` has never recorded, in any
version, at any poll: `snap_vehicles` has no row for them at all. Fifteen `dim_vehicle` versions
carry `is_deleted = true`. `DELETE /vehicles/{id}` is a hard delete on purpose (CLAUDE.md), and a
deleted row leaves nothing behind for a poller — or a `dbt snapshot` — to find, no matter how
tight the interval. Shortening the poll from ten minutes to ten seconds would not have found these
four vehicles; they were never there to find. This is the same lesson phase 2's poller-vs-CDC
diff (`docs/silver-in-dbt.md`, and the phase-2 `compare.py` it inherits) already taught, now
visible one layer up, in a dimension instead of a current-state table.

---

## 5. The aggregate totals — and why the obvious subtraction is not the number

| | CDC (`dim_*`) | polling (`snap_*`) |
| --- | --- | --- |
| vehicles | **123** versions | **51** rows |
| drivers | **82** versions | **40** rows |

Fifteen vehicles show more CDC versions than snapshot rows. It is tempting to subtract — 123 − 51 =
72 — and report "72 versions lost to polling." **That number is not defensible, and reporting it
would be exactly the kind of overstatement this project exists to avoid.** The two counts are not
measuring the same window: CDC's history for these dimensions reaches back to the Debezium
connector's initial snapshot on 2026-08-12, while the polling snapshot only began running on
2026-08-24T18:01:55Z. Most of the gap between 123 and 51 is "the snapshot was not running yet," not
"the snapshot ran and missed a change." A poller that had been running since 2026-08-12 might well
have caught most of those 72 versions — reassignments that happen to land cleanly between two poll
times are visible to polling just fine. The two measurements this document stands behind instead
are the ones with a controlled or structurally-provable cause:

- **The vehicle-20 case (§3):** 3 CDC versions vs. 2 snapshot rows, over a window both paths were
  actually running for. This is the case the acceptance criteria name, and it is real.
- **The four deleted vehicles (§4):** structurally invisible to any poller, regardless of interval
  or how long it had been running.

Everything else in the 123-vs-51 gap is a measurement artifact of when the two data collection
methods started, not a finding about what polling can or cannot see.

---

## 6. What this number is NOT

`docs/source-system-reference.md:259` reports a phase-1 figure of **12 reassignments performed
against 8 changed rows** — four intermediate depot assignments that current state had already
overwritten by the time anyone queried it. That figure predicted the shape of exactly this effect,
and it was a good prediction: it is why this ticket exists. **It cannot be reproduced by this
measurement, and should not be treated as an expected result.** It was measured before the
Debezium connector existed. CDC's version history begins at the connector's initial snapshot
(2026-08-12); any reassignment that happened before that date, including whichever ones produced
the 12-vs-8 figure, left no trace in Bronze and never will. The 12-vs-8 number and the 3-vs-2
number in §3 are two different, non-comparable measurements of the same underlying idea, taken with
different tools at different times. Citing the phase-1 number as this ticket's result would mean
reporting a figure this codebase cannot check against ground truth — the standard CLAUDE.md holds
every number in this project to.

---

## 7. The three fabrications the dimension admits to

A Type 2 dimension makes claims about history. Three of those claims, on this volume, are not
directly observed — they are filled in, and the dimension says so rather than hiding it.

1. **The 40 `op='r'` `valid_from`s.** Forty of `dim_vehicle`'s 123 versions (and the equivalent
   share of `dim_driver`'s 82) begin at the Debezium connector's initial snapshot rather than at an
   observed change — `valid_from_is_observation_start` is true on all forty. That `valid_from` is
   fabricated in a specific sense: it records when CDC started watching the row, not when the row
   actually acquired the attributes it has. Vehicle 20's own first version makes the point
   concretely — CDC's `dim_vehicle` says `valid_from` 2026-08-12 (the connector's snapshot date);
   the polling snapshot, sourced from the row's real `updated_at`, says 2026-08-07. On this one
   point the *polling* path is more honest about when the row's current values began, and CDC is
   only honest about when observation began. Neither is wrong; they answer different questions,
   which is exactly what `valid_from_is_observation_start` exists to make visible instead of
   quietly picking one answer and calling it history.

2. **The before-images the design refuses to read.** Every version's `valid_from` and `valid_to`
   are derived from after-images and a window function over `source_ts_ms` — never from a
   before-image body. Only a delete needs the before-image at all, and it contributes only its
   commit timestamp, never its attribute values. That is a deliberate design boundary (see
   `CLAUDE.md`'s new Key Pattern), and it is also what keeps `known-issues.md` §1's fabricated
   before-images — plate `""`, capacity 0, `created_at` 1970-01-01 — out of Gold entirely: this
   dimension never asks the question whose answer would be fabricated.

3. **`relreplident` on this volume.** Measured directly:

   ```sql
   select relname, relreplident from pg_class
    where relname in ('depots','drivers','vehicles','jobs');
   ```

   returns `'f'` (FULL) for all four — the `ALTER TABLE ... REPLICA IDENTITY FULL` statements in
   `docker/oltp/init.sql` did execute on this volume. So `known-issues.md` §1 does **not** apply
   here: before-images are real on this host, not Debezium-fabricated type defaults. That measured
   fact does not change anything about how this dimension is built, which is the point of listing
   it as a fabrication risk rather than a bug: the design in §7.2 above reads no before-image body
   regardless of whether `relreplident` is `'f'` or `'d'`, so the dimension stays correct on a
   volume where §1 does apply, and in CI, where it also does not run the `ALTER`s.

---

## 8. What CI proves about any of this: nothing

CI's Postgres service starts from an empty volume with `bronze` created but no data ever loaded
into it, so every SCD2 assertion in this ticket — the current-version cardinality check, the
zero-width-interval check, the Kafka-coordinate grain check — passes over **zero rows**. `lead()`
over an empty partition returns nothing rather than erroring, and a query that returns nothing
satisfies a `HAVING count(*) > 1` filter vacuously. Every test in this ticket is green in CI, and
CI has never once evaluated the logic those tests exist to check. Said out loud because a green
tick reads like coverage: `dbt build` passing in CI proves the SQL compiles and executes without a
Postgres error. It proves nothing about the SCD2 rules being correct, because CI has never seen a
vehicle change depot.

---

## Next

- Defects found along the way: [`known-issues.md`](known-issues.md), including a new entry for
  20,530 pings permanently lost while the CDC consumer was down — measured while producing this
  document, unrelated to the dimensions above but found in the same volume.
- The same "one layer up" argument silver already made for current-state tables:
  [`silver-in-dbt.md`](silver-in-dbt.md).
