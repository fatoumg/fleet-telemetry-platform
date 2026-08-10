# Source system reference

What the fleet application actually produces. **Every number here came from a query**, not from
the schema and not from the simulator's configuration.

There are **two ways to produce data, and their timestamp profiles differ by four orders of
magnitude.** Almost everything below was measured in backfill mode; §9 covers continuous mode and
says explicitly which figures change.

**Backfill** — a window of history, generated flat out. Needs the privileged `server_ts` override,
because a correct server would stamp every row with *now*.

```bash
docker compose -f docker/docker-compose.yml up -d oltp
FLEET_ALLOW_SERVER_TS_OVERRIDE=true uvicorn app.main:app --port 8000
python -m simulator --vehicles 40 --hours 6
python -m fleet_telemetry.profile_source
```

**Continuous** — real speed, indefinitely. What `docker compose up -d` runs, and what a real fleet
would look like. No override, no special configuration.

```bash
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml logs -f simulator
```

| | Baseline run (§1–§8) | Continuous run (§9) |
| --- | --- | --- |
| Measured | 2026-08-07 | 2026-08-10 |
| Vehicles | 40 | 10 (the compose default) |
| Span | 6 simulated hours | ongoing |
| Rows | 172,800 | 550 in the sampled window |
| Seed | 42 | 42 |

The compose default is **10 vehicles**, not the 40 this baseline used — 40 writes ~690,000 rows a
day and over a gigabyte a week, which is not something to leave running unattended. Set
`SIM_VEHICLES=40` when throughput is the thing under test. Per-ping *shape* is identical either
way; only volume figures scale.

---

## 1. What the source system is

A normalised Postgres 16 database behind a seven-endpoint FastAPI application. Six tables, two
of them mutable, two append-only.

The important structural fact, and the reason the rest of this platform exists:

> **The OLTP keeps only the current state.** Change a driver's home depot and the previous value
> is gone. Not archived — gone. `drivers` has no history column and no history table.

That is correct behaviour for an application. It is also why "how many trips ran out of Brikama
last March" is unanswerable here, and why Phase 3 has to reconstruct history from the change
stream rather than query it.

| Table | Mutability | Rows (this run) | Role |
| --- | --- | --- | --- |
| `depots` | reference | 8 | Real Gambian towns, Banjul to Basse Santa Su |
| `drivers` | **mutable** | 40 | Overwrites on update → SCD Type 2 material |
| `vehicles` | **mutable** | 40 | Overwrites on update → SCD Type 2 material |
| `jobs` | mutable status | 143 | A state machine |
| `job_events` | append-only | 535 | How each job reached its status |
| `pings` | append-only | 172,800 | The high-volume table |

Row counts are this baseline run's. The database grows continuously now, so live counts will be
higher — `python -m fleet_telemetry.profile_source` reports current figures.

---

## 2. Volume

| Table | Rows | Total | Heap | Indexes | Bytes/row |
| --- | --- | --- | --- | --- | --- |
| `pings` | 172,800 | 37.4 MB | 18.0 MB | 19.4 MB | 227 |
| `job_events` | 535 | 176 KB | 48 KB | 128 KB | 337 |
| `jobs` | 143 | 104 KB | 16 KB | 88 KB | 745 |
| others | ≤40 | 48–64 KB | 8 KB | 40–56 KB | — |

**Indexes are 52% of the `pings` table.** Three indexes on a nine-column table roughly doubles
its footprint. They are not free, and each one also slows every insert. `pings` carries three
because each answers a different question the pipeline will ask — arrival order, per-vehicle
sequence, per-vehicle event time — but that is a deliberate trade, not a default.

### Rate

| Measure | Value |
| --- | --- |
| Overall | 28,807 pings/hour across 40 vehicles |
| Per vehicle | 617 pings/vehicle-hour (min 105, max 720) |
| Implied daily | ~7,405 rows per vehicle per 12-hour day |
| **Implied at target scale** | **~22.2M rows** for 100 vehicles over 30 days |

The theoretical maximum at one ping per five seconds is 720/hour. The observed mean is 617
because vehicles are staggered — a vehicle that starts mid-hour contributes a partial hour, which
is why the minimum is 105.

Continuous mode measured **2.24 pings/sec at 10 vehicles**, against a nominal 2.0. The excess is
the wall-clock stepping described in §8, which shortens some intervals and so squeezes in slightly
more readings than the interval implies. Scaled to 40 vehicles that is 8 pings/sec, ~690,000 rows
and ~170 MB a day.

The 22.2M projection is close to the spec's 26M estimate, which used the theoretical 720 rather
than the measured 617. **The measurement is the one to trust.**

### Write throughput, and what the target run costs

Sustained **4,500 pings/sec** through the API, in batches of 500 over HTTP. So:

| Target | Rows | Wall clock |
| --- | --- | --- |
| This run (40 vehicles, 6 h) | 172,800 | 38 s |
| 100 vehicles, 30 days | ~22.2M | **~82 minutes** |

That answers the spec's open item: 22M rows is feasible but is not something to do casually. It
would also be about 4.8 GB with indexes. Options if that proves painful: fewer vehicles, a
longer ping interval, or `COPY` instead of `executemany`.

### Dead tuples

The profiler reports `dead` rows alongside live ones because Postgres does not reclaim space on
delete or update — the old row version sits there until `VACUUM` runs. An earlier run showed
`pings` at 485 bytes/row against this run's 227, purely because a `--reset` had deleted 172,800
rows whose space had not yet been returned. Size on disk overstating the data by 2× is normal and
not a leak.

---

## 3. Timestamps — the part that matters most

Three timestamps exist, and **only one is trustworthy**.

| Column | Set by | Trustworthy? |
| --- | --- | --- |
| `device_ts` | The device's own clock | **No.** Phone clocks drift and can be badly wrong |
| `server_ts` | The API on receipt | Yes — one clock, ours |
| `created_at` / `updated_at` | Postgres, via trigger | Yes |

### Measured lateness (`server_ts − device_ts`, seconds)

| min | p50 | p95 | p99 | max | mean |
| --- | --- | --- | --- | --- | --- |
| 1.0 | 34.0 | 63.0 | 67.0 | 68.0 | 34.4 |

All positive, no negatives — the clean Phase 1 baseline.

The shape is a direct consequence of batching. A device buffers 500 readings before uploading,
which at 40 vehicles pinging every 5 s covers about 62 seconds of real time, and then adds 1–8 s
of transmission delay. So the **oldest** reading in a batch is ~68 s late and the newest is ~1 s
late, roughly uniformly distributed between. The median of 34 s is half a batch.

**This is the number Phase 3's watermark is built on.** A lateness bound below 68 s would already
quarantine correct data in the clean case — before any pathology is injected.

> **Percentiles, not the mean.** Once devices start reconnecting after outages the distribution
> develops a long right tail, and a mean over that tail describes nobody. The p99 and max are
> what a watermark actually has to survive.

### The same system, two lateness distributions

Continuous mode produces the same rows through the same endpoint, and its lateness is **four
orders of magnitude smaller**:

| Mode | p50 | p99 | max | What the number is |
| --- | --- | --- | --- | --- |
| Backfill | 34.0 s | 67.0 s | 68.0 s | batch buffering + a synthetic 1–8 s delay |
| Continuous | 0.0031 s | 0.0090 s | 0.0090 s | real localhost round trip |

Neither is wrong, and that is the point: **lateness is a property of how data arrives, not of the
schema.** A watermark tuned on one of these is badly wrong for the other — too tight and continuous
mode's correct data gets quarantined; too loose and every backfilled aggregate waits a minute for
data that came in three milliseconds.

Phase 3 has to state which arrival regime it assumes, and Phase 2 has to avoid mixing them in one
window. Mixing them is easy to do by accident: while measuring for this document, a two-minute
`--live` backfill of 72 rows landed inside a 3,822-row continuous sample. Those 72 rows were 1.9%
of the sample and therefore sat exactly on the p99 — dragging it from 0.009 s to **59 s**. The
p50 barely moved. A single stray backfill is enough to make a percentile lie.

---

## 4. Sequence integrity

| Measure | Value |
| --- | --- |
| Gaps (`step > 1`) | **0** |
| Repeats (`step = 0`) | **0** |

Every vehicle's `sequence_no` is complete and strictly increasing. Recording this now is what
makes Phase 3's injected gaps provably the injection rather than a pre-existing bug.

Why the column exists at all:

> **A gap in the sequence proves data was lost. An unchanging position with no gap proves the
> vehicle stopped.** Timestamps alone cannot separate those two, and conflating them makes every
> traffic count silently understate reality whenever a device drops out.

---

## 5. Nullability, as measured

Declared nullability and observed nullability are different questions, and the gap between them
is a trap.

| Column | Declared | Observed null | Note |
| --- | --- | --- | --- |
| `pings.speed_kmh` | nullable | **0%** | Never null *here*. Do not build a join on that |
| `pings.heading_deg` | nullable | **0%** | Same |
| `jobs.vehicle_id` | nullable | 0% | Null only while a job is `created` but unassigned; this run assigned every job immediately |
| `vehicles.current_driver_id` | nullable | 0% | Seeded 1:1; a real fleet would have gaps |
| `drivers.phone` | nullable | 0% | Seeded for every driver |

**A nullable column that happens to be full today is a promise nobody made.** The simulator
currently populates speed and heading on every ping; a real device with a poor GPS fix would not.
Silver must handle the null, even though Phase 1 never sees one.

---

## 6. Jobs

Status distribution after 6 hours (jobs in flight at the cut-off are still open):

| Status | Count | Share |
| --- | --- | --- |
| `delivered` | 106 | 74.1% |
| `picked_up` | 37 | 25.9% |

Transitions observed:

```text
    (new) -> created      143
  created -> assigned     143
 assigned -> picked_up    143
picked_up -> delivered    106
```

The funnel narrows because jobs still in progress when the window closed have not been delivered
yet. **That is right-censoring** — the delivery event lies beyond the end of observation, so its
duration is known only as "at least this long".

It matters downstream: averaging delivery time over completed jobs alone systematically
understates it, because the jobs still open are disproportionately the slow ones. Any duration
metric has to state how it treats them.

The state machine is enforced in the application, not the database. `created → delivered` returns
`409 Conflict`, and terminal states reject everything.

---

## 7. Entity churn — the SCD Type 2 workload

| Table | Rows differing from creation | Of |
| --- | --- | --- |
| `vehicles` | **8** | 40 |
| `drivers` | 0 | 40 |

### The most important number in this document is one the database cannot show you

The simulator performed **12** vehicle reassignments. The database reports **8** changed rows.

Both are correct. Four vehicles were reassigned more than once, and each later change simply
overwrote the earlier one. The intermediate depots are gone — not archived, not recoverable,
gone — and **nothing in this database records that they ever existed.** Querying the OLTP can
never reveal the discrepancy; it is only visible because the simulator kept its own count.

That gap is the entire argument for Phase 2 and Phase 3 in one measurement:

- A Type 2 dimension needs all **12** versions to answer "which depot was this vehicle attached
  to on the 14th". The OLTP can supply at most **8**, and cannot tell you it is short.
- A batch poller (`WHERE updated_at > watermark`) would find the same 8. Two changes between two
  polls are indistinguishable from one, by construction. Polling faster narrows the window but
  never closes it.
- Reading the write-ahead log catches every committed change, because the log records the
  *transitions* rather than the current state.

Extrapolated to the target scale, 30 days would produce roughly 2,000 changes — with an unknown
and unknowable fraction invisible to any polling strategy.

---

## 8. Continuous mode, and a clock that cannot be trusted

Measured 2026-08-10: 10 vehicles, 5 s interval, 550 rows, seed 42.

| Measure | Value | Against expectation |
| --- | --- | --- |
| Throughput | 2.24 pings/sec | 2.0 nominal (10 ÷ 5 s) |
| Lateness p50 | 0.0031 s | real round trip, no synthetic delay |
| Negative lateness | **0** | invariant holds |
| Sequence gaps / repeats | **0 / 0** | clean |
| `server_ts` inversions | **0** of 120 | see below |

### The wall clock steps backwards, and it broke the simulator

The tick interval measured **mean 4.52 s** against a nominal 5.00, with 4 excursions in 23 gaps —
all of them *short* (min 2.20 s), never long. Two independent measurements explain it.

Directly, inside the simulator's own container:

```text
wall clock stepped -2.6768s relative to monotonic
worst wall-vs-monotonic divergence over 30s: 2.6768s
```

The container's wall clock jumps **backwards ~2.7 s roughly every 30 s** — WSL2 resyncing its VM
clock against the Windows host. And the arithmetic closes exactly: 4 steps × 2.8 s shortfall ÷ 23
gaps = 0.487 s, and 5.00 − 0.487 = **4.51**, against 4.52 observed.

This surfaced a real bug. The `--forever` loop originally computed its sleep from
`datetime.now()`, so a backward step made one tick fire ~2.8 s early and the next ~2.8 s late —
**pairs of gaps summing to exactly two intervals**, which is what the `pings` table showed. The fix
is the standard one, now enforced by a regression test whose signature refuses a `datetime`:

> **Pace on `time.monotonic()`. Timestamp on the wall clock.** Monotonic only ever moves forward
> at one second per second. A device timestamp is a real point in time and must stay wall-clock —
> but nothing that *schedules* may do arithmetic on it.

After the fix, the compensating long gaps disappeared and the maximum gap fell from 7.82 s to
5.07 s. The remaining short gaps are the wall clock itself, faithfully recorded.

### Why this matters well beyond the simulator

**`device_ts` is untrustworthy even when we own the device.** This document already said phone
clocks drift. It turns out our own container, on our own machine, with no phone involved, steps its
clock by seconds. The distrust is not a hypothetical about cheap hardware — it is measured here.

**`server_ts` is only monotonic by luck at this interval.** It survived: 0 inversions in 120
consecutive readings, minimum step +2.199 s. But that is because the 2.7 s step is *smaller than
the 5 s ping interval*. **Inferred, not measured:** at a 1 s interval a 2.7 s backward step would
put `server_ts` out of order with respect to insertion order, and any Phase 2 watermark advancing
on `max(server_ts)` would then skip rows permanently. Worth testing deliberately before trusting
`server_ts` ordering at sub-step intervals.

**Phase 4 inherits this.** Airflow schedules on wall-clock time by design. A host that steps its
clock is exactly how a scheduled interval gets skipped or run twice.

---

## 9. What this implies downstream

1. **The watermark floor is 68 s in backfill and 9 ms in continuous mode** — measured, before any
   pathology exists. Phase 3's `lateness_bound_hours` has to clear the real distribution of the
   regime it is running against, and a window must not mix the two (§3).
2. **`device_ts` cannot be the event time without correction.** It is client-controlled, and §8
   shows it is unreliable even when we own the client: our own container steps its wall clock
   backwards by 2.7 s. Phase 3 estimates per-device skew; Silver must not silently trust the raw
   value.
3. **`ping_id` is the deduplication key.** It is client-generated and stable across retries,
   which makes ingestion exactly idempotent rather than approximately.
4. **22M rows is the realistic target**, ~4.8 GB with indexes, ~82 minutes to generate. Large
   enough that partitioning matters; small enough for a laptop.
5. **Deletes exist and leave nothing behind.** `DELETE /vehicles/{id}` is a hard delete, on
   purpose: Phase 2's batch poller finds changes via `updated_at`, and a deleted row has no
   `updated_at` to find. That is the cleanest demonstration of why CDC exists.
6. **Right-censoring is already present.** Any duration metric must state how it treats jobs that
   were still open at the window boundary — see §6.
7. **Pace on a monotonic clock, timestamp on the wall clock.** Measured, not theoretical: the
   simulator's own container steps its wall clock backwards ~2.7 s every ~30 s, and pacing on
   wall-clock arithmetic was a real bug in this repo. Anything that schedules work — the simulator
   here, Airflow in phase 4 — must not do interval arithmetic on wall-clock time. See §8.
8. **`server_ts` ordering is not guaranteed at short intervals.** It held here (0 inversions in
   120 readings) only because the clock step is smaller than the 5 s ping interval. A watermark
   advancing on `max(server_ts)` should be tested against a deliberate clock step before it is
   trusted.
