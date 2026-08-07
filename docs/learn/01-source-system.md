# Phase 1 — Understand the source system

**Question:** What data exists, what shape is it, and how often does it arrive?

You cannot ingest what you have not characterised. Every decision in the next four phases —
how often to poll, what the deduplication key is, how late data is allowed to be, whether
partitioning is worth it — is set by numbers you measure here. Guess any of them and you rebuild
later.

---

## The inversion

You build the source system, then study it **as if you had never seen it** — profiling it the way
you would profile a third-party API you did not write and cannot ask questions about.

That feels pointless and is not, for two reasons:

- Source-system analysis is a real job, and this is the only time you can **check your findings
  against the truth**. You know what the simulator intended, so a profile that disagrees means
  your measurement is wrong, not the data.
- It builds the reflex the previous version of this project died without. That one assumed there
  would be aircraft over conflict zones. There were zero. Nobody checked until months of design
  had been built on the assumption.

---

## What you build

| Component | Path | What it is |
| --- | --- | --- |
| OLTP schema | [docker/oltp/init.sql](../../docker/oltp/init.sql) | Six tables. Read the comments — they explain every choice |
| The application | [app/](../../app/) | Seven FastAPI endpoints. Scaffolding, not a product |
| The simulator | [simulator/](../../simulator/) | Moves vehicles around The Gambia and posts what they'd report |
| The profiler | [profile_source.py](../../src/fleet_telemetry/profile_source.py) | **The actual deliverable.** Measures everything |

Output: [docs/source-system-reference.md](../source-system-reference.md) — every number in it
produced by a query.

---

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d oltp

# The override is what lets the simulator write historical timestamps. See below.
FLEET_ALLOW_SERVER_TS_OVERRIDE=true uvicorn app.main:app --port 8000

python -m simulator --vehicles 40 --hours 6
python -m fleet_telemetry.profile_source
```

`http://127.0.0.1:8000/docs` gives you a browser for the API. Poke at it before reading further —
create a job, try to move it straight to `delivered`, watch it refuse.

Tests:

```bash
pytest -m "not integration"   # pure logic, no docker
pytest                         # everything, needs the OLTP container
```

---

## The six ideas in this phase

### 1. OLTP and OLAP want opposite things

The source database is **normalised** and stores **only the current state**. That is right for an
application: it answers "where is vehicle 12 now" in a millisecond and never duplicates a fact.

It is useless for analysis. Change a driver's home depot and the old value is *gone* — not
archived, gone. So "how many trips ran out of Brikama last March" cannot be answered from this
database at all, and no amount of clever SQL fixes it. The information is not there.

That single fact is why a warehouse exists, why Phase 2 captures a change stream, and why Phase 3
builds Type 2 dimensions. Everything downstream is a response to this.

### 2. Event time is not processing time

Every ping has two timestamps, and **one of them lies**:

```text
device_ts   when the device says it took the reading   -- NOT trustworthy
server_ts   when our API received it                    -- trustworthy
```

`device_ts` is client-controlled. A phone with a wrong clock reports a wrong time, and no
validation can detect it, because there is nothing implausible about "12:04:31".

`server_ts − device_ts` is the **lateness**, and measured over this run:

| min | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- |
| 1 s | 34 s | 63 s | 67 s | 68 s |

Note the shape. A device buffers 500 readings before uploading, which covers ~62 s of real time,
then adds 1–8 s of transmission. So the *oldest* reading in each batch is ~68 s late and the
newest ~1 s, spread roughly evenly between. **The median is half a batch.**

That number is not trivia — it is the floor for Phase 3's watermark. A lateness bound below 68 s
would quarantine correct data in the *clean* case, before any pathology exists.

### 3. Idempotency needs a key the client controls

Networks fail after the server has already committed. The device sees a timeout, retries, and now
the same reading arrives twice. This is not an edge case; it is normal.

The fix is `ping_id` — a UUID the **device** generates and **reuses on retry**:

```sql
insert into pings (...) values (...)
on conflict (ping_id) do nothing
```

Now a retry is exactly a no-op. The endpoint reports duplicates rather than hiding them, because
the duplicate rate tells you something about network conditions.

Note what would not work: deduplicating on `(vehicle_id, device_ts)` looks equivalent and is not.
Two genuinely distinct readings can share a timestamp when a clock is coarse or stuck, and you
would silently drop real data.

### 4. A gap is not the same as a stop

`sequence_no` counts up per vehicle, and it is not redundant with the timestamps:

> **A gap in the sequence proves data was lost. An unchanging position with no gap proves the
> vehicle stopped.**

Without it, a vehicle that loses signal for ten minutes and a vehicle parked for ten minutes
produce identical data. Conflating them makes every traffic count understate reality exactly when
the network is worst — and the error points in one direction, so it does not average out.

Measured this run: **0 gaps, 0 repeats.** That clean baseline is the whole point. When Phase 3
injects gaps deliberately, you will know they are the injection.

### 5. The database cannot tell you what it has forgotten

The single most instructive measurement in this phase is a disagreement.

- The simulator performed **12** vehicle reassignments.
- The database reports **8** changed rows.

Both are right. Four vehicles were reassigned twice, and the second change overwrote the first.
The intermediate depots are gone, and **nothing in the database records that they ever existed** —
so no query can reveal the shortfall. It is visible only because the simulator kept its own count,
which in real life you would not have.

Sit with that, because it justifies the next two phases at a stroke:

- A Type 2 dimension needs all **12** versions to answer "which depot was this vehicle attached to
  on the 14th". The OLTP can supply **8** and cannot tell you it is short.
- A batch poller (`WHERE updated_at > watermark`) finds the same 8, by construction: two changes
  between two polls are indistinguishable from one. Polling faster shrinks the window and never
  closes it.
- Reading the write-ahead log catches all 12, because a log records *transitions* rather than
  current state.

You will rediscover this concretely in Phase 2. Having measured it here is what makes that
rediscovery a confirmation rather than a surprise.

### 6. Reject the impossible, record the merely suspicious

Two kinds of bad input, two opposite policies:

| Input | Policy | Why |
| --- | --- | --- |
| `latitude: 900` | **Reject** (422) | Physically impossible, cannot be corrected later, would poison every aggregate |
| `device_ts` three hours in the future | **Accept and store** | Evidence that *this device's clock is wrong* |

Rejecting the second would destroy the only signal that the device is misbehaving. The warehouse
can then say "vehicle 12's clock runs four minutes fast" instead of "vehicle 12 went quiet".

---

## Two bugs the profiler found in the simulator

Both would have been invisible without measuring, and both were in code that looked correct.

**Negative lateness on 169,480 rows.** `server_ts` landed *before* `device_ts` — a median of
−29 s. The flush routine stamped each batch with the *previous* batch's timestamp, backdating
every row by roughly one batch. A batch cannot arrive before its own newest reading was taken.
Fixed by deriving `server_ts` from the newest reading in the buffer.

**7,201 duplicate `(vehicle_id, sequence_no)` pairs.** Every simulator run restarted each
vehicle's counter at zero, so the second run collided with the first. A real device's counter
persists — it does not rewind because a process restarted. Fixed by resuming from the database,
with `--reset` for a genuinely clean start.

That second one would have quietly destroyed Phase 3: a counter that rewinds makes sequence gaps
meaningless as a loss signal.

**This is the lesson of the phase.** Two bugs, in code I had just written and believed was
correct, found in seconds by a query. `docs/learn` cannot teach you this; running the profiler
can.

---

## Why the server_ts override is guarded

Generating a month of history in minutes means writing rows whose `server_ts` is in the past —
which a correct server would never allow, because `server_ts` being *ours* is the entire reason
it can be trusted.

So there is an override, and it is refused unless `FLEET_ALLOW_SERVER_TS_OVERRIDE=true`:

```bash
curl -X POST .../pings -d '{"server_ts_override": "2020-01-01T00:00:00Z", ...}'
# 403: server_ts_override requires FLEET_ALLOW_SERVER_TS_OVERRIDE=true
```

This is a real pattern, not a shortcut — production systems do have privileged backfill paths.
The rule is that they are **explicit, off by default, and impossible to trigger by accident**. A
silent override would quietly destroy the only trustworthy timestamp in the system.

`--live` avoids the whole question: real clock, no override, no special configuration. Lower
volume, but nothing is pretending.

---

## Numbers worth remembering

| Measure | Value |
| --- | --- |
| Ping rate | 617 per vehicle-hour (720 theoretical; staggered starts explain the gap) |
| Target scale | ~22.2M rows for 100 vehicles × 30 days, ~4.8 GB, ~82 min to generate |
| Write throughput | 4,500 pings/sec, batches of 500 over HTTP |
| Index overhead | **52%** of the `pings` table |
| Lateness p99 | 67 s (clean baseline) |

Full detail: [docs/source-system-reference.md](../source-system-reference.md).

---

## Explain-back

Answer from memory. If you cannot, the phase is not finished, however well the code runs.

1. **Why do pings carry three timestamps? Which can you trust, and why not the others?**
2. **What does a gap in `sequence_no` prove that a repeated position does not?**
3. **Why is `latitude: 900` rejected but a device clock three hours fast accepted?**
4. **Your OLTP has 22 million ping rows and full history of every driver change. True or false?**
   (False. It has the pings. It has *no* history of driver changes — only current values.)
5. **Why is deduplicating on `ping_id` correct where `(vehicle_id, device_ts)` is not?**
6. **The simulator made 12 vehicle changes and the database shows 8. Where did the other four
   go, and what query would find them?** (Nowhere, and none. That is the point.)

---

## Next

**Phase 2 — Ingestion** (`02-ingestion.md`, not written yet): getting this data out without losing or duplicating any
of it. You will build a batch poller first, watch it fail to notice a deleted vehicle, and only
then find out what change data capture is for.
