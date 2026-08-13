# Phase 2 — code tour

[`02-ingestion.md`](02-ingestion.md) is the **ideas**: watermarks, at-least-once, what the WAL
remembers. This is the **map of the code that implements them** — what each module in
`src/fleet_telemetry/ingest/` and `src/fleet_telemetry/load/` is for, and why the work is divided
the way it is.

Read the guide first. Then read this with the files open beside it; every section names the lines it
is talking about.

---

## The map

Five modules in `ingest/`, two in `load/`. They split cleanly: **one file is the naive path, three
are the CDC path, one judges them both.**

```text
                  ┌─ poller.py ────────────────────────────────► bronze.poll_rows
   OLTP ──────────┤                                                      │
                  └─ connector.py → [Debezium → Redpanda]                │
                                            │                            │
                                       consumer.py                  compare.py
                                       envelope.py                        │
                                        writer.py ──► bronze.raw_* ◄──────┘
```

| Module | Owns | When you run it |
| --- | --- | --- |
| [`ingest/poller.py`](../../src/fleet_telemetry/ingest/poller.py) | the naive path, end to end | `--cycles N`, or as a loop |
| [`ingest/connector.py`](../../src/fleet_telemetry/ingest/connector.py) | **setup only** — tells Debezium what to capture | once, `--register` |
| [`ingest/envelope.py`](../../src/fleet_telemetry/ingest/envelope.py) | routing and decoding decisions | never directly; pure logic |
| [`ingest/consumer.py`](../../src/fleet_telemetry/ingest/consumer.py) | the CDC loop and its transaction boundary | continuously |
| [`load/writer.py`](../../src/fleet_telemetry/load/writer.py) | the INSERT into `bronze.raw_*` | never directly |
| [`load/schema.py`](../../src/fleet_telemetry/load/schema.py) | all bronze DDL | on every startup, automatically |
| [`ingest/compare.py`](../../src/fleet_telemetry/ingest/compare.py) | the phase-2 deliverable — the diff | after both paths have run |

**The asymmetry is the shape of the lesson.** The poller is *one file*, because polling is one idea.
CDC is four files plus two containers, because it is not. If the second column of that table looks
like more machinery than the problem deserves, the guide's measured section on what CDC cost is the
answer to that instinct — and the instinct is correct until you have watched the poller lose a row.

---

## 1. `poller.py` — one line of SQL, and 190 lines of why

The whole idea:

```sql
select * from vehicles where updated_at > <watermark> order by updated_at limit 1000
```

Everything else in the file is machinery around that query, and each piece of it exists because a
simpler version broke.

### The one question a poller can answer

> *What does this row look like **now**, if it changed since I last looked?*

To ask that you need two things: a column that moves when the row changes, and a memory of where you
last reached.

### The column is per-table, and that is not a detail

[`POLLED_TABLES`](../../src/fleet_telemetry/ingest/poller.py#L41-L48):

```text
depots, drivers, vehicles, jobs  ->  updated_at    mutable, trigger-maintained
job_events                       ->  created_at    append-only, never updated
pings                            ->  server_ts     append-only, and has no updated_at at all
```

`pings` has no `updated_at` because there is nothing to update. So its watermark rides on
`server_ts` — **ours**, not `device_ts`. Watermarking on a client-controlled clock would let a single
device with a fast clock drag the watermark into the future and make every other vehicle's pings
invisible.

Note also that `updated_at` is maintained by a **database trigger**, not by application code
([`docker/oltp/init.sql:28-34`](../../docker/oltp/init.sql#L28-L34)). If the app set it by hand and
forgot on one code path, the poller would silently skip those rows forever — data loss that produces
no error and no log line.

### The memory

`bronze.poll_watermarks`, one row per table. Missing means *never polled*, which
[`_watermarks`](../../src/fleet_telemetry/ingest/poller.py#L90-L95) resolves to `EPOCH`.

That resolution is load-bearing in a way worth knowing before you touch the table: deleting a
watermark row does not mean "start fresh", it means **re-read the entire source system**. See
[known-issues §2](../known-issues.md) for the 317,090-row version of that sentence.

### Three things in the loop are load-bearing

**(a) A batch size bounds the statement, not the poll.**
[`poll_once`](../../src/fleet_telemetry/ingest/poller.py#L124-L159):

```text
while True:
    rows = select(... limit BATCH)
    insert rows; advance watermark; commit
    if len(rows) < BATCH or watermark did not move:
        break
```

A poll means *everything since the watermark*, so it keeps issuing statements until one comes back
short. The first version of this stopped at `LIMIT` — and with 172,800 pings in the baseline the
poller would sit permanently ~1,000 rows into a 172,800-row backlog, **reporting success every
cycle**. The comment at [`BATCH`](../../src/fleet_telemetry/ingest/poller.py#L77-L87) records it.

**(b) Rows and the watermark commit together.**

```text
insert rows        ─┐
update watermark   ─┤  one transaction
commit             ─┘
```

Split them and you are choosing a failure direction, whether you meant to or not. Watermark first →
a crash loses rows and **nothing reports it**. Rows first → a crash re-reads them, which is a
duplicate. The poller chooses duplicates, because a duplicate is visible and recoverable and silent
loss is neither.

It commits **per batch**, not per drain. One transaction around the whole drain would hold an OLTP
snapshot open for 172,800 rows — which is failure 3 of the poller, made worse by the fix for
failure 2.

**(c) The watermark can only move forward.**
[`next_watermark`](../../src/fleet_telemetry/ingest/poller.py#L51-L70) starts at `current` and only
ever raises it. Two guards, both earned:

- An empty batch returns `current`, not `now()`. Advancing on an empty poll is the textbook leak.
- The result is clamped at `current` because **the container clock steps backwards** ~2.7 s every
  30 s under WSL2 ([source-system-reference §8](../source-system-reference.md#8-continuous-mode-and-a-clock-that-cannot-be-trusted)).
  With a strict `>`, a retreating watermark would skip everything committed in between.

### Where it loses, which is the point

The `>` is strict, deliberately — `>=` would re-read the boundary row on every poll forever. The
cost: two rows sharing a timestamp to the microsecond, straddling a batch boundary, and the second is
dropped.

| Failure | How it was reproduced |
| --- | --- |
| Never sees a DELETE | vehicles created and deleted between two polls → `deletes_seen_by_poller: 0` |
| Two changes collapse into one | one vehicle patched 4→7→4→7; the poller reports one row, final state |
| Competes with the app for the OLTP | it is `select *` against the live database, on the same connection budget |
| **The watermark leaks under the ordinary clock** | two vehicles patched a second apart; the one written *second* carries an `updated_at` 1.5 s **earlier**, and can never be selected again |

The fourth is not in the module docstring and is the one that surprised us — it needs no long
transaction, no unusual load, nothing but two API calls and a clock that resyncs. Written up as
[known-issues §4](../known-issues.md).

**The argument for CDC, in one sentence:** the poller asks the database *what things look like*, and
no answer to that question can tell you *what happened*.

---

## 2. `connector.py` — the only module here that is not a program

It moves no data. It PUTs a JSON config to the Kafka Connect REST API, and Debezium does the work.
So the file is mostly a **docstring justifying every key**, which is the honest form for it: the real
risk in CDC is not code, it is a config key someone copied without understanding.

It is Python rather than a shell script for two reasons — this repository is Windows-first, and the
config carries the OLTP password, which must come from
[`config.py`](../../src/fleet_telemetry/config.py) rather than being committed. The file on disk holds
`${OLTP_USER}` / `${OLTP_PASSWORD}` placeholders, filled by `Template.substitute` at registration.

Three keys worth carrying in your head:

| Key | Why |
| --- | --- |
| `heartbeat.interval.ms = 10000` | **The one nobody expects.** Debezium only advances the slot's confirmed LSN when it *emits* something. A captured table goes quiet → the slot stops advancing → WAL grows → **the disk fills behind a connector reporting healthy.** |
| `partitions = 1` | Kafka orders *per partition*. A second partition lets two changes to the same row arrive out of order, and a Type 2 dimension built from a reordered stream is wrong in a way that looks plausible. It also makes `(partition, offset)` a total order per topic — which is what bronze's dedup index relies on. |
| no `ExtractNewRecordState` SMT | The popular `unwrap` transform flattens the envelope to the after-image and turns deletes into tombstones. That discards `before` and `op` — **the exact two fields the entire CDC argument rests on.** |

Also worth knowing: `--delete` removes the connector and **not** the replication slot, so it can be
recreated and resume. That default is right; the missing tooling around it is
[known-issues §5](../known-issues.md).

---

## 3. `envelope.py` — 78 lines, no Kafka import

Two functions, and both are *decisions* rather than plumbing.

`table_for_topic` maps six topics onto **three** bronze tables — pings (enormous, append-only),
job_events (append-only and out of order), and the four mutable entities (carrying the before/after
images Type 2 dimensions are built from). It returns `None` for anything unrecognised rather than a
default table, because an unplanned topic is a **finding**: someone changed `table.include.list`, or
the prefix moved. Filing it somewhere plausible hides that in a table nobody would look at.

`decode` has one rule: **it never raises and never returns nothing. Every input produces a row.**

| Input | What lands in bronze |
| --- | --- |
| valid JSON object | `payload` |
| invalid UTF-8 | `raw_payload = repr(bytes)` + the error — no lossless text exists, and the repr is what survives retention |
| invalid JSON | `raw_payload = text` + the parser's message |
| valid JSON that is not an object | the text + *"payload is a list, not a JSON object"* — `jsonb` would accept `[1,2,3]` silently and then every generated column is null with no visible cause |
| null value (tombstone) | recorded, not dropped — dropping it leaves a gap in the offset sequence with nothing to explain it, and offset gaps are how you wrongly conclude the loader lost data |

**Why there is no Kafka import here.** Partly so CI can install `[dev]` only without every test in the
file becoming a collection error. But the real reason is that these decisions — what counts as
malformed, what happens to a message nothing expects — are the ones worth testing, and they should be
testable without a broker.

---

## 4. `consumer.py` — four steps, and the order is the entire design

```text
1. poll a batch
2. INSERT ... ON CONFLICT DO NOTHING
3. Postgres COMMIT
4. consumer.commit()        <- only now may the offset move
```

**Nothing is lost**, because the offset advances only after the rows are durable — crash before
step 3 and the same messages are redelivered. **Nothing is duplicated**, because a redelivery carries
the same `(partition, offset)` and the unique index refuses it.

Swap 3 and 4 — or simply leave `enable.auto.commit` at its default of `true`, which effectively
swaps them — and you get **silent data loss**: the loader reports success, the offsets sit past data
that never landed, and nothing anywhere says a batch went missing. That is why auto-commit is
disabled explicitly at
[`consumer.py:112`](../../src/fleet_telemetry/ingest/consumer.py#L112) rather than left alone.

Two smaller decisions in the same file:

- `auto.offset.reset = earliest`, so a new consumer group does not skip the Debezium snapshot. The
  default, `latest`, would discard it silently.
- On an **unrouted topic it raises** rather than skipping
  ([L149-152](../../src/fleet_telemetry/ingest/consumer.py#L149-L152)). Skipping would advance the
  offset past a topic and lose it permanently once retention expires, with no gap in the offsets to
  show it ever happened.

---

## 5. `load/` — the insert, and the DDL

[`writer.py`](../../src/fleet_telemetry/load/writer.py) does one INSERT per bronze table per batch.
It deliberately does **not** commit: the caller owns that boundary, because the whole restart
guarantee is a statement about commit *ordering*, and a writer that committed on its own would take
that choice away from the only code that can make it.

Its interesting output is not `inserted`, it is **`suppressed`** — how many rows the dedup index
refused. Zero forever means nothing has crashed yet. A jump after a restart is the at-least-once
guarantee doing visibly what it promises. Measured in the run behind the guide: 153,566 replayed
messages, 0 inserted.

`executemany` rather than `COPY`, because `COPY` cannot do `ON CONFLICT`, and correctness under
replay is worth more than throughput at 8 events a second.

[`schema.py`](../../src/fleet_telemetry/load/schema.py) holds all bronze DDL and is applied on every
startup rather than as a manual step — a loader that needs someone to remember a setup command is a
loader that fails on a fresh clone. Two design points:

- **Python inserts the envelope and nothing else.** Every column Silver reads is a
  `GENERATED ALWAYS ... STORED` projection over `payload`, evaluated by Postgres. They really are
  columns, so dbt's `source()` references and tests work unchanged.
- **No casts in those generated columns** — all `text`, including `sequence_no` and `source_ts_ms`. A
  cast inside `GENERATED ALWAYS` is evaluated on INSERT, so one device sending `"banana"` would fail
  the whole batch: bronze rejecting precisely the malformed evidence it exists to keep. Postgres 16
  has no `TRY_CAST`. **Silver casts, where a bad value is a failing test rather than data loss.**

Note the shape of `bronze.poll_rows` beside `bronze.raw_cdc_entities`: no `op`, no `before`. The
poller cannot observe either, and inventing them would be inference dressed as measurement. **The
missing columns are the finding.**

---

## 6. `compare.py` — the part that makes this a project rather than a pipeline

Every number comes from a query. Nothing is asserted from the schema or from what a tool is
documented to do — the same rule the phase 1 profiler follows, for the same reason: a measurement
that disagrees with ground truth means the *measurement* is wrong.

Two findings carry the phase.

**The headline, and a hardcoded zero:**

```text
out["deletes_seen_by_cdc"]    = <count from a query>
out["deletes_seen_by_poller"] = 0        # structural: no query could return anything else
```

That second line is a literal, with a comment saying why it can never be anything else. It is the one
place in the codebase where *not* measuring something is the correct answer.

**`changes_the_poller_collapsed`** is a left join asking: for which vehicle did CDC record more
changes than the poller recorded observations? Each row is a state the database genuinely held, that
no poller at any frequency can recover.

And `poller_blind_spots` runs the check in the harder direction — vehicle ids present in
`bronze.poll_rows` that no longer exist in the OLTP. Bronze says those vehicles are active, and
**nothing in bronze will ever say otherwise.** The poller cannot shrink that set.

---

## Reading order

If you are following the code with the system running:

1. `poller.py` top to bottom. It is self-contained and it is the version you must understand before
   the next four make sense.
2. `connector.py`'s **docstring only** — do not read the code yet. The keys are the content.
3. `envelope.py`. Small, pure, and the decisions are all visible at once.
4. `consumer.py`, then `writer.py`, in that order. The four-step comment at the top of `consumer.py`
   is the thing to remember; `writer.py` is what step 2 does.
5. `schema.py`, once you have seen what gets inserted. The DDL reads differently after that.
6. `compare.py` last, with the output of an actual run in front of you.

---

## Explain-back

Answer from memory. If you cannot, this tour has not landed, however clear it looked.

1. **Why does `pings` watermark on `server_ts` while `vehicles` watermarks on `updated_at`?**
   `pings` is append-only and has no `updated_at` — there is nothing to update. `server_ts` is also
   the only trustworthy choice: `device_ts` is client-controlled, so one device with a fast clock
   would drag the watermark into the future and hide every other vehicle's pings.

2. **What breaks if `BATCH` bounds the poll instead of the statement?**
   "Everything since the watermark" silently becomes "up to 1,000 rows". The poller falls permanently
   behind while every cycle still reports success. The bug is invisible in the logs.

3. **Why must `consumer.commit()` come after `conn.commit()`, and what does the reverse look like from
   the outside?**
   The offset may only advance once the rows are durable. Reversed, a crash in between leaves the
   offsets past data that never landed — the loader reports success and nothing anywhere records that
   a batch went missing. The correct order fails toward duplicates, which the unique index absorbs.

4. **Why does `envelope.decode` store a message it could not parse instead of raising?**
   Because the message that broke something is the evidence bronze exists to hold. Raising would lose
   it; skipping would lose it *and* leave an unexplained gap in the offset sequence.

5. **Why is `deletes_seen_by_poller` a hardcoded `0` in a module whose stated rule is that every
   number comes from a query?**
   Because it is structural, not measured. A poller reads current state, so no query over that state
   could ever return a different number — and stating that is worth more than measuring it.

---

## Next

- The ideas behind all of this: [`02-ingestion.md`](02-ingestion.md)
- What is broken and what is deliberate: [`../known-issues.md`](../known-issues.md) — read its §10
  before "fixing" anything in the poller
- Phase 3, which is the project: `03-transformation.md`
