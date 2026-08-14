# Phase 2 — Ingestion

> **How do I get the data out, without losing or duplicating any of it?**

Phase 1 measured the source system. This phase moves what it holds into `bronze.*`, twice: once
with a batch poller, once with change data capture. The poller is built first and is meant to
lose. Watching *what* it loses is the phase.

The deliverable is not "bronze is populated". It is the diff between the two paths, and the
diff is at the bottom of this page.

---

## The inversion

The obvious way to get data out of a database is to query it: `SELECT ... WHERE updated_at >
watermark`, on a loop. It works, it is twenty lines, and it needs no new infrastructure.

It is also structurally incapable of seeing three things, and the third only becomes obvious
once you have watched the first two happen.

Change data capture reads the write-ahead log instead — the journal Postgres writes *before* it
touches a table, so it can recover after a crash. That journal contains every committed change
in order, including the ones a query can no longer see. Reading it is how you find out what the
database *did* rather than what it currently *holds*.

---

## What you build

```text
                    ┌─ poller ──── SELECT WHERE updated_at > wm ──> bronze.poll_rows
oltp (postgres) ────┤
                    └─ WAL ─ Debezium ─ Redpanda ─ consumer ──────> bronze.raw_*
```

| Piece | File | Job |
| --- | --- | --- |
| Batch poller | `src/fleet_telemetry/ingest/poller.py` | The naive version. Watermark per table, drains each one. |
| Bronze schema | `src/fleet_telemetry/load/schema.py` | All bronze DDL. Owned by Python, not dbt. |
| Connector | `docker/debezium/fleet-connector.json` | Debezium's configuration, one justified key at a time. |
| Registrar | `src/fleet_telemetry/ingest/connector.py` | Registers it over the Connect REST API. |
| Router / decoder | `src/fleet_telemetry/ingest/envelope.py` | Topic → bronze table. Never drops a message. |
| Writer | `src/fleet_telemetry/load/writer.py` | The only thing that writes `bronze.*`. |
| Consumer | `src/fleet_telemetry/ingest/consumer.py` | The loop, and the commit ordering that makes restarts safe. |
| Comparison | `src/fleet_telemetry/ingest/compare.py` | The deliverable. Every number from a query. |

## Run it

```bash
docker compose -f docker/docker-compose.yml up -d
python -m fleet_telemetry.load.schema
python -m fleet_telemetry.ingest.connector --register

python -m fleet_telemetry.ingest.consumer        # CDC path, runs until interrupted
python -m fleet_telemetry.ingest.poller          # poller path, in another terminal
python -m fleet_telemetry.ingest.compare         # the diff
```

---

A module-by-module tour of the code that implements all of this — what each file in `ingest/` and
`load/` owns, and why the work is divided that way — is in
[`02-ingestion-code-tour.md`](02-ingestion-code-tour.md). Read it after this guide; the ideas below
are what it assumes.

## The six ideas in this phase

### 1. A watermark leaks in two directions

`next_watermark` is nine lines and both of its guards were earned.

**Never advance on an empty poll.** The tempting version sets the watermark to `now()` when
nothing came back. Then a transaction that committed with an earlier timestamp but became
*visible* afterwards is permanently behind the mark, and is never read. Nothing errors. The row
simply never arrives.

**Never let the watermark move backwards.** Phase 1 measured this container's clock stepping
back about 2.7 seconds every 30 under WSL2 (`docs/source-system-reference.md`, §8). With a
strict `>` comparison, a watermark that accepted a lower value would skip every row committed in
between.

### 2. `LIMIT` can silently redefine what a poll means

The first version of `poll_once` selected `... LIMIT 1000` per table and called that a poll.
It passed every test except one: *a second poll with no changes lands nothing*. With 172,800
pings, the second poll landed another 1,000.

The batch size has to bound the **statement**, not the poll. Otherwise "everything since the
watermark" quietly becomes "up to 1,000 rows", the poller is permanently catching up, and every
poll still reports success. A poll now drains each table until a statement returns less than a
full batch.

The termination condition is worth reading too: it stops when the batch is short **or the
watermark did not move**. Without the second clause, a full batch of rows sharing one timestamp
loops forever.

### 3. Three failures, reproduced rather than recited

**It never sees a delete.** `tests/test_poller.py::test_a_poller_can_never_see_a_delete` inserts
a vehicle, polls, deletes it, polls again. The second poll lands nothing, and bronze still holds
exactly one row saying the vehicle exists. No polling frequency changes this: the row is not
there to be selected. `DELETE /vehicles/{id}` is a hard delete for precisely this demonstration.

**It collapses two changes into one.** A driver's depot moves to 5 and back to 1 between two
polls. One observation lands, holding depot 1. Depot 5 was committed, was real, and is gone.

Phase 1 already measured this at scale without naming it: the simulator made **12** vehicle
reassignments and the database shows **8** changed rows. Four committed states are unrecoverable
by any poller at any frequency.

**It competes with the application.** Measured against `pg_stat_database` rather than asserted:

| | wall clock | `tup_returned` |
| --- | --- | --- |
| 30 idle polls | 1.3 s | +2,587 |
| 1 catch-up poll from an empty watermark | 25.9 s | +172,338 |

The shape is the finding, and it is not the one the design predicted. A *current* poller is
cheap. The cost arrives when it catches up — which is exactly after an outage, when the
application is already recovering. (`blks_read` stayed 0 throughout: this dataset fits in shared
buffers, so both figures are a floor.)

### 4. At-least-once, and the column that saves you

The consumer does four things in a fixed order:

```text
1. poll a batch
2. INSERT ... ON CONFLICT DO NOTHING
3. Postgres COMMIT          <- rows are durable
4. consumer.commit()        <- only now may the offset move
```

Nothing is **skipped**, because the offset advances only after the rows are durable. Nothing is
**duplicated**, because a redelivery carries the same `(partition, offset)` and the unique index
refuses it.

Reverse 3 and 4 — or leave `enable.auto.commit` at its default of `true`, which effectively does
reverse them — and a crash produces silent data loss. The loader reports success, the offsets sit
past the data, and nothing anywhere says a batch went missing.

**The deduplication key is the Kafka coordinate, not `ping_id`.** This surprises people, because
`ping_id` is the idempotency key everywhere else. But the application already removed device
retries with `ON CONFLICT (ping_id) DO NOTHING`, so the OLTP holds one row per ping and Debezium
emits one message for it. A duplicate reaching bronze can therefore *only* be a broker
redelivery. `ping_id` would also be wrong twice over: it is null for deletes, and it is shared
between a create and any later re-snapshot of the same row.

Rewinding the consumer group proves it:

```text
153,566 messages replayed → 0 inserted, 153,566 suppressed by the dedup index
```

### 5. The WAL only remembers what you told it to

This is the one that nearly shipped as fabricated history.

Every table was `REPLICA IDENTITY DEFAULT`, which writes only the **primary key** of the old row
to the WAL on an update or delete. Debezium reports what the WAL contains — and fills the columns
it was not given with **type defaults**, not nulls. Deleting a vehicle produced:

```json
"before": {"vehicle_id": 9501, "plate": "", "capacity": 0, "home_depot_id": 0,
           "created_at": "1970-01-01T00:00:00.000000Z", "current_driver_id": null}
```

Only `vehicle_id` is real. The rest is a row that never existed, and it is worse than missing
data: it passes every `not_null` test you would think to write, and a Type 2 dimension built on
it would faithfully record that the vehicle had an empty plate and capacity zero at the moment
it was deleted.

`REPLICA IDENTITY FULL` on the four mutable entities fixes it. `pings` and `job_events` stay at
the default deliberately — they are append-only, so there is no before-image to describe, and
`pings` is where the extra WAL would actually cost something.

The general lesson: **before-images are the entire reason CDC beats snapshots, and they are
off by default.**

### 6. Bronze keeps the evidence, including the evidence of its own failures

`envelope.decode` never raises and never returns nothing. Valid JSON, malformed JSON, bytes that
are not UTF-8, a tombstone, and valid JSON that is not an object all produce a row bronze can
store, with the parse error alongside.

The columns Silver reads are `GENERATED ALWAYS AS` projections over the payload, so Python parses
nothing. And **none of them casts** — every scalar one is `text`, including the obviously numeric
`sequence_no` and `source_ts_ms`. A cast inside `GENERATED ALWAYS` is evaluated on INSERT, so a
device sending `ping_id: "banana"` would raise and take the whole batch with it. Bronze would
reject exactly the malformed evidence it exists to preserve. Postgres 16 has no `TRY_CAST`, so
the only safe projection is the one that cannot fail.

---

## What broke on the way

Written down before being fixed, per the working rule in `README.md`.

| Symptom | Cause |
| --- | --- |
| Redpanda restart-loops: `Argument parse error: unrecognised option '--set=redpanda.auto_create_topics_enabled=true'` | `rpk` does not recognise the *equals* form of `--set`, so it forwards the whole string to the `redpanda` binary. Two argv entries. The equals form works for every other flag in the block, which is what makes it a trap. |
| Connector FAILED: `Missing required configuration "topic.creation.default.replication.factor"` | `topic.creation.enable` reads like a connector property and is a **worker** property. Setting it in connector config makes Connect validate the whole `topic.creation` group and fail naming a key you never set. |
| `--register` raises `404: No status found for connector fleet-oltp` on a connector that is fine | Creating a connector is asynchronous. The PUT returns as soon as the config is accepted; status answers 404 until the herder has started it. |
| `--max-batches N` never returns on a caught-up topic | Empty polls do not count as batches — rightly — so the loop spins on `continue` and the counter never reaches its limit. Looks exactly like a hung consumer. |
| Every generated column null | `CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE` not set to `false`, so each message is wrapped in a `{schema, payload}` envelope and the top-level keys move. Check one message with `rpk topic consume` before trusting any of them. |
| `ruff format` rewrites plan documents | It formats Python code blocks inside Markdown. A plan's snippets are fragments, and it added parentheses to make one parse, changing what it instructed. `docs/superpowers/plans` is excluded. |

### Still broken, and not fixed here

Everything found by running the pipeline end to end — including two defects worse than this one —
is in the register at [`docs/known-issues.md`](../known-issues.md). Read its §10 before fixing any
of it: several of this phase's most conspicuous failures are deliberate.

`DELETE /vehicles/{id}` returns **500** for every vehicle in the seeded dataset.
`app/main.py` deletes `pings`, `jobs` and `vehicles`, but not `job_events`, which holds a foreign
key to `jobs`:

```text
psycopg.errors.ForeignKeyViolation: update or delete on table "jobs" violates foreign key
constraint "job_events_job_id_fkey" on table "job_events"
```

All 40 seeded vehicles have jobs with events, so the endpoint cannot succeed on any of them. The
delete demonstrations above use freshly created vehicles that have no job history. This is a
phase-0 application bug, not an ingestion one, but it disables the cleanest argument this phase
has — so it is written here rather than left to be rediscovered.

---

## The diff

One window, both paths running, the same simulator run underneath.

### What each path captured

| | poller | CDC |
| --- | --- | --- |
| Records a **create** | as an observation, if it survives to the next poll | `op='c'` with an after-image |
| Records an **update** | the latest value only | `op='u'` with **before and after** |
| Records a **delete** | never | `op='d'` with the full final row |
| Records the **order** of changes | no | yes, `source.ts_ms` from the WAL |
| Sees a row created and deleted between polls | never existed | both events |

### Measured

```text
poller    2 observations
CDC       6 events
```

Vehicles 9501 and 9502 were created, updated and deleted entirely between two polls:

```text
poll_rows mentioning them      0
raw_cdc_entities events        5
```

Vehicle 1's driver moved 1 → 5 → 1 between polls:

```text
CDC     op=u after.current_driver_id=5
        op=u after.current_driver_id=1
poller  current_driver_id=1
        current_driver_id=1
```

Driver 5 held that vehicle. Both paths agree on where it ended. Only one can say it happened.

### Bronze after the run

| table | rows | malformed |
| --- | --- | --- |
| `bronze.raw_ping_events` | 172,800 | 0 |
| `bronze.raw_cdc_entities` | 240 | 0 |
| `bronze.raw_job_events` | 535 | 0 |
| `bronze.poll_rows` | 173,568 | — |

The ping counts match exactly, which is the point: for an **append-only** table the poller is
not worse. It is worse on everything that changes.

---

## What CDC cost

Honesty about the other side of the ledger.

- **Two more containers.** Redpanda (~1 GB configured) and a JVM running Kafka Connect.
- **A replication slot that must be drained.** An unconsumed slot makes Postgres retain WAL
  forever and eventually fills the disk. That is a real way to take an instance down.
- **An initial snapshot.** `snapshot.mode=initial` reads all 172,800 ping rows and emits them as
  `op='r'` before streaming anything.
- **More configuration to get wrong**, as the failure table above demonstrates at length.
- **A broker that is not durable storage.** A topic has a retention window, not a memory — which
  is why `bronze.*` exists downstream of it, and why an unconsumed event is simply gone.

Worth it here because the warehouse must reconstruct history the OLTP does not keep. Not
automatically worth it for an append-only table, where the poller matched CDC exactly.

---

## Numbers worth remembering

| | |
| --- | --- |
| Replayed messages absorbed by the dedup index | 153,566 → 0 inserted |
| Catch-up poll vs 30 idle polls, tuples read | 172,338 vs 2,587 |
| Simulator reassignments vs rows a poller can see | 12 vs 8 |
| Row lifecycles invisible to the poller, this run | 2 |
| Malformed payloads in bronze | 0 (so far — the column exists for when it is not) |

---

## Explain-back

Answer from memory. If you cannot, the phase is not finished, however well the code runs.

1. **Why can a batch poller never detect a delete?**
   Because it reads current state. A deleted row has no `updated_at` to exceed the watermark and
   is not there to be returned. No frequency helps; there is no query that would find it.

2. **What does at-least-once mean for your Bronze table, and which column saves you?**
   It means the same message can arrive twice — after a crash between the database commit and
   the offset commit. `_kafka_offset` (with `_kafka_partition`) saves you, not `ping_id`: the
   application already deduplicated device retries, so a duplicate here can only be a
   redelivery, and `ping_id` is null for deletes anyway.

3. **What happens to a replication slot if the consumer stops for a week?**
   Postgres cannot discard WAL the slot has not confirmed, so it retains all of it and the disk
   fills. The slot's `wal_status` moves `reserved` → `extended` → `lost`, and at `lost` the
   connector cannot resume — it needs a fresh snapshot, and anything that happened in between is
   gone. `heartbeat.interval.ms` exists because a *quiet* captured table causes the same growth
   without any consumer being down at all.

Check it yourself:

```bash
psql "postgresql://fleet:fleet@127.0.0.1:55433/fleet" -c \
  "select slot_name, active, wal_status,
          pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as retained
     from pg_replication_slots"
```

Stop the consumer, leave it stopped, and watch `retained` grow.

---

## Next

**Phase 3 — Transformation** (`03-transformation.md`, not written yet). Bronze is the durable
record; nothing in it is trustworthy yet. Phase 3 is where the central problem lives: events
arrive late, out of order, and in bursts, and the aggregates must still be correct.
