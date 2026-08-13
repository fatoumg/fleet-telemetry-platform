# Known issues

Defects found by running the pipeline, recorded before being fixed. Each entry states the
**symptom** (what a reader sees), the **cause**, what it **costs**, and the **fix** — in that
order, because the symptom is what someone will search for and the cause is never what the symptom
suggests.

Found during the first full end-to-end run on 2026-08-12, after the phase-2 CDC path merged.

**Read §10 before fixing anything.** Several of this project's most conspicuous failures are
deliberate and load-bearing; "fixing" them destroys the lesson they exist to teach. That section
lists them so they are not mistaken for entries in this register.

| # | Issue | Severity | Status |
| --- | --- | --- | --- |
| 1 | `REPLICA IDENTITY FULL` never reaches an existing volume | **high** — silently fabricates data | patched by hand; repo unfixed |
| 2 | Integration tests wipe bronze and trigger a full re-ingest | **high** — destroys the durable record | open |
| 3 | Fixed-id test rows poison every later run | medium | open |
| 4 | The watermark leak fires under the ordinary clock | medium — doc claim is wrong | open |
| 5 | The replication slot has no lifecycle owner | medium — can fill the disk | open |
| 6 | `DELETE /vehicles/{id}` returns 500 for every seeded vehicle | medium | open, already written up |
| 7 | Debezium connects as the table owner | low here, ships badly | open |
| 8 | Three `PostToolUse` hooks are permission-rule syntax | low — noise only | open |
| 9 | `writer.py` claims to be the only writer of `bronze.*`, and is not | low — doc claim is wrong | open |

---

## 1. `REPLICA IDENTITY FULL` never reaches an existing volume

**Symptom.** CDC updates carry an empty `before`, and deletes carry a before-image that looks like
a real row and is not:

```text
op | vehicle_id | before_depot | before_plate | before_capacity | before_created_at
d  |       9601 |            0 |           "" |               0 | 1970-01-01T00:00:00Z
```

Only the primary key is real. Every other field is a **type default invented by Debezium** because
the WAL did not carry the old row.

**Cause.** [docker/oltp/init.sql:189-192](../docker/oltp/init.sql#L189-L192) is correct:

```sql
ALTER TABLE depots   REPLICA IDENTITY FULL;
ALTER TABLE drivers  REPLICA IDENTITY FULL;
ALTER TABLE vehicles REPLICA IDENTITY FULL;
ALTER TABLE jobs     REPLICA IDENTITY FULL;
```

But `init.sql` is mounted into `docker-entrypoint-initdb.d`, and **that directory runs only when
`PGDATA` is empty**. The `oltp` volume was created 2026-08-07T13:06:21Z; those four lines landed
2026-08-12 10:19:37. Any developer whose volume predates that commit has a database where the
statements exist in the repo and have never executed. `docker compose up` will not apply them.
`docker compose restart` will not either.

**Cost.** This disables CDC's headline advantage while every component reports success — no error,
no warning, valid-looking JSON in bronze. A Type 2 dimension built on it would record a vehicle
that moved *from depot 0, plate `""`, capacity 0, created 1970-01-01*, and the history would be
wrong in a way that reads as data rather than as a bug.

**Detect it:**

```sql
select relname, relreplident from pg_class
 where relname in ('depots','drivers','vehicles','jobs');
-- 'd' = DEFAULT (broken)   'f' = FULL (correct)
```

**Fix.** Applied live on 2026-08-12 — the `ALTER`s need no restart and take effect on the next
change. The repo is still unfixed, and the class of bug matters more than this instance: **DDL in a
run-once init script is not a migration.** Two candidate fixes:

- `connector.py --register` reads `relreplident` for the four mutable tables and refuses to
  register — or loudly warns — when any is `'d'`. This is the cheap one and it puts the check where
  the assumption is made.
- A small idempotent migration step, run like `load.schema.apply()` is, that owns OLTP alterations
  that must survive an existing volume.

The same class already bit once in this phase: see §2, where a `CREATE TABLE IF NOT EXISTS`
silently skipped a table whose columns had diverged.

---

## 2. Integration tests wipe bronze and trigger a full re-ingest

**Symptom.** `bronze.poll_rows` jumps to ~318,877 rows for no reason anyone can point to, and a
`pytest -m integration` run takes ~40 s longer than expected.

**Cause.** The `databases` fixture in [tests/test_poller.py:52-70](../tests/test_poller.py#L52-L70)
clears both poll tables unconditionally:

```sql
delete from bronze.poll_rows;
delete from bronze.poll_watermarks;
```

Deleting the watermarks is the expensive half. A missing watermark means *never polled*, which
[poller.py:90-95](../src/fleet_telemetry/ingest/poller.py#L90-L95) resolves to `EPOCH` — so the very
next `poll_once` selects the entire source system. Measured: **317,090 pings re-ingested in ~40 s.**

**Cost.** Two things, and the second is worse than the slow test.

1. *Bronze is not optional* — it is the durable record everything downstream is rebuilt from. A test
   run silently truncating it means a dbt build can produce different numbers before and after
   someone ran the suite, with nothing in the history to explain the difference.
2. `bronze.poll_rows` has no idempotency key — `poll_row_id` is a `bigserial` and there is no unique
   constraint — so the re-ingest lands as 317,090 *additional* observations rather than being
   absorbed. That is a deliberate asymmetry with the CDC tables, which dedupe on
   `(_kafka_partition, _kafka_offset)`; the poller genuinely cannot supply an equivalent key. It
   does mean, though, that nothing downstream can distinguish the re-ingest from a real burst.

**Fix.** Point the integration tests at a warehouse the developer does not care about, rather than
making them tidy the one they do. Options, best first:

- A separate test database (`telemetry_test`), created by the fixture and dropped after. Nothing to
  clean up, and no path by which a test can reach real bronze.
- A test-only schema, with `schema.apply()` parameterised on the schema name.
- Failing both: scope the writes with a self-identifying label — a `capture_run` column — and delete
  by that label only, never with an unqualified `delete from`.

**Related, and the reason this was found.** An earlier branch left three orphaned tables in bronze,
one of which was a `poll_watermarks` with different columns. `CREATE TABLE IF NOT EXISTS` is
idempotent against **absence, not divergence** — it skipped the table silently, and four tests
failed with `column "watermark_value" does not exist`. Dropping those tables is what reset the
watermarks and caused the re-ingest above.

---

## 3. Fixed-id test rows poison every later run

**Symptom.**

```text
psycopg.errors.UniqueViolation: duplicate key value violates unique constraint "drivers_pkey"
DETAIL:  Key (driver_id)=(9401) already exists.
```

on a test that passed yesterday and whose code has not changed.

**Cause.** [tests/test_poller.py:141-147](../tests/test_poller.py#L141-L147) inserts driver 9401
with a plain `insert`, and deletes it in the **last statement of the test body**. Any earlier
assertion failure — or an interrupt — skips the teardown and leaves the row behind. Every subsequent
run then fails on the insert, before reaching the behaviour under test.

The choice of high ids (`9401`, above the seeded fleet and above anything the simulator touches) is
right and should stay; the lifecycle around them is what fails.

**Cost.** A one-off failure becomes a permanent one, and it presents as a *different* bug from the
one that actually broke. Worse, the first failure's real message is buried under the second.

**Fix.** Two changes, both small:

- `on conflict (driver_id) do nothing` on the insert, so a leftover row is not fatal.
- Teardown in the fixture, not the test body — a `finally`, or a fixture that deletes the test ids
  before yielding as well as after. Cleaning up *before* is what makes a run recoverable without
  hand-editing the database.

---

## 4. The watermark leak fires under the ordinary clock, not just under long transactions

**Symptom.** Two vehicles patched in sequence, and the one written **second** carries the earlier
`updated_at`:

```text
vehicle_id | updated_at
         2 | 11:32:31.153851     <- patched first
         3 | 11:32:29.637101     <- patched second, 1.5 s EARLIER
```

Once the poller's watermark passes `11:32:31`, vehicle 3's change can never be selected.
`11:32:29 > 11:32:31` is false, permanently. No polling frequency helps.

**Cause.** Not a long transaction. The container's **wall clock stepped backwards** between the two
requests — the same WSL2 resync documented in
[source-system-reference.md §8](source-system-reference.md#8-continuous-mode-and-a-clock-that-cannot-be-trusted),
measured at roughly 2.7 s every 30 s. `set_updated_at()` stamps `now()`, so `updated_at` ordering
follows a clock that does not monotonically advance.

**Cost.** The write-up in [02-ingestion.md §1](learn/02-ingestion.md) attributes the leak to
transaction visibility — a transaction that starts early and commits late, so its row is stamped
behind a watermark that has already moved past it. That is real and it is the textbook cause. But
**on this machine the clock is the more likely trigger**, and it needs no unusual conditions: two
ordinary API calls a second apart are enough. Leaving the doc as-is teaches a reader to look for
long transactions when the actual cause is sitting in the environment.

`next_watermark`'s clamp does not help here. It stops the *watermark* from moving backwards, which
prevents re-reading and skipping in between; it cannot recover a row whose timestamp landed behind a
mark that has already advanced.

**Fix.** Nothing to change in the poller — the leak is deliberate, and reproducing it is the point.
What needs changing is the claim: §1 of the ingestion guide should name **both** causes and note
that the clock one fires in ordinary use here. Reference this entry for the measurement.

---

## 5. The replication slot has no lifecycle owner

**Symptom.** None, until the disk fills.

**Cause.** By design, `connector.py --delete` removes the connector and **not** the slot — so the
connector can be recreated and resume from where it stopped, which is the right default. The gap is
that nothing else owns the slot afterwards:

- No command drops it. The instructions live in a docstring
  ([connector.py:150](../src/fleet_telemetry/ingest/connector.py#L150)), the README, and `CLAUDE.md`.
- No command reports on it. `--status` describes the connector; it says nothing about whether the
  slot is being drained or how much WAL it is holding.
- Nothing keeps the thing that drains it running. **The slot advances only while the Debezium
  connector is running inside `fleet-connect`** — a stopped Connect worker, a `docker compose down`,
  or a `--delete` all stop the advance while the slot itself survives, because the slot lives in
  Postgres and not in Connect.

  Worth stating plainly, because it is easy to get backwards: `consumer.py` reads **Kafka**, not the
  slot. A running Python consumer does nothing for WAL retention, and believing otherwise is exactly
  the reasoning that leaves a slot unattended.

An inactive slot makes Postgres retain WAL **indefinitely**. It is the one piece of live state in
this project that can take the machine down. `heartbeat.interval.ms` covers a *quiet* captured table;
it cannot cover a connector that is not running.

**Cost.** Measured after the end-to-end run: `active=t`, `wal_status=reserved`, `retained=748 kB` —
healthy, because the connector is up. The failure mode is that same number growing without bound
while every component reports success and nothing surfaces it.

**Detect it:**

```sql
select slot_name, active, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as retained
  from pg_replication_slots;
```

**Fix.**

- Fold that query into `--status`, so the hazard is visible from the command a reader already runs.
- Add an explicit `--drop-slot`, separate from `--delete` and never implied by it, so tearing down
  deliberately does not require remembering a `psql` incantation.
- Longer term this belongs to phase 4: a supervised consumer is what keeps the slot drained.

---

## 6. `DELETE /vehicles/{id}` returns 500 for every seeded vehicle

Already written up in
[02-ingestion.md, "Still broken, and not fixed here"](learn/02-ingestion.md) — recorded here so the
register is the one place to look.

**Summary.** `app/main.py` deletes `pings`, `jobs` and `vehicles`, but not `job_events`, which holds
a foreign key to `jobs`:

```text
psycopg.errors.ForeignKeyViolation: update or delete on table "jobs" violates foreign key
constraint "job_events_job_id_fkey" on table "job_events"
```

All 40 seeded vehicles have jobs with events, so the endpoint cannot succeed on any of them. The
delete demonstrations in the guide use freshly created vehicles with no job history.

**Cost.** It disables the cleanest argument this phase has — a hard delete is the single thing a
poller provably cannot see, and the endpoint that produces one works only on vehicles created for
the demonstration.

**Fix.** Delete `job_events` for the vehicle's jobs before deleting the jobs, inside the existing
transaction. A phase-0 application bug, not an ingestion one.

---

## 7. Debezium connects as the table owner

**Cause.** `docker/debezium/fleet-connector.json` takes `database.user` from `config.oltp()`, which
is `fleet` — the role that owns every table in the OLTP. No `GRANT` is issued anywhere in
`docker/`, and no dedicated replication role exists.

**Cost.** Nothing locally, and this is not a security finding in a project with a static bearer
token by design. It matters because least privilege for CDC is *a real topic in the domain being
learned*, and the current setup is the version that ships by accident: a connector with write access
to everything it reads.

**Fix.** A `fleet_cdc` role with `REPLICATION`, `SELECT` on the six tables, and nothing else, created
in `init.sql` and used by the connector. Note the §1 caveat — a role added to `init.sql` will not
appear on an existing volume either.

---

## 8. Three `PostToolUse` hooks are permission-rule syntax

**Symptom.** Every `Edit`/`Write` prints:

```text
syntax error near unexpected token 'python'
PostToolUse:Write [PowerShell(python -m ruff check .)] failed with non-blocking status code 1
```

**Cause.** Three entries in `.claude/settings.json` hold `PowerShell(python -m ruff check .)` and
similar. That is **permission-rule** syntax — the form used in `permissions.allow` — pasted into a
hook `command`, where the value is executed as a shell command. `PowerShell(...)` is not a command.
The two hooks that work (`bash .claude/hooks/format-python.sh`, `check-python.sh`) show the correct
form, and they already do the ruff work the broken three duplicate.

**Cost.** Noise only — the hooks are non-blocking and nothing is skipped. But the file is tracked in
git, so every collaborator gets the same three errors on every file write, and real hook failures are
harder to notice in the middle of them.

**Fix.** Delete the three `PowerShell(...)` hook entries. `format-python.sh` and `check-python.sh`
already cover formatting and linting; if a test hook is wanted, it needs the same
`bash .claude/hooks/...` shape.

---

## 9. `writer.py` claims to be the only writer of `bronze.*`, and is not

**Symptom.** None at runtime. It misleads a reader instead.

**Cause.** [writer.py:1](../src/fleet_telemetry/load/writer.py#L1) opens with *"the only thing in this
project that writes `bronze.*`"*, and `CLAUDE.md`'s module table repeats it as *"the only writer of
`bronze.*`"*. Both are wrong: [poller.py:136-150](../src/fleet_telemetry/ingest/poller.py#L136-L150)
inserts into `bronze.poll_rows` and `bronze.poll_watermarks` directly, without going through
`writer.py` at all.

**Cost.** The claim is the kind a reader relies on — "where does bronze get written?" has a
one-file answer, and that answer is wrong for two of the five bronze tables. Anyone auditing bronze
writes, or adding a constraint expecting one code path to enforce it, is misled.

**Fix.** The *code* is defensible as-is and should probably stay: `BronzeRow` carries a Kafka
coordinate, the poller's rows have none, and forcing them through a shared writer would mean
inventing fields the poller cannot observe — the same argument that keeps `op` and `before` out of
`poll_rows` (§10). So correct the claim rather than the structure:

> `writer.py` is the only writer of `bronze.raw_*` — the CDC tables. The poller owns
> `bronze.poll_rows` and `bronze.poll_watermarks`, because its rows have no Kafka coordinate.

Two places to change: the `writer.py` docstring, and the `src/fleet_telemetry/load/` row in
`CLAUDE.md`'s module-boundaries table.

---

## 10. Not bugs — deliberate, and please do not "fix" them

Every item here looks like a defect and is a design decision with a comment explaining it. They are
listed because a reader working through §1-§8 will be in exactly the frame of mind to remove them.

| Looks broken | Why it stays |
| --- | --- |
| The poller never sees a `DELETE` | The single cleanest argument for CDC. Reproduced by `test_a_poller_can_never_see_a_delete`. |
| Two changes between polls collapse into one | Failure 2. A poller reads state, not changes; no frequency fixes it. |
| The poller uses a strict `>`, which can drop a row sharing a timestamp | The alternative re-reads the boundary row forever. Stated rather than hidden behind `>=` plus a dedup step. |
| `bronze.poll_rows` has no `op` and no `before` column | A poller cannot observe either. Inventing them would be inference dressed as measurement. **The missing columns are the finding.** |
| Every scalar generated column in `bronze.*` is `text`, including the numeric ones | A cast inside `GENERATED ALWAYS` runs on INSERT, so one `"banana"` fails the whole batch — bronze rejecting the malformed evidence it exists to keep. Silver casts. |
| `DELETE /vehicles/{id}` is a hard delete | A soft delete has an `updated_at`, so a watermark poller could see it. The hard delete is what makes it invisible. |
| No unique constraint on `(vehicle_id, sequence_no)` | A gap proves loss; anything that lets the counter rewind destroys that signal. |
| `pings` and `job_events` stay at `REPLICA IDENTITY DEFAULT` | Append-only, so no before-image exists to describe — and `pings` is where the extra WAL would cost something. Only the four mutable tables need `FULL`. See §1. |
| `--delete` leaves the replication slot behind | So the connector can be recreated and resume. The gap is the missing tooling around it, not the default. See §5. |

---

## 11. Housekeeping

Not defects; loose ends that will be harder to reconstruct later.

- Issues **#6, #7, #8, #9, #10** are still open on GitHub despite being implemented and merged.
- Branch `phase-2/batch-poller` is orphaned — commit `ec504e6` is not an ancestor of `main`; the
  merged CDC work supersedes it.
- An open question deferred with the poller: whether `capture_run_stats.cycles` should be `units`,
  so the same column can count polls and Kafka batches. Deferred until the CDC consumer's stats
  shape is settled.
