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
| 3 | Integration tests depend on state they do not own (three cases) | medium | open |
| 4 | The watermark leak fires under the ordinary clock | medium — doc claim is wrong | open |
| 5 | The replication slot has no lifecycle owner | medium — can fill the disk | open |
| 6 | `DELETE /vehicles/{id}` returns 500 for every seeded vehicle | medium | open, already written up |
| 7 | Debezium connects as the table owner | low here, ships badly | open |
| 8 | Misconfigured tooling in tracked files: `PostToolUse` hooks, and a `.gitignore` typo that made CI uneditable | low — noise, and one blocked edit | hooks open; `.gitignore` **fixed** |
| 9 | `writer.py` claims to be the only writer of `bronze.*`, and is not | low — doc claim was wrong | **fixed** |
| 12 | 20,530 ping events permanently lost while the CDC consumer was down | **high** — unrecoverable data loss | data unrecoverable; operational rule is the fix |

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

### 3b. A second case, same cause: the test that fails whenever the pipeline is running

**Symptom.** `tests/test_bronze_load.py::test_a_rewound_consumer_group_does_not_duplicate_bronze`
fails with:

```text
cimpl.KafkaException: KafkaError{code=UNKNOWN_MEMBER_ID,val=25,
                                 str="Commit failed: Broker: Unknown member"}
```

**Cause.** The test rewinds the consumer group by committing offsets from a client that never joined
it. That is only permitted while the group has no active member. Measured during the phase 3 work,
with a CDC consumer left running from an earlier session:

```text
GROUP        bronze-loader
STATE        Stable
MEMBERS      1
TOTAL-LAG    0
```

One live member, so an outside commit is refused — correctly, by Kafka's protocol.

**Cost.** The test passes only when no consumer is running, which is the opposite of this project's
normal operating state: the consumer is a daemon, and the README tells you to start it. So the suite
is green on a machine where the pipeline is idle and red on one where it works, and the failure
message points at Kafka membership rather than at the test's assumption.

Same family as §3 and §2: **a test that depends on state it does not own.** Three instances now, in
three different test files.

**Fix.** Have the test own the group it rewinds — a `group.id` unique to the test run, seeded by
consuming a few messages under that group and then rewinding it. It costs one extra consume and
removes the dependency on what else happens to be running.

### 3c. A third case, and the one where two defects compound

**Symptom.** `tests/test_poller.py::test_a_poller_can_never_see_a_delete` fails intermittently:

```text
assert landed["vehicles"] == 0, "a poller reporting a delete would mean the test is wrong"
```

It passes when run alone. It failed in a full-suite run at 13:52 on 2026-08-13.

**Cause.** The simulator was running, and it reassigns vehicles between depots as part of normal
operation — measured **3 changes in the hour** around that run, one of them vehicle 9 at
`13:51:27.940369`, inside the test's window. The poller correctly reported one changed vehicle. The
assertion reads that as "the poller saw the delete", because it counts rows landed for the *whole
vehicles table* rather than for the vehicle the test created. The test's own second assertion — a
count scoped to `vehicle_id = 9401` — is the one that actually proves the claim, and it passed.

**And this is where §2 makes §3 worse.** The window is not milliseconds: the fixture's unqualified
`delete from bronze.poll_watermarks` resets the watermark to `EPOCH`, so the first `poll_once` in the
file drains the entire source system — ~40 s at current volume. At 3 vehicle changes an hour, a 40 s
window carries roughly a 3% chance of catching one per run. Fix §2 and this flake becomes rare
without being fixed; fix this and it goes away regardless.

**Cost.** An intermittent red suite whose message actively misdirects: it says the poller saw a
delete, and asserts something the test does not need. A developer's first instinct will be to doubt
the poller.

**Fix.** Assert on the vehicle the test owns, not on the table:

```text
assert landed["vehicles"] == 0     <- scope it to vehicle 9401, or drop it
```

The scoped count already in the test is sufficient; the table-wide assertion should go. Same root
cause as 3 and 3b — **a test that depends on state it does not own** — except here the state is the
system under test behaving normally, which is the hardest version to notice.

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

### 8b. A `.gitignore` typo that made CI uneditable

**Symptom.** Staging a change to the CI workflow is refused:

```text
$ git add .github/workflows/ci.yml
The following paths are ignored by one of your .gitignore files:
.github
hint: Use -f if you really want to add them.
```

`ci.yml` is tracked. It is in `HEAD`. `git ls-files .github/` lists it. It still cannot be staged.

**Cause.** `.gitignore` carried the bare line `.github` under a comment block describing *Claude
Code local agent settings* — a typo for `.claude`. Tracked files are normally exempt from
`.gitignore`, which is why this looks impossible, but an ignored **directory** is pruned during
pathspec expansion, so git never descends into `.github/` to notice that the file inside it is
tracked. The refusal names `.github`, not the typo, and not the tracked file it is blocking.

**Cost.** Any change to CI needs `git add -f`, or it appears to be silently skipped — and a
developer who does not read the hint closely will conclude their edit committed when it did not. The
prior write-up of this entry got it wrong in a way worth recording: it said *"`ci.yml` is already
tracked so edits to it commit fine"*, which is exactly the reasonable inference, and it is false.

**Fixed** on `phase-3/silver-by-hand`: the line now reads `.claude/settings.local.json`, which is
what the surrounding comment always described. Note that `.claude/settings.json`, the hooks and the
status line are all tracked and shared, so the block had been ignoring nothing except CI.

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

**Fixed** on the `phase-3/silver-by-hand` branch: both now say `bronze.raw_*` and name the poller as
the owner of `poll_rows` and `poll_watermarks`. The structure is unchanged, deliberately.

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

---

## 12. 20,530 ping events permanently lost while the CDC consumer was down

**Symptom.** `bronze.raw_ping_events` holds Kafka offsets 0 through 377,251 contiguously, then
jumps straight to 397,782. One gap, exactly:

```text
397,782 - 377,251 - 1 = 20,530 missing offsets
```

**Cause.** The CDC consumer (`fleet_telemetry.ingest.consumer`, consumer group `bronze-loader`)
stopped advancing at offset 377,251 on 2026-08-24 and did not restart for roughly 15 hours.
Redpanda's topic retention trimmed offsets 377,252 through 397,781 off the front of the log before
the consumer came back. Confirmed directly:

```text
$ rpk group describe bronze-loader
TOPIC          PARTITION  LOG-START-OFFSET  ...
fleet.public.pings  0     397782             ...
```

`LOG-START-OFFSET` had already moved to 397,782 by the time the consumer resumed — the messages
the consumer needed next did not merely arrive late, they had already been deleted. This is
`CLAUDE.md`'s own warning arriving as measured fact: *"Bronze is not optional. A Kafka topic has a
retention window, not a memory."* A topic's retention window is a promise about how long **unread**
messages survive, not about how long a stopped consumer gets to catch up — and this consumer was
stopped for longer than that promise covers.

**Cost.** Those rows exist in the OLTP (563,102 pings, measured) and will never reach Bronze
(543,288, measured) — two separate totals, not meant to net to exactly the 20,530-row gap above,
since both systems keep taking on new pings continuously and each count is a snapshot of a moving
target. They are gone from the durable record permanently: Bronze has no earlier copy to
re-read, and the OLTP is not the system of record for history — it is current-state-plus-recent,
not an archive. No dbt model, no re-run of `consumer.py`, and no amount of downstream
reprocessing can recover them. Any Silver/Gold aggregate covering that window (2026-08-24, the
outage period) is built on a base that is missing rows, permanently, and nothing in the pipeline
flags which rows those were — the loss is a hole with no label on it.

**What was NOT affected, and this matters for trusting the rest of this ticket.**
`bronze.raw_cdc_entities` for vehicles holds offsets 0 through 143 — 144 rows, contiguous, zero
missing. The four entity streams (`vehicles`, `depots`, `drivers`, `jobs`) that
[`type-2-dimensions.md`](type-2-dimensions.md) is built from were not touched by this outage: their
volume is low enough, and the consumer's downtime happened to fall in a window where nothing on
those streams needed to be read past the point retention trimmed. So `dim_vehicle` and
`dim_driver` are complete on this volume — this incident cost ping history, not the Type 2
dimensions this ticket delivers.

**Severity: high.** This is not a near-miss or a theoretical hazard like §5's unattended
replication slot — it is realized, permanent, unrecoverable loss of primary telemetry data, in the
stream this whole project's central lateness problem is about.

**Status.** The data itself is unrecoverable; there is nothing to "fix" in the sense of getting
those 20,530 rows back. What is actionable is the operational rule the incident argues for:

- The CDC consumer needs to be a supervised, always-running process (the phase 4 argument
  `known-issues.md` §5 already makes for the replication slot applies identically here — the
  failure mode is the same shape, a piece of live state that only survives while something keeps
  draining it).
- Topic retention should be sized against **measured worst-case consumer downtime**, not against an
  optimistic assumption that the consumer is always close to caught up. A retention window long
  enough to survive a multi-hour outage costs disk; a retention window that does not is a bet that
  this incident lost.
- A monitoring signal on `LOG-START-OFFSET` vs. the consumer's committed offset would have caught
  this while some of the window was still recoverable, rather than after.
