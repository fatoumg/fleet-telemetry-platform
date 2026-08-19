-- --------------------------------------------------------------------------------------
-- stg_rejected_rows.  GRAIN: one row per rejected Bronze message.
-- --------------------------------------------------------------------------------------
--
-- Reads:  bronze.raw_ping_events, bronze.raw_job_events, bronze.raw_cdc_entities
-- Feeds:  a human, and the two reconciliation tests in dbt/tests/.
--
-- Ported from the eight INSERT statements at the foot of sql/silver/00 through 30. Every model in
-- this layer excludes rows it cannot type; this is where they land, with a reason. That is the
-- only honest way to drop a row: a filtered row that lands nowhere is indistinguishable from a
-- row that never existed, and Bronze went to considerable trouble to keep exactly this evidence
-- (src/fleet_telemetry/load/__init__.py:6-8).
--
-- The Kafka coordinate is kept rather than the row's business key, because a rejected row may
-- have no usable business key -- that is frequently WHY it was rejected. (_topic, partition,
-- offset) identifies the Bronze row exactly and immutably.
--
-- WHY THIS IS ONE MODEL AND WAS EIGHT INSERTS. A dbt model is one SELECT owning one relation, so
-- eight writers into one reject table could not survive the port. The eight branches become
-- three UNION ALL arms here.
--
-- WHAT dbt FIXED, AND WHAT IT DID NOT.
--
-- Fixed: the hand-written 00_rejected_rows.sql had to be named `00` so it ran first, and its own
-- header called that fragile -- rename it to 99_ and every other script fails on a missing table,
-- naming `silver_manual.rejected_rows` rather than the renamed file, so the error points at the
-- symptom. ref() removes that entirely. This model cannot run before or after the wrong thing and
-- renaming the file changes nothing. It also no longer needs a TRUNCATE, and so cannot commit a
-- truncate while the rows it explains stay in place -- an inconsistency the hand-written layer
-- did commit, harmlessly only because the table happened to be empty
-- (docs/silver-by-hand.md, section 4b).
--
-- NOT fixed: the reject predicate is still written twice -- once as each model's `usable` filter,
-- once here -- and nothing structurally prevents them drifting apart. A macro would prevent it.
-- Deliberately not used: this project's habit is to assert the property rather than engineer away
-- the possibility, and the two reconciliation tests in dbt/tests/ are that assertion. If the two
-- copies ever disagree, `bronze in = silver out + rejects` stops balancing and the build fails.
--
-- A LIVE BUG, PORTED VERBATIM ON PURPOSE.
-- The `payload -> 'after' IS NULL` branch below is DEAD CODE and every reason it would have
-- produced is wrong. Debezium emits `"after": null` -- the key is present with a JSON null value
-- -- and `->` returns jsonb 'null', which is not SQL NULL. Verified on a real delete at
-- raw_ping_events offset 172808: `payload -> 'after' IS NULL` evaluates to FALSE.
--
-- So every delete falls through to the ELSE arm and is labelled 'ping_id is null' or
-- 'job_event_id is null', when the truth is 'no after-image, op=d'. Measured: 16 pings and 20
-- job_events, all op='d', all mislabelled. The correct predicate is
-- jsonb_typeof(payload -> 'after') = 'null'.
--
-- Kept as-is so the diff against silver_manual.rejected_rows stays 0 and this port remains a
-- comparison of logic rather than of two different filters. Note what is NOT affected: the SET of
-- rejected rows is identical either way, because the key IS NULL check does the exclusion. Only
-- the human-readable reason is wrong. Recorded in docs/silver-in-dbt.md; fixing it is a
-- follow-up, and fixing it will move 36 rows in that diff.
--
-- AND THE DELETES THEMSELVES ARE THE OTHER FINDING. dbt/models/staging/_sources.yml:50-54 said
-- only c and r were expected on these append-only streams, and that a u or a d arriving "is
-- itself a finding rather than something to handle". It arrived. The before-images also confirm
-- the fabricated-defaults pathology exactly as documented: latitude 0.0, longitude 0.0,
-- device_ts 1970-01-01, because pings is deliberately left at the default REPLICA IDENTITY, so
-- ping_id is the only real field in them.

WITH ping_rejects AS (
    SELECT 'raw_ping_events'                                       AS source_table,
           CASE
               WHEN parse_error IS NOT NULL THEN 'parse_error: ' || parse_error
               -- Dead branch. See the header.
               WHEN payload -> 'after' IS NULL
                   THEN 'no after-image, op=' || coalesce(op, 'null')
               ELSE 'ping_id is null'
           END                                                     AS reason,
           _topic,
           _kafka_partition,
           _kafka_offset,
           payload
      FROM {{ source('bronze', 'raw_ping_events') }}
     WHERE parse_error IS NOT NULL
        OR payload -> 'after' IS NULL
        OR ping_id IS NULL
),

job_event_rejects AS (
    SELECT 'raw_job_events'                                        AS source_table,
           CASE
               WHEN parse_error IS NOT NULL THEN 'parse_error: ' || parse_error
               -- Dead branch. See the header.
               WHEN payload -> 'after' IS NULL
                   THEN 'no after-image, op=' || coalesce(op, 'null')
               ELSE 'job_event_id is null'
           END                                                     AS reason,
           _topic,
           _kafka_partition,
           _kafka_offset,
           payload
      FROM {{ source('bronze', 'raw_job_events') }}
     WHERE parse_error IS NOT NULL
        OR payload -> 'after' IS NULL
        OR payload #>> '{after,job_event_id}' IS NULL
),

entity_keyed AS (
    -- Four entities share one Bronze table and each declares a different primary key. The
    -- hand-written layer repeated that mapping across four separate INSERT statements (20-23),
    -- which is four places for it to drift; joining a VALUES list puts it in one.
    --
    -- The join is INNER, so a raw_cdc_entities row whose source_table is none of these four is
    -- invisible here -- a fifth topic arriving would be neither modelled nor rejected nor
    -- counted. That is a real blind spot rather than a safe assumption, and it is stated rather
    -- than relied on. The connector currently publishes exactly these four
    -- (docker/debezium/fleet-connector.json).
    SELECT e.source_table,
           e.parse_error,
           e._topic,
           e._kafka_partition,
           e._kafka_offset,
           e.payload,
           k.key_field,
           coalesce(e."after" ->> k.key_field, e."before" ->> k.key_field) AS entity_key
      FROM {{ source('bronze', 'raw_cdc_entities') }} e
      JOIN (VALUES ('vehicles', 'vehicle_id'),
                   ('depots',   'depot_id'),
                   ('drivers',  'driver_id'),
                   ('jobs',     'job_id')) AS k(source_table, key_field)
        ON k.source_table = e.source_table
),

entity_rejects AS (
    -- No dead branch here, and that asymmetry is not an oversight: the entity models never
    -- excluded rows for a missing after-image. A delete is expected on these tables and its key
    -- comes from the before-image, so the only rejections are an unparseable payload or a key
    -- absent from BOTH images.
    SELECT source_table,
           CASE
               WHEN parse_error IS NOT NULL THEN 'parse_error: ' || parse_error
               ELSE 'no ' || key_field || ' in either image'
           END                                                     AS reason,
           _topic,
           _kafka_partition,
           _kafka_offset,
           payload
      FROM entity_keyed
     WHERE parse_error IS NOT NULL
        OR entity_key IS NULL
)

SELECT * FROM ping_rejects
UNION ALL
SELECT * FROM job_event_rejects
UNION ALL
SELECT * FROM entity_rejects
