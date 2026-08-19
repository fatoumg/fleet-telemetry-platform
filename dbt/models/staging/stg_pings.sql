-- --------------------------------------------------------------------------------------
-- stg_pings.  GRAIN: one row per ping_id.
-- --------------------------------------------------------------------------------------
--
-- Reads:  bronze.raw_ping_events
-- Feeds:  nothing in dbt yet. silver_manual.ping_quality and silver_manual.vehicle_day read the
--         hand-written twin; moving those two to Gold is a separate ticket.
--
-- Ported from sql/silver/10_stg_pings.sql, which is kept rather than deleted. Column names,
-- order and types are identical on purpose, so the diff is a one-liner:
--
--   SELECT * FROM silver_manual.stg_pings EXCEPT SELECT * FROM silver.stg_pings;
--
-- Silver's four jobs, per the design spec (section 9): deduplicate on ping_id, type, normalise
-- units, construct geometry. Nothing else. Clock-skew CORRECTION is Gold's, deliberately -- it
-- is an estimate whose method will be revised, and revising it must not mean reprocessing every
-- raw row. Recording the observed lateness is not correction, so it stays here.
--
-- WHY EVERY COLUMN NEEDS A CAST.
-- Bronze projects text, including the obviously numeric columns, because a cast inside
-- GENERATED ALWAYS is evaluated on INSERT -- so one device sending a ping_id of "banana" would
-- fail the whole batch, and Bronze would reject precisely the malformed evidence it exists to
-- keep (src/fleet_telemetry/load/schema.py:18-27). Silver is where the cast belongs.
--
-- AND WHY THAT CAST NO LONGER TAKES THE WHOLE LAYER DOWN.
-- There is still no TRY_CAST in Postgres 16, so a value that fails to cast still aborts this
-- model. What changed is the blast radius. The hand-written runner executed nine scripts in
-- filename order in one process: a failure left one table committed, one rolled back to its
-- PREVIOUS contents, and seven never attempted -- with nothing anywhere recording which was
-- which (docs/silver-by-hand.md, section 4b). dbt fails this model, skips only its descendants,
-- and still builds every sibling. So the promise in dbt/models/staging/_sources.yml:14-15 --
-- "a bad value is a failing test rather than lost data" -- assumed a framework that could fail
-- one model and carry on, and silver-by-hand.md section 6 recorded that no such framework
-- existed. Now one does. That is the change dbt actually delivers here; the cast itself is
-- unchanged.
--
-- The two exclusions below are therefore about MISSING values, not malformed ones -- which is
-- what Bronze actually contains.

WITH usable AS (
    SELECT *
      FROM {{ source('bronze', 'raw_ping_events') }}
     -- parse_error non-null means the bytes never became JSON, so there is no after-image to
     -- read. A null after-image on an append-only table means something stranger: an op of 'u'
     -- or 'd' on a table that is only ever inserted into. Both are recorded in
     -- stg_rejected_rows.
     --
     -- CAREFUL, AND THIS IS NOT WHAT IT LOOKS LIKE: `payload -> 'after' IS NOT NULL` is TRUE for
     -- a Debezium delete. Debezium emits `"after": null` -- the key is present with a JSON null
     -- value -- and `->` returns jsonb 'null', which is not SQL NULL. Measured on a real delete
     -- at offset 172808: `payload -> 'after' IS NULL` evaluates to FALSE. So this predicate is
     -- dead code for every Debezium payload, and `ping_id IS NOT NULL` below is what actually
     -- excludes deletes -- because #>> through a JSON null does yield SQL NULL.
     --
     -- Kept verbatim rather than corrected, so the EXCEPT diff against silver_manual stays a
     -- comparison of logic rather than of two different filters. The consequence is visible in
     -- stg_rejected_rows.sql, where it mislabels the reason, and it is recorded in
     -- docs/silver-in-dbt.md. The correct predicate is jsonb_typeof(payload -> 'after') = 'null'.
     WHERE parse_error IS NULL
       AND payload -> 'after' IS NOT NULL
       AND ping_id IS NOT NULL
),

deduplicated AS (
    -- DISTINCT ON keeps the FIRST row per ping_id under this ORDER BY: the earliest observation.
    --
    -- Why earliest rather than latest. pings is append-only in the OLTP -- inserted with
    -- ON CONFLICT (ping_id) DO NOTHING, never updated, never deleted -- so a ping_id can appear
    -- at most twice in Bronze: once as the streamed create (op='c'), and once more as a
    -- snapshot re-read (op='r') if the connector is ever re-registered against a fresh offset.
    -- The re-read always arrives later, so earliest-offset necessarily selects the create over
    -- the re-read. "Prefer op='c'" and "lowest offset" pick the same row -- today. They stop
    -- agreeing the moment this stops being an append-only table, which is why the reasoning is
    -- written here rather than the rule alone.
    --
    -- MEASURED, AND UNCOMFORTABLE: bronze holds 173,062 ping rows and no duplicate ping_ids, so
    -- this deduplication currently removes NOTHING. The query is byte-for-byte identical in
    -- output whether the rule is right, reversed, or absent, so no amount of looking at the
    -- result can tell you.
    --
    -- THE GRAIN ASSERTION DOES NOT CLOSE THAT GAP EITHER, and this is the thing worth
    -- understanding. tests/assert_stg_pings_unique_on_ping_id.sql proves the output holds no
    -- duplicate ping_id. It does NOT prove the RIGHT row survived: reverse this ORDER BY and the
    -- output still has one row per ping_id, so the grain test still passes while the model
    -- silently prefers a later re-snapshot over the original streamed create.
    --
    -- _unit_tests.yml is what closes it. Two synthetic bronze rows sharing a ping_id at offsets
    -- 10 and 99, asserting offset 10 survives -- a duplicate that reality has not supplied.
    -- Reverse this ORDER BY and that test fails while the grain test passes. That contrast is
    -- the whole argument for this ticket.
    --
    -- Ordering on (_kafka_partition, _kafka_offset), not the offset alone: the offset is only
    -- unique per partition, and this topic having one partition is a configuration choice
    -- (docker/debezium/fleet-connector.json:20), not a property of Kafka. The wider key costs
    -- nothing and does not inherit the assumption.
    SELECT DISTINCT ON (ping_id) *
      FROM usable
     ORDER BY ping_id, _kafka_partition, _kafka_offset
)

SELECT
    ping_id::uuid                                   AS ping_id,
    (payload #>> '{after,vehicle_id}')::integer     AS vehicle_id,
    sequence_no::bigint                             AS sequence_no,

    -- Both timestamps carry an explicit Z in the payload
    -- ("2026-08-11T10:52:49.599265Z"), so this cast is independent of the session TimeZone.
    -- That matters more than it looks: docker/warehouse/init.sql sets the database to UTC, but
    -- CI never runs that file -- it creates the schemas with psql instead -- so a cast that
    -- depended on the session default would behave differently in CI than locally.
    device_ts::timestamptz                          AS device_ts,
    server_ts::timestamptz                          AS server_ts,

    -- Observed lateness, not corrected lateness. server_ts is ours and trustworthy; device_ts
    -- is client-controlled and is not. The difference is evidence ABOUT the device, so it is
    -- arithmetic on two stored columns rather than a judgement, and Silver may hold it.
    -- Measured baseline for the backfill: p50 34.0s, p99 67.0s
    -- (docs/source-system-reference.md, section 3) -- and 10 rows measured NEGATIVE in
    -- continuous mode, because the host clock is not monotonic (docs/silver-by-hand.md, s3).
    EXTRACT(EPOCH FROM (server_ts::timestamptz - device_ts::timestamptz))
                                                    AS lateness_seconds,

    (payload #>> '{after,latitude}')::double precision  AS latitude,
    (payload #>> '{after,longitude}')::double precision AS longitude,

    -- Nullable in the OLTP and measured 0% null in phase 1
    -- (docs/source-system-reference.md:205-216: "a nullable column that happens to be full
    -- today is a promise nobody made"). #>> yields SQL NULL for a JSON null, so the cast of a
    -- missing value is NULL rather than an error -- no COALESCE, because inventing a zero speed
    -- would be a measurement nobody took.
    (payload #>> '{after,speed_kmh}')::double precision   AS speed_kmh,
    (payload #>> '{after,heading_deg}')::double precision AS heading_deg,

    -- ST_MakePoint takes LONGITUDE FIRST. Swapping them is the classic GIS bug and it does not
    -- error -- it silently relocates The Gambia (13.4N, 16.6W) into the Atlantic off Somalia.
    -- 4326 is WGS84, the SRID the raw degrees are already in
    -- (docker/oltp/init.sql:14-17: constructing geometry from these columns is Silver's job).
    --
    -- This is the exact token the silent break moved: swapped, it passed nine of nine scripts
    -- with exit 0 and shifted fleet total distance +2.14% (34,154.8 -> 34,885.9 km), with every
    -- row count identical and the latitude/longitude columns above still perfectly correct.
    -- Now asserted by tests/assert_stg_pings_geometry_matches_its_coordinates.sql.
    ST_SetSRID(
        ST_MakePoint(
            (payload #>> '{after,longitude}')::double precision,
            (payload #>> '{after,latitude}')::double precision
        ),
        4326
    )                                               AS position,

    -- Provenance, carried so any Silver row can be traced to the Bronze message behind it.
    op                                              AS bronze_op,
    _topic                                          AS bronze_topic,
    _kafka_partition                                AS bronze_partition,
    _kafka_offset                                   AS bronze_offset,
    _ingested_at                                    AS bronze_ingested_at
  FROM deduplicated
