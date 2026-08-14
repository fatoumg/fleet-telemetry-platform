-- --------------------------------------------------------------------------------------
-- 10 -- stg_pings.  GRAIN: one row per ping_id.
-- --------------------------------------------------------------------------------------
--
-- Reads:  bronze.raw_ping_events
-- Feeds:  40_ping_quality.sql, 50_vehicle_day.sql
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
-- AND WHY THAT CAST CAN STILL TAKE THE WHOLE LAYER DOWN.
-- There is no TRY_CAST in Postgres 16. If a value ever fails to cast, this script aborts, its
-- table rolls back to the previous run's contents, and every script after it never runs. The
-- promise in dbt/models/staging/_sources.yml:14-15 -- "a bad value is a failing test rather
-- than lost data" -- assumes a framework that can fail one model and carry on. There isn't one
-- yet. That limit is a finding, recorded in docs/silver-by-hand.md rather than papered over
-- with per-column validation: nothing can currently produce such a value, because Debezium
-- renders every field here from an already-typed OLTP column (docker/oltp/init.sql:249-259),
-- and building the defence before the problem exists is what this phase is arguing against.
--
-- The two exclusions below are therefore about MISSING values, not malformed ones -- which is
-- what Bronze actually contains.

DROP TABLE IF EXISTS silver_manual.stg_pings;

CREATE TABLE silver_manual.stg_pings AS
WITH usable AS (
    SELECT *
      FROM bronze.raw_ping_events
     -- parse_error non-null means the bytes never became JSON, so there is no after-image to
     -- read. A null after-image on an append-only table means something stranger: an op of 'u'
     -- or 'd' on a table that is only ever inserted into. Both are recorded in 00's table.
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
    -- MEASURED, AND UNCOMFORTABLE: at the time of writing Bronze holds 350,742 ping rows and
    -- 350,742 distinct ping_ids. This deduplication currently removes NOTHING. The query is
    -- byte-for-byte identical in output whether the rule is right or wrong, so no amount of
    -- looking at the result can tell you. Only an assertion on the grain can -- which is the
    -- single most direct argument for issue #12 that this file contains.
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
    -- (docs/source-system-reference.md, section 3).
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
  FROM deduplicated;

-- --------------------------------------------------------------------------------------
-- what was left out, and why
-- --------------------------------------------------------------------------------------

INSERT INTO silver_manual.rejected_rows
    (source_table, reason, _topic, _kafka_partition, _kafka_offset, payload)
SELECT 'raw_ping_events',
       CASE
           WHEN parse_error IS NOT NULL THEN 'parse_error: ' || parse_error
           -- An op of 'u' or 'd' reaching an append-only table is itself the finding, not
           -- something to handle (dbt/models/staging/_sources.yml:50-54).
           WHEN payload -> 'after' IS NULL THEN 'no after-image, op=' || coalesce(op, 'null')
           ELSE 'ping_id is null'
       END,
       _topic, _kafka_partition, _kafka_offset, payload
  FROM bronze.raw_ping_events
 WHERE parse_error IS NOT NULL
    OR payload -> 'after' IS NULL
    OR ping_id IS NULL;
