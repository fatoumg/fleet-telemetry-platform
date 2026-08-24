-- --------------------------------------------------------------------------------------
-- stg_jobs.  GRAIN: one row per job_id, current state.
-- --------------------------------------------------------------------------------------
--
-- Reads:  bronze.raw_cdc_entities WHERE source_table = 'jobs'
-- Feeds:  nothing yet; stg_job_events is a sibling, not a child.
--
-- Ported from sql/silver/23_stg_jobs.sql. Same shape as stg_vehicles.sql; the reasoning about
-- latest-wins, the source_ts_ms::bigint cast, the before-image caveat and why there is no macro
-- all lives there.
--
-- TWO THINGS ABOUT jobs THAT ARE NOT ABOUT SQL.
--
-- `status` here is the CURRENT status only. The path a job took to reach it lives in
-- stg_job_events, and the two can disagree in an interesting way: a job's newest event can be a
-- transition the jobs row has not caught up to, because they are separate WAL rows in separate
-- topics with no ordering guarantee between them. Reconciling those is a judgement, so it
-- belongs in Gold, not here. Note that dbt does not help with this one -- the two models are
-- siblings in the DAG, so nothing about the lineage graph makes the disagreement visible. It is
-- a modelling fact, not a dependency fact.
--
-- `estimated_duration_minutes` is frozen at creation and never revised
-- (docker/oltp/init.sql:144-146). It is an estimate the source system made once, not a
-- measurement, and anything comparing it to actual duration is doing analysis -- Gold's.
--
-- Right-censoring is live here: 37 of 143 jobs were still open at the phase 1 observation
-- boundary (docs/source-system-reference.md, section 6). A mean duration over this table would
-- silently exclude every job that had not finished yet, which is the classic survivorship error.

WITH usable AS (
    SELECT *,
           coalesce(payload #>> '{after,job_id}', payload #>> '{before,job_id}') AS entity_key
      FROM {{ source('bronze', 'raw_cdc_entities') }}
     WHERE source_table = 'jobs'
       AND parse_error IS NULL
),

latest AS (
    SELECT DISTINCT ON (entity_key) *
      FROM usable
     WHERE entity_key IS NOT NULL
     ORDER BY entity_key,
              source_ts_ms::bigint DESC,
              _kafka_partition DESC,
              _kafka_offset DESC
)

SELECT
    -- bigserial in the OLTP, so bigint here -- not integer, which is what the other four keys
    -- are and what a copy-paste would have made this.
    entity_key::bigint                                          AS job_id,
    -- Nullable: a job can exist before a vehicle is assigned to it.
    ("after" ->> 'vehicle_id')::integer                         AS vehicle_id,
    ("after" ->> 'pickup_depot_id')::integer                    AS pickup_depot_id,
    ("after" ->> 'dropoff_depot_id')::integer                   AS dropoff_depot_id,
    ("after" ->> 'status')                                      AS status,
    ("after" ->> 'estimated_duration_minutes')::integer         AS estimated_duration_minutes,
    ("after" ->> 'requested_at')::timestamptz                   AS requested_at,
    ("after" ->> 'created_at')::timestamptz                     AS created_at,
    ("after" ->> 'updated_at')::timestamptz                     AS updated_at,

    op                                                          AS bronze_op,
    source_ts_ms::bigint                                        AS source_ts_ms,
    _kafka_offset                                               AS bronze_offset
  FROM latest
 WHERE op <> 'd'
