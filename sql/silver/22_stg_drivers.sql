-- --------------------------------------------------------------------------------------
-- 22 -- stg_drivers.  GRAIN: one row per driver_id, current state.
-- --------------------------------------------------------------------------------------
--
-- Reads:  bronze.raw_cdc_entities WHERE source_table = 'drivers'
-- Feeds:  nothing yet. Modelled because the entity exists, not because a consumer asked.
--
-- Same shape as 20_stg_vehicles.sql; reasoning lives there.
--
-- `phone` is nullable in the OLTP and measured 0% null, so nothing in phase 1 exercises the null
-- path. Kept as a plain cast-free text column rather than coalesced to '': an absent phone
-- number and an empty phone number are different facts, and only one of them is true.
--
-- Worth knowing while reading this: the profiler measured drivers as 0 of 40 rows changed since
-- creation (docs/source-system-reference.md, section 7). The table is currently a copy of the
-- snapshot. That is a fact about the simulator's behaviour, not about the model.

DROP TABLE IF EXISTS silver_manual.stg_drivers;

CREATE TABLE silver_manual.stg_drivers AS
WITH usable AS (
    SELECT *,
           coalesce(
               payload #>> '{after,driver_id}',
               payload #>> '{before,driver_id}'
           ) AS entity_key
      FROM bronze.raw_cdc_entities
     WHERE source_table = 'drivers'
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
    entity_key::integer                             AS driver_id,
    ("after" ->> 'full_name')                       AS full_name,
    ("after" ->> 'phone')                           AS phone,
    ("after" ->> 'status')                          AS status,
    ("after" ->> 'home_depot_id')::integer          AS home_depot_id,
    ("after" ->> 'created_at')::timestamptz         AS created_at,
    ("after" ->> 'updated_at')::timestamptz         AS updated_at,

    op                                              AS bronze_op,
    source_ts_ms::bigint                            AS source_ts_ms,
    _kafka_offset                                   AS bronze_offset
  FROM latest
 WHERE op <> 'd';

INSERT INTO silver_manual.rejected_rows
    (source_table, reason, _topic, _kafka_partition, _kafka_offset, payload)
SELECT 'drivers',
       CASE
           WHEN parse_error IS NOT NULL THEN 'parse_error: ' || parse_error
           ELSE 'no driver_id in either image'
       END,
       _topic, _kafka_partition, _kafka_offset, payload
  FROM bronze.raw_cdc_entities
 WHERE source_table = 'drivers'
   AND (
        parse_error IS NOT NULL
        OR coalesce(payload #>> '{after,driver_id}', payload #>> '{before,driver_id}') IS NULL
   );
