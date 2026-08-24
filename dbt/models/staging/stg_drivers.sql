-- --------------------------------------------------------------------------------------
-- stg_drivers.  GRAIN: one row per driver_id, current state.
-- --------------------------------------------------------------------------------------
--
-- Reads:  bronze.raw_cdc_entities WHERE source_table = 'drivers'
-- Feeds:  nothing yet. Modelled because the entity exists, not because a consumer asked.
--
-- Ported from sql/silver/22_stg_drivers.sql. Same shape as stg_vehicles.sql; the reasoning about
-- latest-wins, the source_ts_ms::bigint cast and the before-image caveat now lives in the shared
-- scaffolding macro, dbt/macros/cdc.sql, which both this model and stg_vehicles.sql call.
--
-- `phone` is nullable in the OLTP and measured 0% null, so nothing in phase 1 exercises the null
-- path. Kept as a plain cast-free text column rather than coalesced to '': an absent phone
-- number and an empty phone number are different facts, and only one of them is true.
--
-- Worth knowing while reading this: the profiler measured drivers as 0 of 40 rows changed since
-- creation (docs/source-system-reference.md, section 7). The table is currently a copy of the
-- snapshot. That is a fact about the simulator's behaviour, not about the model -- and it means
-- the latest-wins rule below is, like the ping deduplication, currently unexercised by real
-- data. Unlike the ping rule, no unit test pins it yet; it would need one before anything
-- downstream depends on version ordering.

WITH usable AS (
    {{ cdc_usable('drivers', 'driver_id') }}
),

latest AS (
    {{ cdc_latest() }}
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
 WHERE op <> 'd'
