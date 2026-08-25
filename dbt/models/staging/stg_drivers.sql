-- --------------------------------------------------------------------------------------
-- stg_drivers.  GRAIN: one row per driver_id, current state.
-- --------------------------------------------------------------------------------------
--
-- Reads:  bronze.raw_cdc_entities WHERE source_table = 'drivers'
-- Feeds:  dbt/snapshots/snap_drivers.sql, which polls this model as the naive-comparison baseline
--         for gold.dim_driver (docs/type-2-dimensions.md).
--
-- Ported from sql/silver/22_stg_drivers.sql. Same shape as stg_vehicles.sql; the reasoning about
-- latest-wins, the source_ts_ms::bigint cast and the before-image caveat now lives in the shared
-- scaffolding macro, dbt/macros/cdc.sql, which both this model and stg_vehicles.sql call.
--
-- `phone` is nullable in the OLTP and measured 0% null in current state -- not because no driver
-- has ever gone without one, but because the one driver whose fixture leaves it null (9401, see
-- stg_driver_versions.sql) is hard-deleted as of this writing, so it holds no row here to carry
-- the null forward. Kept as a plain cast-free text column rather than coalesced to '': an absent
-- phone number and an empty phone number are different facts, and only one of them is true.
--
-- Worth knowing while reading this: the profiler measured drivers as 0 of 40 rows changed since
-- creation (docs/source-system-reference.md, section 7) -- true in phase 1, when there was no
-- change stream to exercise latest-wins at all. It is no longer true of this table as a whole:
-- see stg_driver_versions.sql, which measures the churn the integration suite now produces (17 c,
-- 34 u, 17 d, 40 r events against Bronze). It remains true of the original 40 fleet-seeded
-- drivers -- the churn is confined to the high-id test fixtures, which is why latest-wins is now
-- exercised by real data without the fleet itself having changed at all. Unlike the ping rule, no
-- unit test pins this model's latest-wins choice yet -- it would need one now that real data
-- exercises it, more than it did when the rule was still a hypothetical.

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
