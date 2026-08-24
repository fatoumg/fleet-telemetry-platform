-- --------------------------------------------------------------------------------------
-- Shared scaffolding for the models that read bronze.raw_cdc_entities.
-- --------------------------------------------------------------------------------------
--
-- FIVE MODELS READ THIS TABLE TODAY. Four of them -- stg_vehicles, stg_drivers, stg_depots,
-- stg_jobs -- call the two macros below. The fifth, stg_rejected_rows, reads
-- bronze.raw_cdc_entities directly and calls neither: it needs the raw rows across all four
-- source_tables at once, not one entity's usable/latest pair, so this scaffolding does not fit it.
-- This file did not always exist -- the duplication across the four callers had not bitten, and a
-- tool introduced before its problem is a tool you cannot explain. The Gold ticket is where it
-- bites -- stg_vehicle_versions and stg_driver_versions need the `usable` half and must NOT have
-- the `latest` half, so the two CTEs stop being one block that is always copied together and
-- become two independently-chosen pieces. After that ticket lands, seven models read this table
-- and six of them call this macro.
--
-- FOUR ENTITIES SHARE ONE BRONZE TABLE, distinguished only by source_table -- and there is no
-- CHECK and no index on that column (src/fleet_telemetry/load/schema.py:81-95). So every caller
-- is a full scan of the same relation. At ~2,000 changes per 30 days that is free; it is worth
-- noticing anyway, because it is the kind of thing that is free until it is not.

{% macro cdc_usable(entity_table, key_field) -%}
{%- set after_path = "{after," ~ key_field ~ "}" -%}
{%- set before_path = "{before," ~ key_field ~ "}" -%}
    SELECT *,
           -- A delete carries no after-image, so the key must come from the before-image.
           -- COALESCE in this order because after wins whenever it exists.
           --
           -- CAUTION on the before-image generally: the four mutable tables are REPLICA
           -- IDENTITY FULL, and on any volume created before 2026-08-12 those ALTERs never ran,
           -- so before-images hold Debezium's type DEFAULTS rather than the old row -- plate '',
           -- capacity 0, created_at 1970-01-01 (docs/known-issues.md, section 1). The primary
           -- key is the one field that is real in that case, which is all this COALESCE needs.
           -- Measured on this volume: relreplident is 'f' for all four tables, so the ALTERs did
           -- run here and before-images are the real old row, not the fabricated default -- the
           -- caution above is for a volume created before that date, or one where the ALTERs were
           -- never re-applied by hand.
           -- The Gold models built on this macro read NOTHING ELSE from a before-image: a delete
           -- contributes only its source_ts_ms, as the timestamp that closes the previous
           -- version's interval. That is deliberate, and it is what keeps a documented
           -- high-severity defect out of the dimensions. Anything in the Gold ticket that reads
           -- the rest of the before-image inherits the problem -- and it is live: pings, which is
           -- deliberately NOT replica identity full, was measured emitting deletes whose
           -- before-image reads latitude 0.0, longitude 0.0, device_ts 1970-01-01 (see
           -- stg_rejected_rows.sql).
           coalesce(
               payload #>> '{{ after_path }}',
               payload #>> '{{ before_path }}'
           ) AS entity_key
      FROM {{ source('bronze', 'raw_cdc_entities') }}
     WHERE source_table = '{{ entity_table }}'
       AND parse_error IS NULL
{%- endmacro %}


{% macro cdc_latest() -%}
    -- Latest wins here, the opposite of stg_pings.sql -- and for the opposite reason. A ping is
    -- immutable, so the earliest observation of it is the truest one. An entity is mutable, so
    -- only the newest event describes it.
    --
    -- source_ts_ms::bigint, NOT source_ts_ms. It is text in Bronze, and text ordering puts
    -- '9' after '10'. Sorting a 13-digit epoch as text is wrong roughly whenever the digit
    -- count changes, which for millisecond epochs is rare enough to survive every test you
    -- would think to write and then be wrong in production.
    --
    -- NOT USED BY THE VERSION MODELS, and that is the point of splitting it out. DISTINCT ON is
    -- exactly the operation Type 2 history must not perform.
    SELECT DISTINCT ON (entity_key) *
      FROM usable
     WHERE entity_key IS NOT NULL
     ORDER BY entity_key,
              source_ts_ms::bigint DESC,
              _kafka_partition DESC,
              _kafka_offset DESC
{%- endmacro %}
