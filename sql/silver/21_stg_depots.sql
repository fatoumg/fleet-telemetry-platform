-- --------------------------------------------------------------------------------------
-- 21 -- stg_depots.  GRAIN: one row per depot_id, current state.
-- --------------------------------------------------------------------------------------
--
-- Reads:  bronze.raw_cdc_entities WHERE source_table = 'depots'
-- Feeds:  50_vehicle_day.sql
--
-- Same shape as 20_stg_vehicles.sql; the reasoning about latest-wins, the bigint cast on
-- source_ts_ms and the before-image caveat all lives there. Two differences worth noting.
--
-- FIRST: depots carries geometry. Silver constructs it (design spec section 9), so the same
-- lon-before-lat rule as 10_stg_pings.sql applies -- and here it is checkable by eye, because
-- there are only eight depots and they are all in The Gambia: latitude near 13.4, longitude
-- near -16.6 to -14.2 (docker/oltp/init.sql:60-68).
--
-- SECOND: depots is the table that changes almost never. It is still modelled and still polled,
-- because "changes rarely" and "never changes" are different claims and only one of them is safe
-- to build on (src/fleet_telemetry/ingest/poller.py:38-40).

DROP TABLE IF EXISTS silver_manual.stg_depots;

CREATE TABLE silver_manual.stg_depots AS
WITH usable AS (
    SELECT *,
           coalesce(payload #>> '{after,depot_id}', payload #>> '{before,depot_id}') AS entity_key
      FROM bronze.raw_cdc_entities
     WHERE source_table = 'depots'
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
    entity_key::integer                                     AS depot_id,
    ("after" ->> 'name')                                    AS name,
    ("after" ->> 'latitude')::double precision              AS latitude,
    ("after" ->> 'longitude')::double precision             AS longitude,

    -- Longitude first. See 10_stg_pings.sql -- swapping them does not error, it moves the
    -- depot into the Atlantic.
    ST_SetSRID(
        ST_MakePoint(
            ("after" ->> 'longitude')::double precision,
            ("after" ->> 'latitude')::double precision
        ),
        4326
    )                                                       AS location,

    ("after" ->> 'created_at')::timestamptz                 AS created_at,
    ("after" ->> 'updated_at')::timestamptz                 AS updated_at,

    op                                                      AS bronze_op,
    source_ts_ms::bigint                                    AS source_ts_ms,
    _kafka_offset                                           AS bronze_offset
  FROM latest
 WHERE op <> 'd';

INSERT INTO silver_manual.rejected_rows
    (source_table, reason, _topic, _kafka_partition, _kafka_offset, payload)
SELECT 'depots',
       CASE
           WHEN parse_error IS NOT NULL THEN 'parse_error: ' || parse_error
           ELSE 'no depot_id in either image'
       END,
       _topic, _kafka_partition, _kafka_offset, payload
  FROM bronze.raw_cdc_entities
 WHERE source_table = 'depots'
   AND (
        parse_error IS NOT NULL
        OR coalesce(payload #>> '{after,depot_id}', payload #>> '{before,depot_id}') IS NULL
   );
