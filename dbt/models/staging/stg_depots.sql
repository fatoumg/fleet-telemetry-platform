-- --------------------------------------------------------------------------------------
-- stg_depots.  GRAIN: one row per depot_id, current state.
-- --------------------------------------------------------------------------------------
--
-- Reads:  bronze.raw_cdc_entities WHERE source_table = 'depots'
-- Feeds:  nothing in dbt yet. silver_manual.vehicle_day reads the hand-written twin of this
--         table; moving that model to Gold is a separate ticket.
--
-- Ported from sql/silver/21_stg_depots.sql, which is kept rather than deleted -- the same
-- treatment poller.py got in phase 2. That file targets silver_manual, this one targets silver,
-- the model names are identical, and the diff between them is therefore a one-liner:
--
--   SELECT * FROM silver_manual.stg_depots EXCEPT SELECT * FROM silver.stg_depots;
--
-- Which is why nothing here may be tidied on the way across. Rename or reorder a single column
-- and that check stops compiling.
--
-- Same shape as stg_vehicles.sql; the reasoning about latest-wins, the bigint cast on
-- source_ts_ms and the before-image caveat now lives in the shared scaffolding macro,
-- dbt/macros/cdc.sql, which both this model and stg_vehicles.sql call. Two differences worth
-- noting.
--
-- FIRST: depots carries geometry. Silver constructs it (design spec section 9), so the same
-- lon-before-lat rule as stg_pings.sql applies -- and here it is checkable by eye, because
-- there are only eight depots and they are all in The Gambia: latitude near 13.4, longitude
-- near -16.6 to -14.2 (docker/oltp/init.sql:60-68).
--
-- SECOND: depots is the table that changes almost never. It is still modelled and still polled,
-- because "changes rarely" and "never changes" are different claims and only one of them is safe
-- to build on (src/fleet_telemetry/ingest/poller.py:38-40).
--
-- WHAT dbt CHANGED, beyond the syntax. The hand-written script opened with DROP TABLE IF EXISTS
-- and rebuilt a table; this is a view, declared once in dbt/dbt_project.yml:19-23 rather than
-- per-file. That is not cosmetic: CREATE TABLE AS registers no catalog dependency, so the
-- hand-written 21 could drop its table while 50_vehicle_day still held rows derived from it
-- (docs/silver-by-hand.md, section 7). A view cannot go stale, and ref() makes the dependency
-- something Postgres and dbt both know about.
--
-- The reject INSERT that used to live at the foot of this file has moved to
-- stg_rejected_rows.sql. A dbt model is one SELECT owning one relation, so eight writers into
-- one reject table could not survive the port.

WITH usable AS (
    {{ cdc_usable('depots', 'depot_id') }}
),

latest AS (
    {{ cdc_latest() }}
)

SELECT
    entity_key::integer                                     AS depot_id,
    ("after" ->> 'name')                                    AS name,
    ("after" ->> 'latitude')::double precision              AS latitude,
    ("after" ->> 'longitude')::double precision             AS longitude,

    -- Longitude first. See stg_pings.sql -- swapping them does not error, it moves the depot
    -- into the Atlantic. Now asserted by
    -- tests/assert_stg_depots_geometry_matches_its_coordinates.sql, which is new here: the
    -- hand-written layer had no such check, and the equivalent swap upstream cost 2.14% of
    -- fleet distance while every script reported ok (docs/silver-by-hand.md, section 5).
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
 WHERE op <> 'd'
