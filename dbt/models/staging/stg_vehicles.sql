-- --------------------------------------------------------------------------------------
-- stg_vehicles.  GRAIN: one row per vehicle_id, current state.
-- --------------------------------------------------------------------------------------
--
-- Reads:  bronze.raw_cdc_entities WHERE source_table = 'vehicles'
-- Feeds:  nothing in dbt yet. silver_manual.vehicle_day reads the hand-written twin; moving
--         that model to Gold is a separate ticket.
--
-- Ported from sql/silver/20_stg_vehicles.sql. THE REASONING FOR ALL FOUR CDC ENTITY MODELS LIVES
-- HERE; stg_depots, stg_drivers and stg_jobs point back at this file rather than repeating it.
--
-- FOUR ENTITIES SHARE ONE BRONZE TABLE, distinguished only by source_table -- and there is no
-- CHECK and no index on that column (src/fleet_telemetry/load/schema.py:81-95). So every one of
-- these four models is a full scan of the same relation. At ~2,000 changes per 30 days that is
-- free; it is worth noticing anyway, because it is the kind of thing that is free until it is
-- not. What dbt changes is only the wall clock: these are four independent models with no
-- dependency between them, so dbt runs them across its four threads (dbt/profiles.yml:30) where
-- the hand-written runner executed them strictly in filename order.
--
-- NO MACRO FOR THE SHARED SCAFFOLDING, DELIBERATELY. The usable/latest CTEs below are repeated
-- almost verbatim in three sibling files, and extracting them into a dbt macro is the obvious
-- move. Not taken. docs/silver-by-hand.md sections 7 and 1-6 record what the hand-written layer
-- actually cost -- no dependency graph, no tests, no lineage, no state -- and duplication is not
-- on that list. It has not bitten. A tool introduced before its problem is a tool you cannot
-- explain, and that applies to Jinja as much as to Airflow. The macro's moment is the Gold
-- ticket, where Type 2 history triples this scaffolding in all four models at once.
--
-- CURRENT STATE IS THE MODEST CLAIM HERE. Type 2 history -- the full validity range per version,
-- which is what a change stream with before-images actually enables -- is the Gold ticket's.
-- This model answers only "what does the vehicle look like now", which is the same question the
-- batch poller answered. The difference is that this one gets DELETES right, and the poller
-- structurally cannot (docs/learn/02-ingestion.md, the diff).

WITH usable AS (
    SELECT *,
           -- A delete carries no after-image, so the key must come from the before-image.
           -- COALESCE in this order because after wins whenever it exists.
           --
           -- CAUTION on the before-image generally: the four mutable tables are REPLICA
           -- IDENTITY FULL, and on any volume created before 2026-08-12 those ALTERs never ran,
           -- so before-images hold Debezium's type DEFAULTS rather than the old row -- plate '',
           -- capacity 0, created_at 1970-01-01 (docs/known-issues.md, section 1). The primary
           -- key is the one field that is real in that case, which is all this COALESCE needs.
           -- Anything in the Gold ticket that reads the rest of the before-image inherits the
           -- problem -- and it is live: pings, which is deliberately NOT replica identity full,
           -- was measured emitting deletes whose before-image reads latitude 0.0, longitude 0.0,
           -- device_ts 1970-01-01 (see stg_rejected_rows.sql).
           coalesce(
               payload #>> '{after,vehicle_id}',
               payload #>> '{before,vehicle_id}'
           ) AS entity_key
      FROM {{ source('bronze', 'raw_cdc_entities') }}
     WHERE source_table = 'vehicles'
       AND parse_error IS NULL
),

latest AS (
    -- Latest wins here, the opposite of stg_pings.sql -- and for the opposite reason. A ping is
    -- immutable, so the earliest observation of it is the truest one. A vehicle is mutable, so
    -- only the newest event describes it.
    --
    -- source_ts_ms::bigint, NOT source_ts_ms. It is text in Bronze, and text ordering puts
    -- '9' after '10'. Sorting a 13-digit epoch as text is wrong roughly whenever the digit
    -- count changes, which for millisecond epochs is rare enough to survive every test you
    -- would think to write and then be wrong in production.
    SELECT DISTINCT ON (entity_key) *
      FROM usable
     WHERE entity_key IS NOT NULL
     ORDER BY entity_key,
              source_ts_ms::bigint DESC,
              _kafka_partition DESC,
              _kafka_offset DESC
)

SELECT
    entity_key::integer                                    AS vehicle_id,
    ("after" ->> 'plate')                                  AS plate,
    ("after" ->> 'capacity')::integer                      AS capacity,
    ("after" ->> 'status')                                 AS status,
    -- Nullable and reassignable in the OLTP (docker/oltp/init.sql:102-103), measured 0% null.
    ("after" ->> 'current_driver_id')::integer             AS current_driver_id,
    ("after" ->> 'home_depot_id')::integer                 AS home_depot_id,
    ("after" ->> 'created_at')::timestamptz                AS created_at,
    ("after" ->> 'updated_at')::timestamptz                AS updated_at,

    op                                                     AS bronze_op,
    source_ts_ms::bigint                                   AS source_ts_ms,
    _kafka_offset                                          AS bronze_offset
  FROM latest
 -- A vehicle whose newest event is a delete is not in current state. The row is simply absent,
 -- which is the correct answer and the one the poller could never give: it reads what is there,
 -- so a deleted row is invisible to it rather than known-absent. The hand-written
 -- 50_vehicle_day.sql will find no match for such a vehicle, and that is right.
 --
 -- NOTE FOR THE GOLD TICKET: this is also why there is no `relationships` test from
 -- stg_jobs.vehicle_id to this model. DELETE /vehicles/{id} is a hard delete on purpose, and the
 -- phase-2 diff proved two vehicle lifecycles really were deleted, so a referential test at
 -- error severity would fail on correct data. Whether a missing dimension member is dropped or
 -- marked unknown is a presentation judgement, and judgements are Gold's.
 WHERE op <> 'd'
