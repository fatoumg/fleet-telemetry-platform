-- --------------------------------------------------------------------------------------
-- stg_vehicles.  GRAIN: one row per vehicle_id, current state.
-- --------------------------------------------------------------------------------------
--
-- Reads:  bronze.raw_cdc_entities WHERE source_table = 'vehicles'
-- Feeds:  dbt/snapshots/snap_vehicles.sql, which polls this model as the naive-comparison baseline
--         for gold.dim_vehicle (docs/type-2-dimensions.md). silver_manual.vehicle_day also reads
--         the hand-written twin of this model; moving that model to Gold is a separate ticket.
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
-- THE SHARED SCAFFOLDING NOW LIVES IN A MACRO: dbt/macros/cdc.sql. This paragraph used to argue
-- against that extraction, on the grounds that the duplication across the four sibling files had
-- not yet bitten. Issue #13 is the bite -- stg_vehicle_versions and stg_driver_versions need the
-- `usable` CTE without the `latest` CTE, so the two had to stop being one block that every model
-- copied whole. See cdc.sql for the reasoning that moved with them.
--
-- CURRENT STATE IS THE MODEST CLAIM HERE. Type 2 history -- the full validity range per version,
-- which is what a change stream with before-images actually enables -- is the Gold ticket's.
-- This model answers only "what does the vehicle look like now", which is the same question the
-- batch poller answered. The difference is that this one gets DELETES right, and the poller
-- structurally cannot (docs/learn/02-ingestion.md, the diff).

WITH usable AS (
    {{ cdc_usable('vehicles', 'vehicle_id') }}
),

latest AS (
    {{ cdc_latest() }}
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
