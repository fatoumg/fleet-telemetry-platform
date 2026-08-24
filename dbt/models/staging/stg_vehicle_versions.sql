-- --------------------------------------------------------------------------------------
-- stg_vehicle_versions.  GRAIN: one row per vehicle change event.
-- --------------------------------------------------------------------------------------
--
-- Reads:  bronze.raw_cdc_entities WHERE source_table = 'vehicles'
-- Feeds:  gold.dim_vehicle
--
-- THE SAME BRONZE ROWS AS stg_vehicles, WITHOUT THE DISTINCT ON. That one difference is the
-- whole ticket. stg_vehicles answers "what does vehicle 17 look like now"; this model answers
-- "what has ever been true of vehicle 17", and only the second one can reconstruct the
-- intermediate depots that docs/source-system-reference.md measured going missing -- 12
-- reassignments performed, 8 changed rows in the database.
--
-- DELETES ARE KEPT HERE, unlike in stg_vehicles, which ends on `WHERE op <> 'd'`. A delete is
-- not a version -- it has no after-image, so every attribute below is null on that row -- but it
-- is the event that ENDS the previous version, and dropping it in Silver would make that
-- unrecoverable in Gold. dim_vehicle uses the delete row's source_ts_ms and nothing else from
-- it; see dbt/macros/cdc.sql on why reading a before-image body would be a mistake.
--
-- NO SUPPRESSION AND NO INTERVALS HERE, DELIBERATELY. Whether an UPDATE that moved only
-- updated_at counts as a change, and where the first interval starts, are judgements, and the
-- design spec's layer table gives Gold every judgement. Silver's claim is only that these are
-- the events, typed, in order, with none lost.
--
-- committed_at IS source_ts_ms, RENDERED. Both are projected because they answer different
-- questions: source_ts_ms is what the ordering and the tie-break use (integers compare exactly),
-- committed_at is what a validity range must be expressed in (a bigint epoch is not a point in
-- time to anyone reading a dimension). Keeping both means Gold never re-derives one from the
-- other in two places and gets them subtly different.

WITH usable AS (
    {{ cdc_usable('vehicles', 'vehicle_id') }}
)

SELECT
    entity_key::integer                                    AS vehicle_id,

    -- Every attribute is projected from the after-image, so a delete row carries nulls across
    -- the board. That is correct and load-bearing: it makes a delete impossible to mistake for
    -- a version in which the plate became unknown.
    ("after" ->> 'plate')                                  AS plate,
    ("after" ->> 'capacity')::integer                      AS capacity,
    ("after" ->> 'status')                                 AS status,
    ("after" ->> 'current_driver_id')::integer             AS current_driver_id,
    ("after" ->> 'home_depot_id')::integer                 AS home_depot_id,
    ("after" ->> 'created_at')::timestamptz                AS created_at,
    ("after" ->> 'updated_at')::timestamptz                AS updated_at,

    op                                                     AS bronze_op,
    source_ts_ms::bigint                                   AS source_ts_ms,
    to_timestamp(source_ts_ms::bigint / 1000.0)            AS committed_at,
    _kafka_partition                                       AS bronze_partition,
    _kafka_offset                                          AS bronze_offset
  FROM usable
 WHERE entity_key IS NOT NULL
