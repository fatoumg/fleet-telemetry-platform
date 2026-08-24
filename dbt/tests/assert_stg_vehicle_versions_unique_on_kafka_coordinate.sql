-- The declared grain: one row per Bronze change event, identified by its Kafka coordinate.
--
-- NOT (vehicle_id, source_ts_ms), which is the grain a reader expects and which would be wrong.
-- source_ts_ms is a millisecond epoch and two commits can share one; the coordinate is the only
-- key Bronze actually guarantees. See assert_stg_depots_unique_on_depot_id.sql for why this is a
-- singular test rather than the built-in `unique`.
--
-- WHY PARTITION AND OFFSET SUFFICE WITHOUT _topic. Bronze's unique index is on
-- (_topic, _kafka_partition, _kafka_offset) because four entity topics land in one table
-- (src/fleet_telemetry/load/schema.py). This model filters to source_table = 'vehicles', which
-- is projected from the payload's source.table and is 1:1 with the topic, so within this model
-- the pair is unique. Remove that filter and this test starts failing correctly.
--
-- Severity is error project-wide (dbt/dbt_project.yml:43-45). A duplicate here is a duplicate
-- version in dim_vehicle: it would produce a zero-width validity interval, which
-- assert_dim_vehicle_intervals_are_non_empty.sql would then also catch -- two tests firing on
-- one cause, and this is the one that names it.

SELECT bronze_partition, bronze_offset, count(*) AS row_count
  FROM {{ ref('stg_vehicle_versions') }}
 GROUP BY bronze_partition, bronze_offset
HAVING count(*) > 1
