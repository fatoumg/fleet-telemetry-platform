-- The declared grain: one row per vehicle_id per validity interval. See
-- assert_stg_depots_unique_on_depot_id.sql for why this is a singular test rather than the
-- built-in `unique` -- and note that a composite grain could not use `unique` at all without
-- adding dbt_utils, which dbt/models/staging/_models.yml rules out by name.
--
-- THIS IS A LIVE HAZARD, NOT A REGRESSION GUARD, and it is the first model in the project where
-- that is true besides vehicle_day. The eight staging models are DISTINCT ON or a single-relation
-- GROUP BY, where Postgres guarantees the grain. This one derives its key from source_ts_ms via
-- committed_at, and source_ts_ms is a millisecond epoch: two commits to the same vehicle inside
-- one millisecond would produce two versions sharing a valid_from, and the tie-break on
-- (bronze_partition, bronze_offset) orders them but does not separate their timestamps.
--
-- Severity is error project-wide (dbt/dbt_project.yml:43-45).

SELECT vehicle_id, valid_from, count(*) AS row_count
  FROM {{ ref('dim_vehicle') }}
 GROUP BY vehicle_id, valid_from
HAVING count(*) > 1
