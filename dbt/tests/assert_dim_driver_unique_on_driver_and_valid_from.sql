-- The declared grain: one row per driver_id per validity interval. See
-- assert_dim_vehicle_unique_on_vehicle_and_valid_from.sql for the reasoning -- why this is a
-- singular test rather than the built-in `unique`, and why it is a live hazard rather than a
-- regression guard on a model whose key is derived from a millisecond epoch.

SELECT driver_id, valid_from, count(*) AS row_count
  FROM {{ ref('dim_driver') }}
 GROUP BY driver_id, valid_from
HAVING count(*) > 1
