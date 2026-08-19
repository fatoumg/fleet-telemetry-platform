-- The declared grain: one row per vehicle_id, current state. See
-- tests/assert_stg_depots_unique_on_depot_id.sql for why this is a singular test rather than the
-- built-in `unique`, and for what a grain assertion on a DISTINCT ON model does and does not buy.

SELECT vehicle_id, count(*) AS row_count
  FROM {{ ref('stg_vehicles') }}
 GROUP BY vehicle_id
HAVING count(*) > 1
