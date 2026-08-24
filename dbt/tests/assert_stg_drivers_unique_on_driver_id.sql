-- The declared grain: one row per driver_id, current state. See
-- tests/assert_stg_depots_unique_on_depot_id.sql for the reasoning.
--
-- Worth knowing: drivers was measured as 0 of 40 rows changed since creation, so the latest-wins
-- rule this asserts the output of has never actually had to choose between two versions. Same
-- blind spot the ping deduplication had before _unit_tests.yml existed.

SELECT driver_id, count(*) AS row_count
  FROM {{ ref('stg_drivers') }}
 GROUP BY driver_id
HAVING count(*) > 1
