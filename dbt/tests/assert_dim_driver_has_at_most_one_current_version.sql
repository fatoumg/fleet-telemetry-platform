-- At most one open-ended version per driver. See
-- assert_dim_vehicle_has_at_most_one_current_version.sql for why AT MOST rather than exactly one,
-- and for what this assertion catches that a grain test cannot.
--
-- Measured on this volume: 43 distinct driver_ids, 40 current versions -- so three ids (9001,
-- 9002, 9401) currently end on a hard delete and hold zero current versions. An `exactly one`
-- assertion would fail on all three, on correct data.

SELECT driver_id, count(*) AS current_versions
  FROM {{ ref('dim_driver') }}
 WHERE is_current
 GROUP BY driver_id
HAVING count(*) > 1
