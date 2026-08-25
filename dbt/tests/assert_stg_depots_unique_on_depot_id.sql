-- The declared grain: one row per depot_id, current state.
--
-- A singular test rather than the built-in `unique`, so every model in this layer asserts its
-- grain the same way whether the grain is one column or several -- and so the assertion reads as
-- a query a human can check rather than a keyword they have to trust. Severity is error
-- project-wide (dbt/dbt_project.yml:44-46): fan-out inflates every downstream number silently,
-- so it must break the build rather than warn into a log nobody reads.
--
-- BE HONEST ABOUT WHAT THIS CATCHES HERE. The model is SELECT DISTINCT ON (entity_key), which
-- cannot fan out -- Postgres guarantees one row per DISTINCT ON key. So this is a regression
-- guard on the declared key, not live hazard detection: it fires if someone changes the
-- DISTINCT ON expression or adds a join without thinking. The model where fan-out is genuinely
-- reachable is vehicle_day, which joins three tables (sql/silver/50_vehicle_day.sql:96-101),
-- and that one is still hand-written and still untested -- the Gold ticket's.
--
-- Also note what it does NOT prove: that the RIGHT row survived. See
-- dbt/models/staging/_unit_tests.yml for the assertion that does.

SELECT depot_id, count(*) AS row_count
  FROM {{ ref('stg_depots') }}
 GROUP BY depot_id
HAVING count(*) > 1
