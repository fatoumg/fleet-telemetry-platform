-- The declared grain: one row per ping_id. Nothing in the hand-written 10_stg_pings.sql
-- asserted it, and the ticket that produced this file exists mostly because of that.
--
-- WHAT THIS CATCHES AND WHAT IT DOES NOT. It catches duplicates in the output. It does NOT prove
-- the deduplication rule is correct. Bronze currently holds no duplicate ping_ids at all --
-- 173,062 rows, 173,062 distinct ids -- so DISTINCT ON discards nothing, and this test passes
-- just as happily under a REVERSED ORDER BY that would silently prefer a later re-snapshot over
-- the original streamed create. One row per ping_id is true either way.
--
-- dbt/models/staging/_unit_tests.yml is what distinguishes them, by supplying the duplicate
-- reality has not. Both tests are needed and neither substitutes for the other:
--
--   this test        -> the output has one row per ping_id
--   the unit test    -> it is the RIGHT row
--
-- Severity is error project-wide (dbt/dbt_project.yml:38-40). A duplicate here would fan out
-- every count, sum and average downstream of it, and fan-out is invisible without an explicit
-- uniqueness test -- which is the bug class the design removed from the original mart.

SELECT ping_id, count(*) AS row_count
  FROM {{ ref('stg_pings') }}
 GROUP BY ping_id
HAVING count(*) > 1
