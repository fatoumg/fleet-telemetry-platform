-- The declared grain: one row per ping_id. Nothing in the hand-written 10_stg_pings.sql
-- asserted it, and the ticket that produced this file exists mostly because of that.
--
-- WHAT THIS CATCHES AND WHAT IT DOES NOT. It catches duplicates in the output. It does NOT prove
-- the deduplication rule is correct, and that gap did not close when real duplicates appeared.
-- Bronze now holds 20 duplicate ping_ids -- 190,504 usable rows, 190,484 distinct -- and this test
-- passes just as happily under a REVERSED ORDER BY that would keep the redelivered copy instead of
-- the original. One row per ping_id is true either way; that is the whole point.
--
-- dbt/models/staging/_unit_tests.yml is what distinguishes them, by supplying the duplicate
-- reality has not. Both tests are needed and neither substitutes for the other:
--
--   this test        -> the output has one row per ping_id
--   the unit test    -> it is the RIGHT row
--
-- Severity is error project-wide (dbt/dbt_project.yml:44-46). A duplicate here would fan out
-- every count, sum and average downstream of it, and fan-out is invisible without an explicit
-- uniqueness test -- which is the bug class the design removed from the original mart.

SELECT ping_id, count(*) AS row_count
  FROM {{ ref('stg_pings') }}
 GROUP BY ping_id
HAVING count(*) > 1
