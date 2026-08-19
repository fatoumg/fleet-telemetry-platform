-- The declared grain: one row per vehicle per day.
--
-- THIS IS THE ONE. Every other grain assertion in this project guards a model that is
-- SELECT DISTINCT ON or a GROUP BY over a single relation, where Postgres guarantees the grain and
-- the test is a regression guard. vehicle_day joins THREE relations, so fan-out here is not
-- hypothetical:
--
--   a duplicate vehicle_id in stg_vehicles -> every ping count, avg speed, max speed and distance
--   in this table doubles. No error. No warning. Row count changes, and every number that a human
--   would sanity-check moves together and therefore still looks plausible.
--
-- That is the bug class the design removed from the original mart, and this test is what stops it
-- coming back. It is +severity: error project-wide (dbt/dbt_project.yml:38-40) for exactly that
-- reason: a warning here would be a log line under a report nobody knows is wrong.
--
-- WHY THIS CANNOT USE THE BUILT-IN `unique`. The grain is composite. dbt's `unique` takes one
-- column, and asserting uniqueness on vehicle_id alone would FAIL on correct data -- a vehicle
-- legitimately has one row per day. dbt_utils.unique_combination_of_columns exists for this and is
-- not used: adding a package for one test is not worth a packages.yml
-- (docs/superpowers/plans/2026-08-11-phase-2-ingestion.md:2233), and a six-line query is more
-- readable than a macro invocation to someone checking whether the assertion says what they think.
--
-- NOTE WHAT THIS DOES NOT CATCH, because it matters here more than anywhere else in the layer:
-- rows DROPPED by the inner join on stg_vehicles. A vehicle deleted from the OLTP takes its whole
-- day with it, and losing rows preserves uniqueness perfectly. Only a reconciliation against
-- stg_pings would catch that, and this model does not have one -- the wrongness is documented in
-- vehicle_day.sql instead. A green grain assertion is not a claim that the join is right.

SELECT vehicle_id,
       event_date,
       count(*) AS row_count
  FROM {{ ref('vehicle_day') }}
 GROUP BY vehicle_id, event_date
HAVING count(*) > 1
