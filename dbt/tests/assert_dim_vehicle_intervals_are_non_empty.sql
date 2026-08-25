-- Every closed interval must satisfy valid_from < valid_to.
--
-- WHAT A FAILURE HERE MEANS, because it is not the same as a grain failure. valid_to is the NEXT
-- version's valid_from by construction, so the intervals cannot overlap -- lead() makes that
-- structurally impossible and there is no test for it. What lead() does NOT prevent is a
-- ZERO-WIDTH interval: two events one millisecond apart, or two inside the same millisecond,
-- produce a version that is valid for no instant at all. `valid_from <= t < valid_to` can never
-- select it, so it is a version the dimension holds and can never return.
--
-- Measured on this volume: 0 zero-width intervals across 103 versions. So this passes today for a
-- real reason rather than vacuously -- there are 63 closed intervals here for it to check, unlike
-- assert_dim_driver_intervals_are_non_empty.sql's original expectation of none.
--
-- ASSERTED AT ERROR SEVERITY (dbt/dbt_project.yml:44-46) EVEN THOUGH IT COULD FIRE ON CORRECT
-- INGESTION. Two genuine commits to one vehicle inside one millisecond is not a pipeline bug --
-- but a dimension containing an unselectable row is still wrong, and the right response is to
-- decide what to do about it rather than to discover it in a query six months from now. If it
-- ever fires, the fix is a decision (collapse the pair, or move to a finer clock), not a
-- loosened predicate.

SELECT vehicle_id, valid_from, valid_to
  FROM {{ ref('dim_vehicle') }}
 WHERE valid_to IS NOT NULL
   AND valid_to <= valid_from
