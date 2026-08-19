-- The declared grain: one row per vehicle per day.
--
-- A GROUP BY over a single relation, so this grain is guaranteed by the query and the test is a
-- regression guard -- it fires if someone adds a column to the GROUP BY, or joins something in to
-- enrich the diagnostic. See tests/assert_vehicle_day_unique_on_vehicle_and_date.sql for the model
-- in this layer where fan-out is genuinely reachable, and for why a composite grain cannot use the
-- built-in `unique`.
--
-- The pairing matters even so: ping_quality and vehicle_day declare the SAME grain and are built
-- from the same stg_pings, so their row counts should track each other. They do not have to be
-- equal -- vehicle_day's inner join on stg_vehicles drops days belonging to deleted vehicles, so it
-- can legitimately hold FEWER rows. If ping_quality ever holds fewer than vehicle_day, something is
-- wrong with one of them. Not asserted here, because that is a claim about two models rather than
-- a property of one, and inventing the test before the failure is what this project argues against.

SELECT vehicle_id,
       event_date,
       count(*) AS row_count
  FROM {{ ref('ping_quality') }}
 GROUP BY vehicle_id, event_date
HAVING count(*) > 1
