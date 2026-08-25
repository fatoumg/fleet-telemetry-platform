-- Every closed interval must satisfy valid_from < valid_to. See
-- assert_dim_vehicle_intervals_are_non_empty.sql for what a failure here means -- overlap is
-- structurally impossible, zero width is not -- and for why it is asserted at error severity even
-- though two genuine commits inside one millisecond would trip it.
--
-- THIS TEST WAS SPECIFIED AS VACUOUS AND IS NOT. The plan expected one version per driver, every
-- valid_to null, and a WHERE clause matching nothing -- worth saying so, it argued, because a test
-- that cannot fail on current data is proving nothing. That expectation was based on
-- docs/source-system-reference.md section 7, which measured 0 of 40 drivers changed. Measured at
-- the time of writing: 79 versions, 40 of them current, so there were 39 closed intervals here and
-- 0 of them zero-width -- a reading, not an invariant; the volume ingests continuously, so these
-- counts have since moved (see docs/type-2-dimensions.md for a fresher one). It passed for a real
-- reason at that reading, and still does for the same reason today: any non-zero number of closed
-- intervals makes this test non-vacuous. The history is test-suite churn rather than fleet
-- behaviour (see dim_driver.sql), but a zero-width interval produced by a test fixture is just as
-- unselectable as one produced by the domain.

SELECT driver_id, valid_from, valid_to
  FROM {{ ref('dim_driver') }}
 WHERE valid_to IS NOT NULL
   AND valid_to <= valid_from
