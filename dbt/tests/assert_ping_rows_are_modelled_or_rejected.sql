-- Bronze rows in = Silver rows out + rejects. The assertion that makes the reject bin worth
-- having: any shortfall beyond the recorded rejects is a row that vanished with no evidence,
-- which is the one outcome Bronze exists to make impossible.
--
-- Measured at the time of writing: 173,062 bronze = 173,046 silver + 16 rejects. It balances, and
-- it is doing real arithmetic rather than holding vacuously -- unlike the clean volume
-- docs/silver-by-hand.md was written against, where rejects were 0.
--
-- THIS IS ALSO THE DRIFT DETECTOR. The reject predicate is written twice -- once as the `usable`
-- filter in stg_pings.sql, once in stg_rejected_rows.sql -- and no abstraction prevents them
-- disagreeing. If they ever do, this arithmetic stops balancing and the build fails. That is the
-- deliberate trade: assert the property rather than engineer away the possibility with a macro.
--
-- BOUNDED BY max(bronze_offset), NOT COUNTING ALL OF BRONZE. The simulator runs continuously at
-- ~2 pings/second, so comparing unbounded counts is two measurements of a moving target -- the
-- first attempt at this by hand showed a 60-row gap that was entirely the seconds between the two
-- queries (docs/silver-by-hand.md:56-60). A test that fails for reasons unrelated to correctness
-- gets "fixed" by loosening it, which is how a real reconciliation check becomes decorative.
--
-- The rejects are bounded by the same offset. Without that, a malformed row arriving after the
-- bound would be counted on the reject side but not the bronze side, and the sum would break on
-- correct data.
--
-- WHAT THIS PROVES IN CI: nothing. Bronze is empty there, max() is null, every comparison is null,
-- and this returns no rows. Said out loud because a green check reads like coverage and is not --
-- what CI catches is that the SQL parses and the columns exist.

WITH bound AS (
    SELECT max(bronze_offset) AS m FROM {{ ref('stg_pings') }}
),

counted AS (
    SELECT
        (SELECT count(*) FROM {{ source('bronze', 'raw_ping_events') }}
          WHERE _kafka_offset <= bound.m)                       AS bronze_rows,
        (SELECT count(*) FROM {{ ref('stg_pings') }})            AS silver_rows,
        (SELECT count(*) FROM {{ ref('stg_rejected_rows') }}
          WHERE source_table = 'raw_ping_events'
            AND _kafka_offset <= bound.m)                       AS reject_rows
      FROM bound
)

SELECT * FROM counted
 WHERE bronze_rows <> silver_rows + reject_rows
