-- --------------------------------------------------------------------------------------
-- 40 -- ping_quality.  GRAIN: one row per (vehicle_id, event_date).
-- --------------------------------------------------------------------------------------
--
-- Reads:  silver_manual.stg_pings   <- built by 10, and nothing here knows that
-- Feeds:  nothing. This is a diagnostic table; a human reads it.
--
-- The first script that depends on another script's output. Two things follow from that, and both
-- are the point of this phase:
--
-- 1. NOTHING RECORDS THE DEPENDENCY. It is the filename: 40 sorts after 10. Postgres does not
--    know either -- CREATE TABLE AS materialises rows and registers no catalog dependency, so
--    10's DROP TABLE succeeds happily while this table still holds data derived from it. Had
--    these been VIEWS, Postgres itself would have refused the drop. Choosing tables removed even
--    the database's own dependency tracking, and that choice was made for a different reason
--    (a stale table is what dbt's +materialized: view exists to avoid), which is how these
--    things usually happen.
--
-- 2. IT IS WHAT BREAKS FIRST. Rename a column in Bronze and 10 fails; this table then holds
--    yesterday's numbers, is still queryable, and looks entirely fine.
--
-- WHAT IT MEASURES, and why these three are the right three:
--
--   gaps       -- sequence_no jumps by more than 1. A gap PROVES data was lost. Measured 0 in
--                 the clean baseline (docs/source-system-reference.md, section 4).
--   repeats    -- sequence_no does not advance. The app has no unique constraint on
--                 (vehicle_id, sequence_no), deliberately, so the warehouse is where this is
--                 detected (docker/oltp/init.sql:236-238). Measured 0 -- after a real bug that
--                 produced 7,201 of them.
--   lateness   -- server_ts - device_ts. The floor for any watermark Phase 3 chooses; a bound
--                 below the observed p99 would quarantine correct data before any pathology
--                 exists (CLAUDE.md:178-182).
--
-- A gap and a stop look identical in the position data and are opposites in meaning: an
-- unchanging position WITH no gap proves the vehicle stopped; a gap proves the pipeline lost
-- something. sequence_no is the only thing that separates them, which is what makes it the most
-- load-bearing column in this file -- and the one the loud experiment removes.

DROP TABLE IF EXISTS silver_manual.ping_quality;

CREATE TABLE silver_manual.ping_quality AS
WITH ordered AS (
    SELECT
        vehicle_id,
        -- Event date from device_ts, the untrustworthy one, because the question this table
        -- answers is "what did the device claim about its own day". Bucketing by server_ts
        -- instead would answer "what did we receive when", which is a different table and the
        -- one lateness policy will need in issue #15.
        (device_ts AT TIME ZONE 'UTC')::date                    AS event_date,
        sequence_no,
        lateness_seconds,
        sequence_no - lag(sequence_no) OVER (
            PARTITION BY vehicle_id ORDER BY sequence_no
        )                                                       AS step
      FROM silver_manual.stg_pings
)

SELECT
    vehicle_id,
    event_date,
    count(*)                                                    AS pings,

    -- step IS NULL only for a vehicle's first row overall, so it is neither a gap nor a repeat.
    count(*) FILTER (WHERE step > 1)                            AS gaps,
    -- Rows lost, not gap events: one gap of 50 is worse than five gaps of 2, and a count of
    -- gaps alone cannot tell those apart.
    coalesce(sum(step - 1) FILTER (WHERE step > 1), 0)          AS rows_missing,
    count(*) FILTER (WHERE step = 0)                            AS repeats,

    round(min(lateness_seconds)::numeric, 4)                    AS lateness_min,
    round(
        percentile_cont(0.50) WITHIN GROUP (ORDER BY lateness_seconds)::numeric, 4
    )                                                           AS lateness_p50,
    round(
        percentile_cont(0.99) WITHIN GROUP (ORDER BY lateness_seconds)::numeric, 4
    )                                                           AS lateness_p99,
    round(max(lateness_seconds)::numeric, 4)                    AS lateness_max,

    -- Negative lateness means server_ts landed BEFORE device_ts. That is not merely a late
    -- device clock -- it is impossible for a clock that is merely wrong in the usual direction,
    -- and it caught a real simulator bug that backdated 169,480 rows to a median of -29s
    -- (docs/learn/01-source-system.md:181-184). Counting it costs nothing and it has paid out
    -- once already.
    count(*) FILTER (WHERE lateness_seconds < 0)                AS negative_lateness
  FROM ordered
 GROUP BY vehicle_id, event_date;
