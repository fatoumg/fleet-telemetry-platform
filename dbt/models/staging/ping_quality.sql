-- --------------------------------------------------------------------------------------
-- ping_quality.  GRAIN: one row per (vehicle_id, event_date).
-- --------------------------------------------------------------------------------------
--
-- Reads:  stg_pings
-- Feeds:  nothing. This is a diagnostic table; a human reads it.
--
-- Ported from sql/silver/40_ping_quality.sql. Lands in `silver`, beside the model it summarises and
-- matching where its hand-written twin lives, so the whole layer is in dbt and no schema is left
-- half-owned. Recorded because the design spec's layer table (section 9) reads the other way --
-- p50/p99 as the lateness summary, and bucketing by device_ts rather than server_ts, are choices
-- rather than facts, and the spec gives Gold every choice. The tension is real and deliberate: it
-- resolves when the dimensional model lands and this becomes the input to mart_pipeline_health
-- rather than a table a human reads directly.
--
-- A VIEW, per dbt/dbt_project.yml:19-23, and that is the more useful property here even though
-- percentile_cont over 173k rows is not free. This is a diagnostic: a stale diagnostic is worse
-- than a slow one, because it reports health that was true yesterday. The hand-written 40 was a
-- table and that is exactly what went wrong with it -- it sat holding 110 rows summing to a
-- perfectly self-consistent 351,742 pings while stg_pings was 360 rows behind Bronze and falling
-- further behind every second, with no error stored anywhere
-- (docs/silver-by-hand.md, section 4b). A view cannot do that.
--
-- WHAT IT MEASURES, and why these three are the right three:
--
--   gaps       -- sequence_no jumps by more than 1. A gap PROVES data was lost. Measured 0 in
--                 the clean baseline (docs/source-system-reference.md, section 4).
--   repeats    -- sequence_no does not advance. The app has no unique constraint on
--                 (vehicle_id, sequence_no), deliberately, so the warehouse is where this is
--                 detected (docker/oltp/init.sql:236-238). Measured 0 -- after a real bug that
--                 produced 7,201 of them.
--   lateness   -- server_ts - device_ts. The floor for any watermark the lateness work chooses; a
--                 bound below the observed p99 would quarantine correct data before any pathology
--                 exists (CLAUDE.md).
--
-- A gap and a stop look identical in the position data and are opposites in meaning: an
-- unchanging position WITH no gap proves the vehicle stopped; a gap proves the pipeline lost
-- something. sequence_no is the only thing that separates them, which is what makes it the most
-- load-bearing column in this file.

WITH ordered AS (
    SELECT
        vehicle_id,
        -- Event date from device_ts, the untrustworthy one, because the question this table
        -- answers is "what did the device claim about its own day". Bucketing by server_ts
        -- instead would answer "what did we receive when", which is a different table and the
        -- one the lateness policy will need.
        (device_ts AT TIME ZONE 'UTC')::date                    AS event_date,
        sequence_no,
        lateness_seconds,
        sequence_no - lag(sequence_no) OVER (
            PARTITION BY vehicle_id ORDER BY sequence_no
        )                                                       AS step
      FROM {{ ref('stg_pings') }}
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
    -- twice: 10 further rows were found in continuous mode, at -0.726s, because the host clock
    -- is not monotonic (docs/silver-by-hand.md, section 3).
    --
    -- Not a test, deliberately. The honest response to a negative value is to investigate the
    -- clock, not to fail a build -- and the project's central claim that server_ts is
    -- trustworthy needs this number to stay visible rather than enforced away.
    count(*) FILTER (WHERE lateness_seconds < 0)                AS negative_lateness
  FROM ordered
 GROUP BY vehicle_id, event_date
