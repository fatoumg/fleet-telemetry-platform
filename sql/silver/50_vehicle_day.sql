-- --------------------------------------------------------------------------------------
-- 50 -- vehicle_day.  GRAIN: one row per (vehicle_id, event_date).
-- --------------------------------------------------------------------------------------
--
-- Reads:  silver_manual.stg_pings   <- 10
--         silver_manual.stg_vehicles <- 20
--         silver_manual.stg_depots   <- 21
-- Feeds:  a human, for now.
--
-- The fan-in: three upstream tables, built by three separate scripts, joined here. This is where
-- a missing dependency graph stops being a tidiness complaint. Break any one of 10, 20 or 21 and
-- this table is wrong -- and the three failures look different:
--
--   10 fails  -> this script fails too, because stg_pings rolled back but is still there with
--                yesterday's rows... which means it might NOT fail. It might just be wrong.
--   20 fails  -> the join silently loses rows, because an INNER JOIN against a stale
--                stg_vehicles drops any vehicle added since. No error. Fewer rows.
--   21 fails  -> depot_name goes null for depots added since, and nothing objects.
--
-- No test here asserts the row count, the grain, or that the join did not drop anything. Writing
-- one by hand would mean writing a query, running it, and reading the answer myself, every time,
-- for every model. That is the labour dbt's `tests:` blocks replace -- and issue #12 is where
-- this stops being manual.
--
-- INNER JOIN ON stg_vehicles IS A DELIBERATE, DOCUMENTED WRONGNESS.
-- A vehicle deleted from the OLTP has no row in stg_vehicles (20 excludes op='d'), so its pings
-- vanish from this table entirely -- even though those pings really happened and are sitting in
-- stg_pings. The honest model keeps the pings and marks the vehicle unknown, which is a LEFT JOIN
-- and a judgement about how to present missing dimension members: Gold's call, not Silver's.
-- Left as an inner join, with this comment, because the phase-2 diff proved deletes are real
-- here (two vehicle lifecycles the poller could not see), so this is a live wrongness rather
-- than a hypothetical one.

DROP TABLE IF EXISTS silver_manual.vehicle_day;

CREATE TABLE silver_manual.vehicle_day AS
WITH daily AS (
    SELECT
        vehicle_id,
        (device_ts AT TIME ZONE 'UTC')::date                     AS event_date,
        count(*)                                                AS pings,
        min(device_ts)                                           AS first_ping_at,
        max(device_ts)                                           AS last_ping_at,
        -- Distinct positions separates a parked vehicle from a moving one without needing a
        -- speed threshold. A vehicle reporting 700 pings from 3 positions did not drive.
        count(DISTINCT position::text)                          AS distinct_positions,
        round(avg(speed_kmh)::numeric, 2)                       AS avg_speed_kmh,
        round(max(speed_kmh)::numeric, 2)                       AS max_speed_kmh,
        -- Nulls are counted rather than ignored, because avg() skips them silently and an
        -- average over 3 of 700 readings looks exactly like an average over 700.
        count(*) FILTER (WHERE speed_kmh IS NULL)               AS speed_missing
      FROM silver_manual.stg_pings
     GROUP BY vehicle_id, (device_ts AT TIME ZONE 'UTC')::date
),

travelled AS (
    -- Straight-line distance between consecutive readings, summed. Cast to geography so the
    -- answer is metres on a spheroid; ST_Distance on geometry in SRID 4326 would return DEGREES,
    -- which is a number that looks like a distance and is not one.
    --
    -- Ordered by device_ts, the untrustworthy timestamp, because it is the only ordering that
    -- describes the vehicle's own path. A device whose clock jumps will produce a distance that
    -- is wrong, and correcting that is skew correction, which is Gold's.
    SELECT
        vehicle_id,
        event_date,
        round(sum(leg_metres)::numeric, 1)                      AS distance_metres
      FROM (
        SELECT
            vehicle_id,
            (device_ts AT TIME ZONE 'UTC')::date                AS event_date,
            ST_Distance(
                position::geography,
                lag(position) OVER (PARTITION BY vehicle_id ORDER BY device_ts)::geography
            )                                                   AS leg_metres
          FROM silver_manual.stg_pings
      ) legs
     GROUP BY vehicle_id, event_date
)

SELECT
    d.vehicle_id,
    d.event_date,
    v.plate,
    v.status                                                    AS vehicle_status,
    v.capacity,
    dep.name                                                    AS home_depot_name,
    d.pings,
    d.first_ping_at,
    d.last_ping_at,
    d.distinct_positions,
    d.avg_speed_kmh,
    d.max_speed_kmh,
    d.speed_missing,
    t.distance_metres
  FROM daily d
  -- See the header: inner on purpose, wrong on purpose, documented on purpose.
  JOIN silver_manual.stg_vehicles v ON v.vehicle_id = d.vehicle_id
  -- Left, because a depot that has not been captured yet should not delete a vehicle's day.
  LEFT JOIN silver_manual.stg_depots dep ON dep.depot_id = v.home_depot_id
  LEFT JOIN travelled t ON t.vehicle_id = d.vehicle_id AND t.event_date = d.event_date;
