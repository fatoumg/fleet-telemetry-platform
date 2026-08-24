-- --------------------------------------------------------------------------------------
-- vehicle_day.  GRAIN: one row per (vehicle_id, event_date).
-- --------------------------------------------------------------------------------------
--
-- Reads:  stg_pings, stg_vehicles, stg_depots
-- Feeds:  a human, for now.
--
-- Ported from sql/silver/50_vehicle_day.sql. Lands in `silver`, matching where its hand-written twin
-- lives, so the whole layer is in dbt and no schema is left half-owned. Recorded because the design
-- spec's layer table (section 9) reads the other way: trip-shaped aggregation and how to present a
-- missing dimension member are judgements, and the spec gives Gold every judgement. The tension is
-- deliberate and resolves when the dimensional model lands and this becomes an input to
-- mart_vehicle_utilisation rather than the end of the line.
--
-- THIS IS THE MODEL THE GRAIN ASSERTION WAS ALWAYS ABOUT. Every other model in this project is
-- SELECT DISTINCT ON, or a GROUP BY over one relation, so Postgres guarantees the grain and the
-- assertion is a regression guard. This one joins THREE relations. Add a second row to stg_vehicles
-- for one vehicle_id and every ping count, speed and distance in this table doubles -- no error, no
-- warning, and every number a human would sanity-check moves together and therefore still looks
-- plausible. That is the bug class an explicit uniqueness test exists to catch, and the one the
-- hand-written layer had no defence against.
--
-- Its header said so plainly: "No test here asserts the row count, the grain, or that the join did
-- not drop anything. Writing one by hand would mean writing a query, running it, and reading the
-- answer myself, every time, for every model." That labour is now
-- tests/assert_vehicle_day_unique_on_vehicle_and_date.sql, at +severity: error, on every build.
--
-- WHAT THE FAN-IN USED TO COST. Three upstream tables built by three separate scripts and joined
-- here, with nothing recording the dependency -- it was the filename sort, and Postgres knew nothing
-- either, because CREATE TABLE AS registers no catalog dependency. Break any one of 10, 20 or 21
-- and this table was wrong in three different ways:
--
--   10 fails  -> this script fails too, because stg_pings rolled back but is still there with
--                yesterday's rows... which means it might NOT fail. It might just be wrong.
--   20 fails  -> the join silently loses rows, because an INNER JOIN against a stale
--                stg_vehicles drops any vehicle added since. No error. Fewer rows.
--   21 fails  -> depot_name goes null for depots added since, and nothing objects.
--
-- ref() removes all three. A failed upstream SKIPS this model rather than rebuilding it from stale
-- data, and the skip is reported. Being a view rather than a table removes the staleness itself.
--
-- INNER JOIN ON stg_vehicles IS STILL A DELIBERATE, DOCUMENTED WRONGNESS.
-- A vehicle deleted from the OLTP has no row in stg_vehicles (that model excludes op='d'), so its
-- pings vanish from this table entirely -- even though those pings really happened and are sitting
-- in stg_pings. The honest model keeps the pings and marks the vehicle unknown, which is a LEFT
-- JOIN plus an "unknown" dimension member, and that is a presentation judgement.
--
-- Kept as an inner join by this port, on purpose: changing it would change the numbers in the same
-- commit that moved the file, and a port whose output differs from its source cannot be verified
-- against it. The phase-2 diff proved deletes are real here -- two vehicle lifecycles the poller
-- could not see -- so this is a live wrongness rather than a hypothetical one, and it is the
-- obvious next change.

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
      FROM {{ ref('stg_pings') }}
     GROUP BY vehicle_id, (device_ts AT TIME ZONE 'UTC')::date
),

travelled AS (
    -- Straight-line distance between consecutive readings, summed. Cast to geography so the
    -- answer is metres on a spheroid; ST_Distance on geometry in SRID 4326 would return DEGREES,
    -- which is a number that looks like a distance and is not one.
    --
    -- Ordered by device_ts, the untrustworthy timestamp, because it is the only ordering that
    -- describes the vehicle's own path. A device whose clock jumps will produce a distance that
    -- is wrong, and correcting that is skew correction, which is Gold's and a separate ticket.
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
          FROM {{ ref('stg_pings') }}
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
  -- See the header: inner on purpose, wrong on purpose, documented on purpose. The grain assertion
  -- does NOT catch this -- dropping rows preserves uniqueness perfectly. Only a reconciliation
  -- against stg_pings would, and this model does not have one, which is why the wrongness is
  -- written down rather than tested for.
  JOIN {{ ref('stg_vehicles') }} v ON v.vehicle_id = d.vehicle_id
  -- Left, because a depot that has not been captured yet should not delete a vehicle's day.
  LEFT JOIN {{ ref('stg_depots') }} dep ON dep.depot_id = v.home_depot_id
  LEFT JOIN travelled t ON t.vehicle_id = d.vehicle_id AND t.event_date = d.event_date
