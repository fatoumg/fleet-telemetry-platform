-- --------------------------------------------------------------------------------------
-- dim_driver.  GRAIN: one row per driver_id per validity interval.
-- --------------------------------------------------------------------------------------
--
-- Reads:  silver.stg_driver_versions
-- Feeds:  nothing yet, same as dim_vehicle.
--
-- Same CTE chain as dim_vehicle.sql, with driver_id as the partition key. THE REASONING LIVES
-- THERE and is not repeated: half-open intervals and why closed ones were rejected, why
-- valid_from_is_observation_start marks a fabricated boundary instead of backdating it, why
-- is_deleted needs the coalesce, and -- most importantly -- why a delete must break the
-- suppression chain rather than being excluded from the window. The precedent for a header that
-- points rather than repeats is stg_drivers.sql:8-10.
--
-- THREE THINGS ARE DRIVER-SPECIFIC.
--
-- 1. THIS IS NOT THE EMPTY DIMENSION IT WAS SPECIFIED AS. The design expected 40 rows, all
--    current, none deleted -- a Type 2 dimension sitting next to one that earns its keep, showing
--    what the machinery costs when there is no history to capture. That was true in phase 1:
--    docs/source-system-reference.md section 7 measured 0 of 40 drivers changed since creation,
--    and the simulator still never revises a driver once seeded. Measured now: 79 versions across
--    43 distinct driver_ids, 40 current, 17 closed by a hard delete, 12 no-op UPDATEs suppressed.
--    The history is real but its source is the integration suite, not the domain --
--    tests/test_app.py patches status and phone on TEST_DRIVER_IDS and hard-deletes them in
--    teardown. Worth writing down rather than quietly reporting a number: this dimension has
--    history because the tests churn, and a reader comparing it to the design spec would
--    otherwise assume the fleet does.
--
-- 2. ID REUSE IS WORSE HERE THAN FOR VEHICLES. Three fixed ids are inserted and hard-deleted
--    repeatedly -- 9001 and 9002 from tests/test_app.py:36, 9401 from tests/test_poller.py:103 --
--    carrying 6, 6 and 5 delete events respectively, so 14 of this dimension's lifecycles begin
--    after a delete of the same id. See dim_vehicle.sql's `compared` CTE for why that is the case
--    the obvious implementation loses.
--
-- 3. phone IS TRACKED, AND NULL IS A DISTINCT TRACKED VALUE. It is nullable in the OLTP
--    (docker/oltp/init.sql:83) and no longer 0% null as phase 1 measured: 15 of the 79 versions
--    here carry a null phone, all of them driver 9401, whose fixture inserts no phone number at
--    all. 6 versions exist because the phone changed and nothing else did. What has NOT happened
--    yet is a transition ACROSS the null boundary -- measured 0 null-to-value and 0 value-to-null
--    -- which is exactly why the comparison is jsonb rather than a chain of `<>`:
--    jsonb_build_object('phone', NULL) yields {"phone": null}, which IS DISTINCT FROM an object
--    carrying a real number, so acquiring or losing a phone number will produce a version the
--    first time it happens rather than being silently swallowed. Kept cast-free rather than
--    coalesced to '', matching stg_drivers.sql:12-14 -- an absent phone number and an empty one
--    are different facts and only one of them is true.

WITH events AS (
    SELECT * FROM {{ ref('stg_driver_versions') }}
),

tagged AS (
    -- The tracked set is everything the OLTP lets a caller set on a driver, and nothing else --
    -- see dim_vehicle.sql on why updated_at and created_at are excluded, and on why the delete
    -- row's tracked is NULL rather than an object.
    SELECT *,
           CASE WHEN bronze_op = 'd' THEN NULL
                ELSE jsonb_build_object(
                    'full_name',     full_name,
                    'phone',         phone,
                    'status',        status,
                    'home_depot_id', home_depot_id
                ) END                                       AS tracked
      FROM events
),

compared AS (
    SELECT *,
           lag(tracked) OVER (PARTITION BY driver_id
                              ORDER BY source_ts_ms, bronze_partition, bronze_offset)
                                                            AS prev_tracked
      FROM tagged
),

changed AS (
    SELECT * FROM compared
     WHERE bronze_op <> 'd'
       AND tracked IS DISTINCT FROM prev_tracked
),

deletes AS (
    SELECT * FROM compared
     WHERE bronze_op = 'd'
),

stream AS (
    SELECT * FROM changed
    UNION ALL
    SELECT * FROM deletes
),

ranged AS (
    SELECT *,
           lead(committed_at) OVER w                        AS next_valid_from,
           lead(bronze_op)    OVER w                        AS next_op
      FROM stream
    WINDOW w AS (PARTITION BY driver_id
                 ORDER BY source_ts_ms, bronze_partition, bronze_offset)
)

SELECT
    driver_id,
    full_name,
    phone,
    status,
    home_depot_id,
    created_at,

    committed_at                                            AS valid_from,
    next_valid_from                                         AS valid_to,
    next_valid_from IS NULL                                 AS is_current,
    coalesce(next_op = 'd', false)                          AS is_deleted,
    bronze_op = 'r'                                         AS valid_from_is_observation_start,

    bronze_op                                               AS source_op,
    source_ts_ms,
    bronze_partition,
    bronze_offset
  FROM ranged
 -- The delete row was carried through the windows for its timestamp and for nothing else; see
 -- dim_vehicle.sql's equivalent filter.
 WHERE bronze_op <> 'd'
