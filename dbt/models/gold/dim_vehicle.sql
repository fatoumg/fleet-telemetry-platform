-- --------------------------------------------------------------------------------------
-- dim_vehicle.  GRAIN: one row per vehicle_id per validity interval.
-- --------------------------------------------------------------------------------------
--
-- Reads:  silver.stg_vehicle_versions
-- Feeds:  nothing yet. The facts that will join to it are later tickets; this model exists
--         first because a fact table cannot be built against a dimension that has no history.
--
-- WHAT THIS RECOVERS. docs/source-system-reference.md:259 measured the gap this ticket closes:
-- the simulator performed 12 vehicle reassignments and the database reports 8 changed rows. Both
-- numbers are right -- four vehicles were reassigned more than once and each later change
-- overwrote the one before it -- and current state has no way to say so. stg_vehicles answers
-- "where is vehicle 17 based now"; this model answers "where was it based on the 14th", which is
-- the question every fact that references a vehicle actually needs.
--
-- HALF-OPEN INTERVALS: [valid_from, valid_to). valid_from is inclusive, valid_to is exclusive, and
-- valid_to of one version IS valid_from of the next -- the same instant, written once, read two
-- ways. Selecting a version is `valid_from <= t AND (valid_to IS NULL OR t < valid_to)`.
--
-- CLOSED INTERVALS WERE REJECTED, and the reason is not aesthetic. A closed [from, to] needs the
-- predecessor's `to` to be strictly before the successor's `from`, which means inventing a gap --
-- "one millisecond before the next change". Then every point-in-time query silently returns
-- nothing for any instant that lands in a manufactured gap, and the width of that gap is a
-- property of the clock resolution rather than of the fleet. Worse, source_ts_ms is a millisecond
-- epoch, so the gap would have to be exactly the granularity at which two real commits can
-- collide. Half-open needs no gap and no subtraction: the boundary instant belongs to the later
-- version, unambiguously, at any clock resolution.
--
-- valid_from_is_observation_start MARKS A FABRICATED BOUNDARY RATHER THAN HIDING IT. 40 of the
-- rows here start at op='r' -- the Debezium snapshot, which is the moment CDC started watching,
-- not the moment the vehicle came into existence. That version's valid_from is therefore a fact
-- about the pipeline, not about the fleet, and the honest thing is a column that says so. The
-- alternative -- backdating it to created_at -- would claim the attributes held from creation
-- onwards, and nothing in Bronze supports that: any change before the snapshot is simply not in
-- the record. created_at is projected alongside so a reader can see both boundaries and judge.
--
-- is_deleted IS coalesce(next_op = 'd', false), NOT next_op = 'd'. The current version has no
-- next event, so next_op is NULL, and `NULL = 'd'` is NULL -- which is neither true nor false and
-- would leave the flag null on exactly the 40 rows a reader is most likely to filter on. A
-- `WHERE NOT is_deleted` would then drop every live vehicle. The coalesce is the whole fix and
-- the _models.yml contract asserts not_null on it so a regression cannot pass quietly.

WITH events AS (
    SELECT * FROM {{ ref('stg_vehicle_versions') }}
),

tagged AS (
    -- WHICH ATTRIBUTES COUNT AS A CHANGE IS A JUDGEMENT, AND THIS IS IT. Everything the OLTP
    -- lets a caller set, and nothing else. updated_at and created_at are excluded on purpose:
    -- updated_at moves on every UPDATE by definition, so tracking it would make the suppression
    -- rule below a no-op, and created_at cannot change at all.
    --
    -- jsonb rather than a five-way IS DISTINCT FROM chain, because the comparison must survive
    -- nulls and because adding a tracked attribute should be a one-line change here rather than
    -- an edit in two places that can drift.
    SELECT *,
           CASE WHEN bronze_op = 'd' THEN NULL
                ELSE jsonb_build_object(
                    'plate',             plate,
                    'capacity',          capacity,
                    'status',            status,
                    'current_driver_id', current_driver_id,
                    'home_depot_id',     home_depot_id
                ) END                                       AS tracked
      FROM events
),

compared AS (
    -- THE WINDOW RUNS OVER ALL EVENTS, DELETES INCLUDED, and that is not an oversight. The first
    -- draft of this model ran it over the non-delete rows only, on the stated grounds that
    -- vehicle_id is a serial and ids are never reused, so no version can follow a delete.
    --
    -- THAT IS FALSE IN THIS DATABASE. The integration suite pins fixed high ids and hard-deletes
    -- them on every run: tests/test_app.py:37 pins TEST_VEHICLE_ID = 9101 and
    -- tests/test_poller.py:102-103 pin 9401, and docs/known-issues.md section 3 documents the
    -- pattern going wrong in its own right. Measured against Bronze: 9101 and 9401 carry six
    -- delete events EACH -- twelve lifecycles between them, ten of which begin after a delete of
    -- the same id. Each re-create repeats the previous fixture's plate, capacity, status and
    -- depot, so with the delete excluded from this window the comparison below saw no change and
    -- discarded five of the six lifecycles per id. 103 versions became 93, silently, and every
    -- test in the project still passed.
    --
    -- Excluding the delete row from the OUTPUT is right; excluding it from the ORDERING is what
    -- was wrong. tagged sets its `tracked` to NULL, so a delete carries no attributes into the
    -- comparison, it only breaks the chain -- the re-create that follows compares against NULL,
    -- is distinct from it, and survives as the new lifecycle's first version.
    SELECT *,
           lag(tracked) OVER (PARTITION BY vehicle_id
                              ORDER BY source_ts_ms, bronze_partition, bronze_offset)
                                                            AS prev_tracked
      FROM tagged
),

changed AS (
    -- IS DISTINCT FROM, not <>. The first event of every lifecycle has prev_tracked = NULL, and
    -- `tracked <> NULL` is NULL rather than true -- which would silently drop the first version
    -- of every entity in the dimension and leave a build that reports success.
    SELECT * FROM compared
     WHERE bronze_op <> 'd'
       AND tracked IS DISTINCT FROM prev_tracked
),

deletes AS (
    -- Deletes are NOT subject to the suppression rule and must not be: a delete's tracked is
    -- NULL, so `tracked IS DISTINCT FROM prev_tracked` would drop a delete that follows another
    -- delete. That cannot happen on well-formed CDC, and a rule whose correctness depends on
    -- that is a rule waiting for a redelivery. Two paths out of `compared`, joined below.
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
    WINDOW w AS (PARTITION BY vehicle_id
                 ORDER BY source_ts_ms, bronze_partition, bronze_offset)
)

SELECT
    vehicle_id,
    plate,
    capacity,
    status,
    current_driver_id,
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
 -- The delete row was carried through the windows for its timestamp and for nothing else. It is
 -- not a version -- it has no after-image, so every attribute above would be null -- and letting
 -- it out would put a row in the dimension describing a vehicle that does not exist.
 WHERE bronze_op <> 'd'
