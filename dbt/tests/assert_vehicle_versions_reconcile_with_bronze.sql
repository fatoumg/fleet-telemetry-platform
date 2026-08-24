-- Every Bronze vehicle change event reaches the version model:
--
--     bronze vehicle rows = version rows + unparseable rows + keyless rows
--
-- There is no reject bin and no deduplication term on this side, unlike the ping and job_event
-- reconciliations, because this model discards nothing except rows it structurally cannot key.
-- That is the property worth asserting: Silver's whole claim here is "none lost".
--
-- WHY A KEYLESS ROW IS POSSIBLE AT ALL. entity_key COALESCEs after.vehicle_id with
-- before.vehicle_id, and a row where both are absent has no identity -- it cannot be attributed
-- to a vehicle, so it cannot become a version. Measured zero today. Counted anyway, because a
-- reconciliation that only balances when a term is zero is a reconciliation that will be
-- "fixed" by loosening it the first time the term is not.
--
-- BOUNDED BY max(bronze_offset), NOT COUNTING ALL OF BRONZE. The simulator runs continuously, so
-- comparing unbounded counts is two measurements of a moving target -- the first attempt at this
-- by hand showed a 60-row gap that was entirely the seconds between the two queries
-- (docs/silver-by-hand.md:56-60). Every term below uses the same bound.
--
-- WHAT THIS PROVES IN CI: nothing. Bronze is empty there, max() is null, every comparison is
-- null, and this returns no rows. Said out loud because a green check reads like coverage.

WITH bound AS (
    SELECT max(bronze_offset) AS m FROM {{ ref('stg_vehicle_versions') }}
),

counted AS (
    SELECT
        (SELECT count(*) FROM {{ source('bronze', 'raw_cdc_entities') }}
          WHERE source_table = 'vehicles'
            AND _kafka_offset <= bound.m)                       AS bronze_rows,
        (SELECT count(*) FROM {{ ref('stg_vehicle_versions') }}) AS version_rows,
        (SELECT count(*) FROM {{ source('bronze', 'raw_cdc_entities') }}
          WHERE source_table = 'vehicles'
            AND parse_error IS NOT NULL
            AND _kafka_offset <= bound.m)                       AS unparseable_rows,
        (SELECT count(*) FROM {{ source('bronze', 'raw_cdc_entities') }}
          WHERE source_table = 'vehicles'
            AND parse_error IS NULL
            AND coalesce(payload #>> '{after,vehicle_id}',
                         payload #>> '{before,vehicle_id}') IS NULL
            AND _kafka_offset <= bound.m)                       AS keyless_rows
      FROM bound
)

SELECT * FROM counted
 WHERE bronze_rows <> version_rows + unparseable_rows + keyless_rows
