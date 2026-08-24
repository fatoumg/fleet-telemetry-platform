-- Every Bronze driver change event reaches the version model. Same shape and same reasoning as
-- assert_vehicle_versions_reconcile_with_bronze.sql, read that file first -- only the
-- source_table filter, the JSON paths and the ref() differ below.
--
-- Measured at the time of writing: 108 bronze rows (17 c, 34 u, 17 d, 40 r) = 108 version rows +
-- 0 unparseable + 0 keyless. Unlike the vehicle side, drivers churn from test fixtures rather than
-- simulated fleet behaviour -- see stg_driver_versions.sql -- but this reconciliation does not
-- care why a row changed, only that none of them went missing.

WITH bound AS (
    SELECT max(bronze_offset) AS m FROM {{ ref('stg_driver_versions') }}
),

counted AS (
    SELECT
        (SELECT count(*) FROM {{ source('bronze', 'raw_cdc_entities') }}
          WHERE source_table = 'drivers'
            AND _kafka_offset <= bound.m)                       AS bronze_rows,
        (SELECT count(*) FROM {{ ref('stg_driver_versions') }})  AS version_rows,
        (SELECT count(*) FROM {{ source('bronze', 'raw_cdc_entities') }}
          WHERE source_table = 'drivers'
            AND parse_error IS NOT NULL
            AND _kafka_offset <= bound.m)                       AS unparseable_rows,
        (SELECT count(*) FROM {{ source('bronze', 'raw_cdc_entities') }}
          WHERE source_table = 'drivers'
            AND parse_error IS NULL
            AND coalesce(payload #>> '{after,driver_id}',
                         payload #>> '{before,driver_id}') IS NULL
            AND _kafka_offset <= bound.m)                       AS keyless_rows
      FROM bound
)

SELECT * FROM counted
 WHERE bronze_rows <> version_rows + unparseable_rows + keyless_rows
