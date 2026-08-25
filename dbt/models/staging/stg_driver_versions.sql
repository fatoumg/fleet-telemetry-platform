-- --------------------------------------------------------------------------------------
-- stg_driver_versions.  GRAIN: one row per driver change event.
-- --------------------------------------------------------------------------------------
--
-- Reads:  bronze.raw_cdc_entities WHERE source_table = 'drivers'
-- Feeds:  gold.dim_driver
--
-- Same shape as stg_vehicle_versions.sql; the reasoning about keeping deletes, about leaving
-- suppression and intervals to Gold, and about projecting both source_ts_ms and committed_at
-- all lives there.
--
-- DRIVERS CHURN NOW -- THEY DID NOT USED TO. docs/source-system-reference.md section 7 measured
-- 0 of 40 drivers changed since creation, and that was true: in phase 1 there was no change
-- stream, and the simulator itself never revises a driver once seeded. It stopped being true once
-- the integration suite started exercising this model's source. Measured directly against Bronze
-- today: 17 c, 34 u, 17 d, 40 r events. Filtering to tracked attributes (full_name, phone,
-- status, home_depot_id) and comparing each event to the version before it -- a recreation after
-- a hard delete counts as a change, a no-op does not -- that is 36 real changes and 12 no-op
-- updates, across 43 distinct entities.
--
-- The churn is test fixtures, not simulated fleet behaviour: tests/test_app.py inserts and
-- hard-deletes disposable drivers on every run (`TEST_DRIVER_IDS`, teardown), and
-- docs/known-issues.md section 3 documents the same fixed-id insert/delete pattern going wrong
-- elsewhere in this suite. That means dim_driver is no longer the "sitting next to one that earns
-- its keep, showing what the machinery costs when there is no history to capture" case the
-- original design expected -- it now has real Type 2 history, earned by test churn rather than
-- by anything the domain does. dbt/models/gold/_unit_tests.yml still matters regardless of which
-- source produced the history it pins.

WITH usable AS (
    {{ cdc_usable('drivers', 'driver_id') }}
)

SELECT
    entity_key::integer                                    AS driver_id,
    ("after" ->> 'full_name')                              AS full_name,
    ("after" ->> 'phone')                                  AS phone,
    ("after" ->> 'status')                                 AS status,
    ("after" ->> 'home_depot_id')::integer                 AS home_depot_id,
    ("after" ->> 'created_at')::timestamptz                AS created_at,
    ("after" ->> 'updated_at')::timestamptz                AS updated_at,

    op                                                     AS bronze_op,
    source_ts_ms::bigint                                   AS source_ts_ms,
    to_timestamp(source_ts_ms::bigint / 1000.0)            AS committed_at,
    _kafka_partition                                       AS bronze_partition,
    _kafka_offset                                          AS bronze_offset
  FROM usable
 WHERE entity_key IS NOT NULL
