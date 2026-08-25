-- --------------------------------------------------------------------------------------
-- snap_vehicles.  The polling comparison -- built to lose.
-- --------------------------------------------------------------------------------------
--
-- A dbt snapshot reads a relation on a schedule and records what changed since last time. That
-- is the same shape as phase 2's batch poller and it has the same blind spot, stated in
-- dbt/models/staging/_sources.yml:98-99: it "would miss any change that happens between two
-- runs". dim_vehicle reads the change stream and cannot miss one. Measuring the gap between them
-- is the deliverable of issue #13.
--
-- IT SNAPSHOTS SILVER, NOT THE OLTP, and that is a constraint rather than a choice. dbt connects
-- to the warehouse (dbt/profiles.yml); it has no route to the operational database, and giving
-- it one would be the exact anti-pattern this project exists to replace. So this polls
-- stg_vehicles -- current state, derived from the same Bronze -- which is the fairest possible
-- version of the naive path: it loses versions purely because of WHEN it looks, not because it
-- is reading worse data.
--
-- strategy='timestamp' ON updated_at, not 'check'. `check` compares column values and would
-- catch a change whose updated_at did not move; `timestamp` is what a real polling pipeline
-- does, because comparing every column of every row is what you adopt CDC to stop doing. Using
-- the flattering strategy would make the comparison dishonest.
--
-- LANDS IN gold, NOT ITS OWN SCHEMA. The tidier arrangement is a fifth schema beside the four
-- medallion layers -- the way silver_manual sits beside silver -- but docker/warehouse/init.sql
-- and the CI job both create exactly four, so a fifth costs two edits and a CI change to buy a
-- naming nicety. The snap_ prefix carries the distinction instead.
--
-- NOTE THAT `dbt build` RUNS SNAPSHOTS TOO. So the polling interval is not "whenever you run
-- dbt snapshot" -- it is that plus every build anyone happens to run. Record the actual run
-- times during the measurement window rather than assuming the interval you intended.

{% snapshot snap_vehicles %}

{{ config(
    target_schema='gold',
    unique_key='vehicle_id',
    strategy='timestamp',
    updated_at='updated_at'
) }}

SELECT * FROM {{ ref('stg_vehicles') }}

{% endsnapshot %}
