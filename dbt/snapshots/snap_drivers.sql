-- --------------------------------------------------------------------------------------
-- snap_drivers.  Same polling comparison as snap_vehicles.sql -- read its header for the
-- reasoning (why a snapshot loses, why it polls stg_drivers rather than the OLTP, why
-- strategy='timestamp' rather than 'check', why it lands in gold rather than a fifth schema,
-- and why `dbt build` itself advances the polling clock).
-- --------------------------------------------------------------------------------------

{% snapshot snap_drivers %}

{{ config(
    target_schema='gold',
    unique_key='driver_id',
    strategy='timestamp',
    updated_at='updated_at'
) }}

SELECT * FROM {{ ref('stg_drivers') }}

{% endsnapshot %}
