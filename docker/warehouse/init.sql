-- Warehouse bootstrap. Runs once, on an empty data directory.
--
-- Creates only extensions and schemas. Tables are owned by dbt (Silver, Gold, marts) or by
-- the Python loader (bronze), so nothing here should ever define one -- otherwise the schema
-- has two sources of truth.

-- --------------------------------------------------------------------------------------
-- extensions
-- --------------------------------------------------------------------------------------

-- Spatial: GEOMETRY(POINT,4326) on pings, depots and job endpoints, GIST indexes, and
-- ST_DWithin for corridor and catchment joins.
CREATE EXTENSION IF NOT EXISTS postgis;

-- Time-series partitioning for fact_ping, which is the only table large enough to need it
-- (~860k rows per simulated day). Hypertable creation lives in dbt, next to the model it
-- partitions.
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- --------------------------------------------------------------------------------------
-- schemas: one per medallion layer
-- --------------------------------------------------------------------------------------
--
-- Separate schemas rather than table-name prefixes so grants can differ per layer: analysts
-- read marts, nobody writes bronze by hand.

-- Raw API responses, append-only, stored as received. Never edited.
CREATE SCHEMA IF NOT EXISTS bronze;

-- Validated, deduplicated, typed. Fixes only what nobody can reasonably disagree about
-- (unit conversions, padded strings, field naming). No analysis decisions.
CREATE SCHEMA IF NOT EXISTS silver;

-- Dimensional model: dim_* and fact_*. Every judgment call lives here or above --
-- clock-skew correction, the lateness policy, trip reconstruction, utilisation rules.
CREATE SCHEMA IF NOT EXISTS gold;

-- Analysis-ready aggregates: mart_vehicle_utilisation, mart_job_performance,
-- mart_corridor_flow, mart_pipeline_health.
CREATE SCHEMA IF NOT EXISTS marts;

-- --------------------------------------------------------------------------------------
-- truth: not a medallion layer
-- --------------------------------------------------------------------------------------
--
-- What the simulator intended to emit, written by the simulator itself and read only by tests. It
-- sits outside bronze/silver/gold/marts because it is not a stage in the pipeline -- it is the
-- reference the pipeline is checked against, and the only one that originates outside it. Every
-- other assertion in this project can pass while data is quietly lost, because Bronze is the
-- earliest thing they can see and Bronze is downstream of the loss.
--
-- Tables are owned by src/fleet_telemetry/truth.py, per the rule at the top of this file. Note that
-- this file only runs on an empty data directory, so that module creates the schema too.
CREATE SCHEMA IF NOT EXISTS truth;

-- --------------------------------------------------------------------------------------
-- session defaults
-- --------------------------------------------------------------------------------------

-- Every timestamp in this system is UTC. Making that explicit at the database level removes
-- a whole class of off-by-one-hour bugs from event-time windowing -- which matters more here
-- than usual, since device clock skew is already a first-class problem.
--
-- Dynamic SQL because ALTER DATABASE needs a literal name and the name is parameterised
-- (POSTGRES_DB), so it cannot be hardcoded here.
DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET timezone TO ''UTC''', current_database());
END
$$;
