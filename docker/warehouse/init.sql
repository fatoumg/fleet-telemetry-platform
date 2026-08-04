-- Warehouse bootstrap. Runs once, on an empty data directory.
--
-- Creates only extensions and schemas. Tables are owned by dbt (Silver, Gold, marts) or by
-- the Python loader (bronze), so nothing here should ever define one -- otherwise the schema
-- has two sources of truth.

-- --------------------------------------------------------------------------------------
-- extensions
-- --------------------------------------------------------------------------------------

-- Spatial: GEOMETRY(POINT,4326) on flights and conflicts, GEOMETRY(POLYGON,4326) on grid
-- cells, GIST indexes, and ST_DWithin for the conflict-proximity and weather-match joins.
CREATE EXTENSION IF NOT EXISTS postgis;

-- Time-series partitioning for fact_flight_trajectory, which is the only table large enough
-- to need it. Hypertable creation itself lives in dbt, next to the model it partitions.
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
-- the 25 km buffer, the +/-12 h window, the distinct-aircraft counting rule.
CREATE SCHEMA IF NOT EXISTS gold;

-- Analysis-ready aggregates: mart_adi, mart_altitude_escalation, mart_baseline_traffic.
CREATE SCHEMA IF NOT EXISTS marts;

-- --------------------------------------------------------------------------------------
-- session defaults
-- --------------------------------------------------------------------------------------

-- Every upstream timestamp is UTC epoch seconds. Making that explicit at the database level
-- removes a whole class of off-by-one-hour bugs from the +/-12 h exposure window.
--
-- Dynamic SQL because ALTER DATABASE needs a literal name and the name is parameterised
-- (POSTGRES_DB), so it cannot be hardcoded here.
DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET timezone TO ''UTC''', current_database());
END
$$;
