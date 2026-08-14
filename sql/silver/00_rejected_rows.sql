-- --------------------------------------------------------------------------------------
-- 00 -- the reject bin, created before anything can write to it
-- --------------------------------------------------------------------------------------
--
-- Every later script excludes rows it cannot type, and puts them here with a reason. That is
-- the only honest way to drop a row: a filtered row that lands nowhere is indistinguishable
-- from a row that never existed, and Bronze went to considerable trouble to keep exactly this
-- evidence (src/fleet_telemetry/load/__init__.py:6-8).
--
-- WHY THIS FILE IS 00 AND WHY THAT IS FRAGILE.
-- The runner sorts by filename, so 00 runs first and the table exists before 10 inserts into
-- it. Nothing enforces that. Rename this file to 99_ and every other script fails on a missing
-- table -- and the failure names `silver_manual.rejected_rows`, not the renamed file, so the
-- error points at the symptom rather than the cause. That is the dependency graph this phase
-- does not have.
--
-- The Kafka coordinate is kept rather than the row's business key, because a rejected row may
-- have no usable business key -- that is frequently WHY it was rejected. (_topic, partition,
-- offset) identifies the Bronze row exactly and immutably, so any reject can be traced back to
-- the message it came from.

CREATE TABLE IF NOT EXISTS silver_manual.rejected_rows (
    source_table     text NOT NULL,
    -- Free text on purpose. A CHECK or an enum here would need editing every time a script
    -- learns to reject something new, and this is the layer whose job is to be honest about
    -- what it could not handle, not to have anticipated it.
    reason           text NOT NULL,
    _topic           text,
    _kafka_partition integer,
    _kafka_offset    bigint,
    payload          jsonb
);

-- Truncated rather than dropped, so the table survives a run that fails before this point and
-- so no later script has to care whether it is the first writer. Rebuilding from scratch every
-- run is the naive contract for the whole layer: there is no incremental anything here.
TRUNCATE silver_manual.rejected_rows;
