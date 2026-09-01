-- Every ping the simulator intended to send arrived in Silver, unchanged.
--
-- THIS IS THE ONE ASSERTION THE PROJECT RESTS ON. Every other test here proves an INTERNAL
-- property: a grain holds, a geometry matches its own coordinates, a reconciliation balances
-- against Bronze. All of them can pass while the pipeline quietly drops or mangles data, because
-- Bronze is the earliest thing they can see and Bronze is downstream of the loss. This one compares
-- against what the simulator KNOWS it generated, which is the only reference outside the pipeline.
-- docs/superpowers/specs/2026-08-07-telemetry-platform-design.md:236-237 calls it "the verification
-- the previous project could not have."
--
-- MUST RETURN ZERO ROWS ON A CLEAN RUN, AND THAT IS WHY IT WAS BUILT BEFORE ANY PATHOLOGY FLAG
-- EXISTED. A diff harness first exercised against deliberately dirty data cannot distinguish an
-- injected pathology from a bug in itself. Validated while the answer is known to be zero, it can.
--
-- ------------------------------------------------------------------------------------------------
-- THE BOUND, AND WHY THE OBVIOUS ONE IS WRONG
-- ------------------------------------------------------------------------------------------------
--
-- Truth is always AHEAD of Silver: a recorded intent must cross the API, the OLTP, Debezium, Kafka
-- and Bronze before a Silver view can see it. So an unbounded diff always fails, and every failure
-- is in-flight data rather than loss -- exactly the flapping that
-- tests/assert_ping_rows_are_modelled_or_rejected.sql:48-52 describes, where a hand-run comparison
-- showed a 60-row gap that was entirely the seconds between two queries. A test that fails for
-- reasons unrelated to correctness gets "fixed" by loosening it, and is decorative from then on.
--
-- The obvious bound -- `intended_device_ts < (SELECT max(device_ts) FROM stg_pings)` -- IS WRONG
-- HERE, AND QUIETLY SO. That maximum is taken over ALL of Silver, including every ping from runs
-- predating this table. A backfill run walks its clock from `now - span` to `now`
-- (simulator/run.py:535-537), so its window can end BEFORE the historical maximum -- in which case
-- the bound admits every truth row including the ones still in flight, and the test fails on a
-- correct pipeline. `--reset` cannot rescue it either: Bronze is append-only, so that flag deletes
-- OLTP pings only (simulator/run.py:129-135) and Silver keeps every historical row forever.
--
-- The bound is therefore a PER-RUN ARRIVAL FRONTIER, derived only from rows that actually landed
-- for that run: max(device_ts) over the Silver rows whose ping_id matches one of this run's truth
-- rows. A run with nothing landed yet has a null frontier and contributes nothing, rather than
-- failing.
--
-- STRICT `<`, NOT `<=`. Every ping in one tick carries the same device_ts, and a batch flush happens
-- at PINGS_PER_BATCH = 500 regardless of tick boundaries -- with 40 vehicles that is 12.5 ticks per
-- batch, so a flush routinely lands mid-tick and the frontier tick is only half arrived. Excluding
-- it costs one tick of coverage and removes the entire class of false failure. Batches are flushed
-- in order and the ping topic has one partition, so every tick strictly before the frontier has
-- fully landed.
--
-- ------------------------------------------------------------------------------------------------
-- WHAT THIS DOES NOT PROVE
-- ------------------------------------------------------------------------------------------------
--
-- That Silver holds no rows truth never recorded. The reverse direction is not assertable while
-- pre-truth history exists: every one of those historical pings would be reported as a phantom.
-- Stated rather than faked, because the honest alternative -- asserting it and then adding
-- exclusions until it passes -- produces a test that cannot fail. When the reverse direction
-- matters, it needs a warehouse whose Bronze was empty when truth recording began.
--
-- It also does not prove the deduplication rule chose the right row. stg_pings keeps the earliest
-- Bronze observation and the fields compared here are identical between duplicate copies, so a
-- reversed rule would still pass. dbt/models/staging/_unit_tests.yml is what settles that.
--
-- WHAT THIS PROVES IN CI: nothing. truth.intended_pings is empty there -- the simulator never runs
-- -- so every frontier is null and this returns no rows. Said out loud because a green check reads
-- like coverage and is not. What CI catches is that the SQL parses and the columns exist.
--
-- FLOAT EQUALITY IS DELIBERATE. The recorder stores the values that went on the wire verbatim,
-- already rounded by simulator/run.py:222-225, and Silver casts the same JSON text back to double
-- precision. If this ever flags a coordinate mismatch, that is a real finding about the JSON round
-- trip and NOT a reason to introduce a tolerance -- diagnose it before loosening it.
--
-- IS DISTINCT FROM rather than <> throughout, because speed_kmh and heading_deg are nullable on both
-- sides and `null <> null` is null, which would make a genuinely differing pair pass silently.

WITH frontier AS (
    SELECT i.run_id,
           max(s.device_ts) AS arrived_through
      FROM {{ source('truth', 'intended_pings') }} i
      JOIN {{ ref('stg_pings') }} s ON s.ping_id = i.ping_id
     GROUP BY i.run_id
),

expected AS (
    SELECT i.*
      FROM {{ source('truth', 'intended_pings') }} i
      JOIN frontier f ON f.run_id = i.run_id
     WHERE i.emitted
       AND f.arrived_through IS NOT NULL
       AND i.intended_device_ts < f.arrived_through
),

compared AS (
    SELECT e.run_id,
           e.ping_id,
           e.vehicle_id,
           e.sequence_no,
           e.intended_device_ts,
           s.ping_id     AS silver_ping_id,
           s.device_ts   AS silver_device_ts,
           e.latitude    AS intended_latitude,
           s.latitude    AS silver_latitude,
           e.longitude   AS intended_longitude,
           s.longitude   AS silver_longitude,
           e.speed_kmh   AS intended_speed_kmh,
           s.speed_kmh   AS silver_speed_kmh,
           e.heading_deg AS intended_heading_deg,
           s.heading_deg AS silver_heading_deg
      FROM expected e
      LEFT JOIN {{ ref('stg_pings') }} s ON s.ping_id = e.ping_id
)

SELECT * FROM compared
 WHERE silver_ping_id     IS NULL
    OR silver_device_ts   IS DISTINCT FROM intended_device_ts
    OR silver_latitude    IS DISTINCT FROM intended_latitude
    OR silver_longitude   IS DISTINCT FROM intended_longitude
    OR silver_speed_kmh   IS DISTINCT FROM intended_speed_kmh
    OR silver_heading_deg IS DISTINCT FROM intended_heading_deg
