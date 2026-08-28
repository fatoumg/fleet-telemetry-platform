-- The declared grain: one row per (run_id, vehicle_id, sequence_no).
--
-- UNLIKE EVERY OTHER GRAIN ASSERTION IN THIS DIRECTORY, THIS ONE GUARDS OUR OWN WRITES RATHER THAN
-- A MODEL'S DEDUPLICATION. There is no DISTINCT ON upstream of it and no GROUP BY -- rows are
-- inserted one per take_reading() call (simulator/run.py:209), so a duplicate here means the
-- recorder wrote the same reading twice. That would inflate the denominator of every ground-truth
-- comparison downstream, and inflate it in the direction that makes the pipeline look WORSE than it
-- is: extra expected rows with no ping to match them.
--
-- The same property is also enforced in the database, as a unique index in
-- src/fleet_telemetry/truth.py. That is a departure from how pings is treated, where the
-- (vehicle_id, sequence_no) constraint is deliberately WITHHELD (docker/oltp/init.sql:236-238) so
-- the warehouse can detect an anomalous device rather than the application rejecting the evidence.
-- The difference is who wrote the row: a duplicate ping is evidence about a device and must be
-- kept, whereas a duplicate truth row could only be our own bug.
--
-- BELT AND BRACES ON PURPOSE, and the two catch different things. The index fails the simulator at
-- the write, which is where a bug is cheapest to find. This fails the build, which is the only
-- thing that catches a row inserted into a warehouse volume created before this ticket existed --
-- where the schema was added by apply() but the index may predate it.
--
-- Holds vacuously on an empty table, which is its state in CI and on any clone that has not run the
-- simulator with --truth.

SELECT run_id,
       vehicle_id,
       sequence_no,
       count(*) AS row_count
  FROM {{ source('truth', 'intended_pings') }}
 GROUP BY run_id, vehicle_id, sequence_no
HAVING count(*) > 1
