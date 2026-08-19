-- The declared grain: one row per Bronze message.
--
-- (_topic, _kafka_partition, _kafka_offset) rather than the offset alone, because four topics land
-- in raw_cdc_entities and offsets restart from zero in every partition. This is the same key the
-- unique index in src/fleet_telemetry/load/schema.py:94-95 enforces on Bronze itself.
--
-- THIS IS THE ONE MODEL IN THIS LAYER WHERE A DUPLICATE IS GENUINELY REACHABLE, which makes it
-- the one grain assertion here that is not merely a regression guard. The other six are
-- SELECT DISTINCT ON, so Postgres guarantees their grain. This one is three UNION ALL branches
-- over three Bronze tables, plus a join in the entity arm. Two ways it could fan out:
--
--   1. A predicate that overlapped between two branches -- say if raw_ping_events rows ever
--      reached the job_events arm -- would list one Bronze row twice.
--   2. The VALUES join in entity_keyed matching a source_table more than once, which a
--      copy-paste into that list would do silently.
--
-- Either would inflate the reject count, which would make the two reconciliation tests below
-- FAIL IN THE DIRECTION THAT LOOKS CORRECT: `bronze = silver + rejects` would balance while rows
-- were being double-counted as rejected. So this test protects those tests, not just this model.

SELECT _topic,
       _kafka_partition,
       _kafka_offset,
       count(*) AS row_count
  FROM {{ ref('stg_rejected_rows') }}
 GROUP BY _topic, _kafka_partition, _kafka_offset
HAVING count(*) > 1
