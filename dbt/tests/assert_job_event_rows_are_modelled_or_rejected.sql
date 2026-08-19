-- Every Bronze job_event row is accounted for: modelled, rejected, or removed as a duplicate.
--
--     bronze = silver + rejects + duplicates_removed
--
-- See tests/assert_ping_rows_are_modelled_or_rejected.sql for the full reasoning: why every term is
-- bounded on max(bronze_offset), why the duplicate term is computed from the source rather than
-- derived from the model (deriving it would make the assertion circular), why omitting that term was
-- a real bug that failed on a correctly-working pipeline, and why this proves nothing in CI.
--
-- Measured at the time of writing: 599 bronze = 579 silver + 20 rejects + 0 duplicates. The
-- duplicate term is zero on this stream today -- unlike pings, where a producer-side redelivery
-- produced 20. Zero is not the same claim as "cannot happen": job_events is append-only and reaches
-- Bronze through the same at-least-once path, so the term belongs here whether or not it is
-- currently exercised. That is the mistake the first version of the ping test made.
--
-- The dedup key is dug out of payload rather than projected by Bronze -- raw_job_events has
-- generated columns for op, job_id, from_status and to_status and nothing else -- so this counts
-- excess copies of payload #>> '{after,job_event_id}', matching what stg_job_events deduplicates on.

WITH bound AS (
    SELECT max(bronze_offset) AS m FROM {{ ref('stg_job_events') }}
),

counted AS (
    SELECT
        (SELECT count(*) FROM {{ source('bronze', 'raw_job_events') }}
          WHERE _kafka_offset <= bound.m)                       AS bronze_rows,
        (SELECT count(*) FROM {{ ref('stg_job_events') }})       AS silver_rows,
        (SELECT count(*) FROM {{ ref('stg_rejected_rows') }}
          WHERE source_table = 'raw_job_events'
            AND _kafka_offset <= bound.m)                       AS reject_rows,
        (SELECT count(*) - count(DISTINCT payload #>> '{after,job_event_id}')
           FROM {{ source('bronze', 'raw_job_events') }}
          WHERE payload #>> '{after,job_event_id}' IS NOT NULL
            AND _kafka_offset <= bound.m)                       AS duplicate_rows
      FROM bound
)

SELECT * FROM counted
 WHERE bronze_rows <> silver_rows + reject_rows + duplicate_rows
