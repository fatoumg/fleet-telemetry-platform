-- Bronze rows in = Silver rows out + rejects, for the job_events stream. See
-- tests/assert_ping_rows_are_modelled_or_rejected.sql for the full reasoning: why it is bounded
-- on max(bronze_offset), why the rejects are bounded by the same value, and why it proves nothing
-- in CI.
--
-- Measured at the time of writing: 599 bronze = 579 silver + 20 rejects, exactly.
--
-- THERE IS NO EQUIVALENT TEST FOR THE FOUR ENTITY MODELS, and the absence is deliberate rather
-- than an omission. stg_vehicles, stg_depots, stg_drivers and stg_jobs deduplicate to CURRENT
-- STATE, so most Bronze rows are neither modelled nor rejected -- they are SUPERSEDED by a later
-- version of the same entity. `in = out + rejects` is simply false for them, and asserting it
-- would mean counting supersessions, which is a different claim.
--
-- The honest version of that check belongs with the Type 2 history work, where every version is
-- retained and the arithmetic closes again: in = versions out + rejects, with no supersession
-- term. Writing it here would require inventing that term, and a reconciliation test whose
-- residual is "everything I could not account for" reconciles nothing.

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
            AND _kafka_offset <= bound.m)                       AS reject_rows
      FROM bound
)

SELECT * FROM counted
 WHERE bronze_rows <> silver_rows + reject_rows
