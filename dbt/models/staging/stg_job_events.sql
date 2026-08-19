-- --------------------------------------------------------------------------------------
-- stg_job_events.  GRAIN: one row per job_event_id.
-- --------------------------------------------------------------------------------------
--
-- Reads:  bronze.raw_job_events
-- Feeds:  nothing yet. The state-machine validation this table exists for is Gold's.
--
-- Ported from sql/silver/30_stg_job_events.sql.
--
-- BRONZE PROJECTS NO EVENT TIME FOR THIS STREAM, and that is the thing to notice. raw_job_events
-- has generated columns for op, job_id, from_status and to_status -- and nothing else
-- (src/fleet_telemetry/load/schema.py:97-105). occurred_at, created_at and job_event_id all live
-- inside payload and have to be dug out here.
--
-- The general lesson: the column list in dbt/models/staging/_sources.yml is not the list of
-- available fields, it is the list of PROJECTED ones. Everything else is still there, one #>>
-- away. A reader who trusts the sources file as an inventory will conclude this stream has no
-- timestamp at all. That gap is now closed from the other side too -- _sources.yml documents
-- payload and names the fields this model reads out of it -- but the asymmetry below is the
-- clearest illustration of it: job_id comes from a generated column, job_event_id from the JSON,
-- and nothing about the two expressions tells you which is which.
--
-- TWO TIMESTAMPS THAT ARE NOT THE SAME THING, and neither is server_ts:
--
--   occurred_at -- when the transition happened, supplied by the caller, NO database default
--                  (docker/oltp/init.sql:203-213). This is the event time.
--   created_at  -- when the row was written. Ours, and trustworthy.
--
-- occurred_at can precede created_at by a lot, and out-of-order arrival is expected here: the
-- simulator deliberately emits deliveries that arrive before their own pickup
-- (dbt/models/staging/_sources.yml:110-113). Ordering this table by created_at and calling it
-- history would therefore be wrong, which is exactly why both are kept.
--
-- from_status is NULL for a job's first event, by design, and is NOT a missing value. So there is
-- no not_null test on it in _models.yml, and adding one would break the build on correct data.

WITH usable AS (
    SELECT *
      FROM {{ source('bronze', 'raw_job_events') }}
     -- Same three exclusions as stg_pings, and the same caveat: `payload -> 'after' IS NOT NULL`
     -- is TRUE for a Debezium delete, because Debezium emits `"after": null` and -> returns jsonb
     -- null rather than SQL NULL. The job_event_id IS NOT NULL check is what actually excludes
     -- them. 20 such deletes were measured on this stream. See stg_pings.sql for the detail.
     WHERE parse_error IS NULL
       AND payload -> 'after' IS NOT NULL
       AND payload #>> '{after,job_event_id}' IS NOT NULL
),

deduplicated AS (
    -- Earliest observation wins, same rule and same reasoning as stg_pings.sql: job_events is
    -- append-only, so the streamed create is truer than any later re-snapshot of it.
    --
    -- Not pinned by a unit test, unlike the ping rule. The argument for one is identical and the
    -- gap is the same; it is simply not what this ticket bought. Worth adding before anything
    -- downstream depends on which of two observations of one event it is reading.
    SELECT DISTINCT ON (payload #>> '{after,job_event_id}') *
      FROM usable
     ORDER BY payload #>> '{after,job_event_id}', _kafka_partition, _kafka_offset
)

SELECT
    (payload #>> '{after,job_event_id}')::bigint        AS job_event_id,
    job_id::bigint                                      AS job_id,
    from_status                                         AS from_status,
    to_status                                           AS to_status,
    (payload #>> '{after,occurred_at}')::timestamptz     AS occurred_at,
    (payload #>> '{after,created_at}')::timestamptz      AS created_at,

    -- How far out of order this event was, in seconds. Positive means it was written after it
    -- happened, which is normal; a negative value would mean a row claiming to have occurred in
    -- the future relative to its own write, which is a finding rather than a number.
    EXTRACT(
        EPOCH FROM (
            (payload #>> '{after,created_at}')::timestamptz
            - (payload #>> '{after,occurred_at}')::timestamptz
        )
    )                                                   AS write_lag_seconds,

    op                                                  AS bronze_op,
    _kafka_partition                                    AS bronze_partition,
    _kafka_offset                                       AS bronze_offset,
    _ingested_at                                        AS bronze_ingested_at
  FROM deduplicated
