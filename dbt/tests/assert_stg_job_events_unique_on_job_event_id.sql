-- The declared grain: one row per job_event_id.
--
-- NOTE THE GRAIN IS THE EVENT ID, not (job_id, to_status). Out-of-order arrival is expected on
-- this stream -- the simulator deliberately emits deliveries that arrive before their own pickup
-- -- and a job may legitimately revisit a status, so a grain built from the business columns
-- would fail on correct data. The surrogate key from the OLTP is the only thing that identifies
-- one transition.
--
-- See tests/assert_stg_depots_unique_on_depot_id.sql for why this is a singular test rather than
-- the built-in `unique`.

SELECT job_event_id, count(*) AS row_count
  FROM {{ ref('stg_job_events') }}
 GROUP BY job_event_id
HAVING count(*) > 1
