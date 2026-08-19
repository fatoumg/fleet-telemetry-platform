-- The declared grain: one row per job_id, current state. See
-- tests/assert_stg_depots_unique_on_depot_id.sql for the reasoning.
--
-- job_id is bigint here, not integer -- bigserial in the OLTP. The grain does not care, but a
-- copy-paste of this file into a new entity model might.

SELECT job_id, count(*) AS row_count
  FROM {{ ref('stg_jobs') }}
 GROUP BY job_id
HAVING count(*) > 1
