-- At most one open-ended version per vehicle. AT MOST, not exactly one, and the difference is
-- the point: a hard-deleted vehicle has ZERO current versions, because its final version was
-- closed at the delete's commit time and flagged is_deleted. Measured on this volume: 44 distinct
-- vehicle_ids, 40 current versions -- so an `exactly one` assertion would fail on four ids right
-- now (9101, 9401, 9601, 9602, all hard-deleted; docs/known-issues.md, section 3), on correct
-- data, and would keep failing as long as DELETE /vehicles/{id} stays a hard delete on purpose.
--
-- This is the assertion a grain test cannot make. Uniqueness on (vehicle_id, valid_from) is
-- satisfied by a dimension in which every version is open-ended, which is precisely the shape a
-- broken lead() produces -- and every "what did the fleet look like on the 14th" query would
-- then return the entire history for every vehicle rather than one row each.
--
-- Severity is error project-wide (dbt/dbt_project.yml:44-46).

SELECT vehicle_id, count(*) AS current_versions
  FROM {{ ref('dim_vehicle') }}
 WHERE is_current
 GROUP BY vehicle_id
HAVING count(*) > 1
