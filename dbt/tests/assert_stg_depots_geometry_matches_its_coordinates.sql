-- Geometry is built lon-first, and this is the test that says so.
--
-- ST_MakePoint takes LONGITUDE FIRST. Swapping the arguments does not error, and the columns the
-- bug lives next to stay perfectly correct -- only the derived geometry is wrong. Measured, on
-- the equivalent swap in stg_pings: fleet total distance moved from 34,154.8 km to 34,885.9 km,
-- +2.14%, with nine of nine scripts reporting ok and exit code 0
-- (docs/silver-by-hand.md, section 5). A 10x error gets caught in review; a 2% error ships and
-- then somebody reconciles a report against it a month later.
--
-- STRUCTURAL, NOT GEOGRAPHIC. tests/test_transform.py:221-235 checks the coordinates fall inside
-- The Gambia, on `limit 1`. This asserts the invariant itself -- that the geometry holds the
-- coordinates it was built from, in that order -- on every row. It needs no assumption about
-- where the fleet operates, so it keeps working if the simulator ever moves, and it holds
-- vacuously against the empty bronze in CI rather than failing there.
--
-- Exact equality is correct for double precision here. ST_MakePoint stores the doubles it was
-- given and ST_X/ST_Y return them unchanged; no arithmetic and no rounding happens in between,
-- so there is no epsilon to choose.
--
-- A row with a null latitude or longitude yields a null location, ST_X of which is null, so the
-- comparison is null and the row is not returned. Absence is not a swap -- not_null on those
-- columns is asserted separately in _models.yml.

SELECT depot_id,
       latitude,
       longitude,
       ST_AsText(location) AS location
  FROM {{ ref('stg_depots') }}
 WHERE ST_X(location) <> longitude
    OR ST_Y(location) <> latitude
