-- Geometry is built lon-first, and this is the test the hand-written layer did not have.
--
-- THIS IS THE MODEL THE SILENT BREAK WAS INTRODUCED INTO. Swapping the two arguments to
-- ST_MakePoint here -- one token, in the upstream-most model -- produced: nine of nine scripts
-- ok, exit code 0, identical row counts in every table, and fleet total distance moving from
-- 34,154.8 km to 34,885.9 km, +2.14% (docs/silver-by-hand.md, section 5).
--
-- The 2% is the whole finding. A 10x error gets caught in review; a 2% error ships and then
-- somebody reconciles a report against it a month later. The reason it is 2% and not 10x is
-- geometric: The Gambia sits at ~13N, ~16W, and swapping the arguments mirrors it to ~16S, ~13E.
-- Distances BETWEEN NEARBY POINTS are nearly preserved across that mirror, so every aggregate
-- stays plausible.
--
-- And the corruption is invisible in the model that contains it. The latitude and longitude
-- columns stay untouched and correct -- measured lat_min 13.271, lat_max 13.567, lon_min -16.682,
-- lon_max -14.217 even with the swap in place. Only the derived geometry is wrong, and nobody
-- reads a geometry by eye.
--
-- STRUCTURAL, NOT GEOGRAPHIC. tests/test_transform.py:221-235 checks the coordinates fall inside
-- The Gambia on `limit 1`. This asserts the invariant itself -- the geometry holds the
-- coordinates it was built from, in that order -- on every row. No assumption about where the
-- fleet operates, so it survives the simulator moving, and it holds vacuously against the empty
-- bronze in CI rather than failing there.
--
-- Exact equality is correct for double precision here: ST_MakePoint stores the doubles it was
-- given and ST_X/ST_Y return them unchanged. No arithmetic happens in between, so there is no
-- epsilon to choose -- and choosing one would be the mistake, since a swap of 13.44 and -16.12
-- is not a rounding difference.

SELECT ping_id,
       latitude,
       longitude,
       ST_AsText(position) AS position
  FROM {{ ref('stg_pings') }}
 WHERE ST_X(position) <> longitude
    OR ST_Y(position) <> latitude
