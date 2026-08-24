-- Every Bronze ping row is accounted for: modelled, rejected, or removed as a duplicate.
--
--     bronze = silver + rejects + duplicates_removed
--
-- Any shortfall beyond those three is a row that vanished with no evidence, which is the one
-- outcome Bronze exists to make impossible.
--
-- THE DUPLICATE TERM IS NOT DECORATION, AND OMITTING IT WAS A REAL BUG IN THIS TEST. The first
-- version asserted `bronze = silver + rejects`, which held for as long as DISTINCT ON (ping_id)
-- discarded nothing -- and docs/silver-by-hand.md section 2 measured exactly that: 350,742 rows,
-- 350,742 distinct ids, deduplication removing zero. Then Bronze acquired genuine duplicates and
-- the assertion failed on a pipeline that was working correctly:
--
--     bronze 190,520 = silver 190,484 + rejects 16 + duplicates 20
--                    ^ the first version stopped here and was short by 20
--
-- A reconciliation that breaks the first time deduplication does its job is worse than no
-- reconciliation, because the obvious repair is to loosen it.
--
-- WHAT THE DUPLICATES ACTUALLY WERE, because it changes what this test is worth. 20 ping_ids
-- appeared twice, every pair exactly 20 offsets apart, and BOTH copies carried op='c':
--
--     017bd8af-0265-46a0-b08c-a2e6a1039af2   c@197396  c@197416
--     12d84b39-f677-4af7-b5d6-6f5d90b976e1   c@197401  c@197421
--
-- Not a create-plus-re-snapshot, which is the case stg_pings.sql's header anticipated. Two
-- creates: a producer-side redelivery of a batch of 20 messages, which the unique index on
-- (_kafka_partition, _kafka_offset) cannot absorb because the redelivery landed at DIFFERENT
-- offsets. Note what that does to the tie-break -- "prefer op='c'" cannot separate these two, so
-- lowest-offset is the only rule that resolves them, which is precisely the rule stg_pings uses
-- and the unit test pins.
--
-- COMPUTED FROM THE SOURCE, NOT FROM THE MODEL, or this test would be circular. The reject
-- predicate is the exact complement of stg_pings' `usable` filter, so bronze - rejects = usable
-- identically, and deriving the duplicate count that way would assert nothing. Instead it comes
-- from the source directly: rows sharing a ping_id, counted as excess copies.
--
-- `ping_id IS NOT NULL` alone characterises the usable set, and that is worth knowing rather than
-- assuming. ping_id is a generated column projected from payload, so a row whose bytes never
-- parsed has a null payload and therefore a null ping_id -- parse_error and a missing after-image
-- are both already implied.
--
-- THIS IS ALSO THE DRIFT DETECTOR. The reject predicate is written twice -- once as the `usable`
-- filter in stg_pings.sql, once in stg_rejected_rows.sql -- and no abstraction prevents them
-- disagreeing. If they ever do, this arithmetic stops balancing. That is the deliberate trade:
-- assert the property rather than engineer away the possibility with a macro.
--
-- BOUNDED BY max(bronze_offset), NOT COUNTING ALL OF BRONZE. The simulator runs continuously at
-- ~2 pings/second, so comparing unbounded counts is two measurements of a moving target -- the
-- first attempt at this by hand showed a 60-row gap that was entirely the seconds between the two
-- queries (docs/silver-by-hand.md:56-60). A test that fails for reasons unrelated to correctness
-- gets "fixed" by loosening it, which is how a real reconciliation check becomes decorative.
--
-- Every term is bounded by the same offset. Without that, a malformed row or a duplicate arriving
-- after the bound would be counted on one side only, and the sum would break on correct data.
--
-- WHAT THIS PROVES IN CI: nothing. Bronze is empty there, max() is null, every comparison is null,
-- and this returns no rows. Said out loud because a green check reads like coverage and is not --
-- what CI catches is that the SQL parses and the columns exist.

WITH bound AS (
    SELECT max(bronze_offset) AS m FROM {{ ref('stg_pings') }}
),

counted AS (
    SELECT
        (SELECT count(*) FROM {{ source('bronze', 'raw_ping_events') }}
          WHERE _kafka_offset <= bound.m)                       AS bronze_rows,
        (SELECT count(*) FROM {{ ref('stg_pings') }})            AS silver_rows,
        (SELECT count(*) FROM {{ ref('stg_rejected_rows') }}
          WHERE source_table = 'raw_ping_events'
            AND _kafka_offset <= bound.m)                       AS reject_rows,
        (SELECT count(*) - count(DISTINCT ping_id)
           FROM {{ source('bronze', 'raw_ping_events') }}
          WHERE ping_id IS NOT NULL
            AND _kafka_offset <= bound.m)                       AS duplicate_rows
      FROM bound
)

SELECT * FROM counted
 WHERE bronze_rows <> silver_rows + reject_rows + duplicate_rows
