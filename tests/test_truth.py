"""The truth row shape and the recorder. No database, no HTTP, no docker.

Hermetic because the injected sink makes it so (truth.py's TruthRecorder docstring explains why the
seam exists). These are the tests that must run on every commit -- the integration ones in
test_truth_load.py need a warehouse and therefore do not.
"""

from __future__ import annotations

from datetime import UTC, datetime

from fleet_telemetry import truth

READING = {
    "ping_id": "11111111-1111-4111-8111-111111111111",
    "vehicle_id": 7,
    "sequence_no": 42,
    "device_ts": "2026-08-28T10:00:00+00:00",
    "latitude": 13.4549,
    "longitude": -16.5790,
    "speed_kmh": 45.0,
    "heading_deg": 91.5,
}
RUN_ID = "22222222-2222-4222-8222-222222222222"
TRUTH_TS = datetime(2026, 8, 28, 10, 0, tzinfo=UTC)


def _collecting_sink(into: list):
    """A sink that records what it was handed and reports every row as landed."""

    def sink(rows):
        into.extend(rows)
        return len(rows)

    return sink


# --------------------------------------------------------------------------------------
# the mapper
# --------------------------------------------------------------------------------------


def test_from_reading_copies_the_wire_values_verbatim():
    """Verbatim matters: the diff compares for equality, so any transformation here is a difference
    the pipeline did not cause."""
    row = truth.from_reading(READING, run_id=RUN_ID, seed=42, truth_ts=TRUTH_TS)

    assert row.ping_id == READING["ping_id"]
    assert row.vehicle_id == 7
    assert row.sequence_no == 42
    assert row.latitude == 13.4549
    assert row.longitude == -16.5790
    assert row.speed_kmh == 45.0
    assert row.heading_deg == 91.5
    assert row.run_id == RUN_ID
    assert row.seed == 42


def test_intended_device_ts_is_the_claim_and_truth_ts_is_the_fact():
    """Identical on a clean run. The columns are separate so clock skew becomes measurable rather
    than merely visible."""
    skewed = READING | {"device_ts": "2026-08-28T13:00:00+00:00"}
    row = truth.from_reading(skewed, run_id=RUN_ID, seed=42, truth_ts=TRUTH_TS)

    assert row.intended_device_ts == datetime(2026, 8, 28, 13, 0, tzinfo=UTC)
    assert row.truth_ts == TRUTH_TS


def test_a_clean_row_is_emitted_with_no_pathology():
    row = truth.from_reading(READING, run_id=RUN_ID, seed=42, truth_ts=TRUTH_TS)
    assert row.emitted is True
    assert row.pathology is None


def test_a_dropped_row_is_still_recorded():
    """The sequence-gap pathology: the intent was formed, the send was not made. Recording it is the
    only thing that makes the gap provably the injection rather than a pipeline bug."""
    row = truth.from_reading(
        READING, run_id=RUN_ID, seed=42, truth_ts=TRUTH_TS, emitted=False, pathology="sequence_gap"
    )
    assert row.emitted is False
    assert row.pathology == "sequence_gap"


def test_a_missing_nullable_field_becomes_none_rather_than_raising():
    """speed_kmh and heading_deg are nullable in the OLTP. A reading without them is a reading, not
    an error -- and inventing a zero speed would be a measurement nobody took."""
    row = truth.from_reading(
        {k: v for k, v in READING.items() if k not in ("speed_kmh", "heading_deg")},
        run_id=RUN_ID,
        seed=42,
        truth_ts=TRUTH_TS,
    )
    assert row.speed_kmh is None
    assert row.heading_deg is None


def test_insert_columns_match_the_row_shape_exactly():
    """The one failure mode Postgres cannot catch.

    write() builds each parameter tuple by getattr over _COLUMNS, so a mismatch between that list and
    IntendedPing's fields is either a dropped column or an AttributeError. This pins them together so
    the failure lands here, in a test that runs in milliseconds, rather than in a warehouse full of
    coordinates with latitude and longitude exchanged -- adjacent columns of the same type, which no
    cast and no constraint would object to.

    Reaching into the private _COLUMNS is deliberate: making it public purely so a test may read it
    would widen the module's surface to describe an internal coupling.
    """
    assert set(truth._COLUMNS) == set(truth.IntendedPing._fields)
    assert len(truth._COLUMNS) == len(truth.IntendedPing._fields)


# --------------------------------------------------------------------------------------
# the recorder
# --------------------------------------------------------------------------------------


def test_recorder_buffers_until_flushed():
    """Buffered because the write must be batched: truth rows are 1:1 with pings, and a six-hour
    forty-vehicle backfill is roughly 190k of them."""
    written: list = []
    rec = truth.TruthRecorder(run_id=RUN_ID, seed=42, sink=_collecting_sink(written))

    rec.record(READING, TRUTH_TS)
    assert written == []

    assert rec.flush() == 1
    assert len(written) == 1
    assert written[0].ping_id == READING["ping_id"]


def test_flush_is_a_no_op_when_nothing_is_buffered():
    """Hit on the final flush of every run whose last batch was already sent."""
    calls: list = []
    rec = truth.TruthRecorder(
        run_id=RUN_ID, seed=42, sink=lambda rows: calls.append(rows) or len(rows)
    )
    assert rec.flush() == 0
    assert calls == []


def test_recorder_counts_everything_it_recorded_including_unemitted():
    written: list = []
    rec = truth.TruthRecorder(run_id=RUN_ID, seed=42, sink=_collecting_sink(written))

    rec.record(READING, TRUTH_TS)
    rec.record(
        READING | {"ping_id": "55555555-5555-4555-8555-555555555555"}, TRUTH_TS, emitted=False
    )
    rec.flush()

    assert rec.recorded == 2
    assert rec.written == 2
    assert [r.emitted for r in written] == [True, False]


def test_written_counts_what_landed_not_what_was_offered():
    """The sink reports what the database accepted. A redelivered batch is suppressed by ON CONFLICT,
    and reporting it as written would overstate coverage in the run summary."""
    rec = truth.TruthRecorder(run_id=RUN_ID, seed=42, sink=lambda rows: 0)

    rec.record(READING, TRUTH_TS)
    assert rec.flush() == 0
    assert rec.recorded == 1
    assert rec.written == 0


def test_flush_clears_the_buffer_so_a_second_flush_sends_nothing_twice():
    """Otherwise every batch would be re-sent on the next flush, and the ON CONFLICT that absorbs it
    would hide the bug entirely."""
    written: list = []
    rec = truth.TruthRecorder(run_id=RUN_ID, seed=42, sink=_collecting_sink(written))

    rec.record(READING, TRUTH_TS)
    rec.flush()
    rec.flush()

    assert len(written) == 1
