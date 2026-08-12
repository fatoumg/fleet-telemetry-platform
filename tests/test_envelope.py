"""Routing and decoding. Hermetic on purpose.

This module must not import confluent_kafka, and this test proves it: CI installs [dev] only
(.github/workflows/ci.yml:37), so anything importing the Kafka client at module scope would
turn a logic test into a collection error.
"""

from __future__ import annotations

import json

from fleet_telemetry.ingest import envelope


def test_the_three_streams_route_to_three_tables():
    assert envelope.table_for_topic("fleet.public.pings") == "raw_ping_events"
    assert envelope.table_for_topic("fleet.public.job_events") == "raw_job_events"
    for entity in ("vehicles", "drivers", "depots", "jobs"):
        assert envelope.table_for_topic(f"fleet.public.{entity}") == "raw_cdc_entities"


def test_an_unknown_topic_routes_nowhere_rather_than_guessing():
    """A topic appearing that nothing expects is a finding. Guessing a destination would bury
    it in a table where nobody would look for it."""
    assert envelope.table_for_topic("fleet.public.audit_log") is None
    assert envelope.table_for_topic("_connect_offsets") is None


def test_valid_json_decodes_with_no_error_and_no_raw_copy():
    """The raw bytes are kept only when they could not be parsed. Keeping both for every row
    would roughly double a table already headed for ~22M rows."""
    value = json.dumps({"op": "c", "after": {"ping_id": "abc"}}).encode()
    payload, raw, error = envelope.decode(value)
    assert json.loads(payload)["op"] == "c"
    assert raw is None
    assert error is None


def test_malformed_bytes_are_kept_verbatim_with_the_error():
    payload, raw, error = envelope.decode(b'{"op": "c", ')
    assert payload is None
    assert raw == '{"op": "c", '
    assert "Expecting" in error


def test_undecodable_bytes_are_still_kept():
    """A payload that is not even valid UTF-8. Bronze has no way to store it as text without
    loss, so it stores the repr and says so -- which is still more than dropping it."""
    payload, raw, error = envelope.decode(b"\xff\xfe\x00")
    assert payload is None
    assert raw is not None
    assert "utf-8" in error.lower()


def test_a_tombstone_is_recorded_rather_than_dropped():
    """tombstones.on.delete is false, so one should never arrive. 'Should never' is not a
    guarantee, and a silently dropped message is an offset gap nobody can explain later."""
    payload, raw, error = envelope.decode(None)
    assert payload is None
    assert raw == ""
    assert "tombstone" in error


def test_valid_json_that_is_not_an_object_is_kept_and_flagged():
    """jsonb accepts a bare array or number quite happily, and then every generated column is
    null with no explanation. Flagging it here makes the cause findable."""
    payload, raw, error = envelope.decode(b"[1, 2, 3]")
    assert payload is None
    assert raw == "[1, 2, 3]"
    assert "object" in error
