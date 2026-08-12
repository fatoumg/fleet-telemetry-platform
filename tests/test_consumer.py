"""The consumer's message-to-row mapping. Hermetic -- no broker, no database.

A stand-in message object rather than a mock of the client: what is under test is the mapping
from a message to a bronze row, and the four accessors that mapping uses are a value, not a
behaviour. Mocking Consumer would assert that we call confluent_kafka correctly, which is a
different claim from "the right row lands".
"""

from __future__ import annotations

import json

from fleet_telemetry.ingest import consumer


class FakeMessage:
    """The five accessors the consumer uses on a message."""

    def __init__(self, topic, partition, offset, value, timestamp=(1, 1_754_568_000_000)):
        self._topic, self._partition, self._offset = topic, partition, offset
        self._value, self._timestamp = value, timestamp

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def timestamp(self):
        return self._timestamp

    def error(self):
        return None


def test_messages_map_to_rows_addressed_to_the_right_tables():
    messages = [
        FakeMessage("fleet.public.pings", 0, 10, json.dumps({"op": "c"}).encode()),
        FakeMessage("fleet.public.vehicles", 0, 3, json.dumps({"op": "u"}).encode()),
    ]
    rows, unrouted = consumer.to_rows(messages)
    assert [row.table for row in rows] == ["raw_ping_events", "raw_cdc_entities"]
    assert rows[0].offset == 10
    assert unrouted == []


def test_an_unrouted_topic_is_reported_not_silently_dropped():
    messages = [FakeMessage("fleet.public.audit_log", 0, 1, b"{}")]
    rows, unrouted = consumer.to_rows(messages)
    assert rows == []
    assert unrouted == ["fleet.public.audit_log"]


def test_a_malformed_message_becomes_a_row_carrying_its_error():
    messages = [FakeMessage("fleet.public.pings", 0, 11, b"{not json")]
    rows, _ = consumer.to_rows(messages)
    assert rows[0].payload is None
    assert rows[0].raw_payload == "{not json"
    assert rows[0].parse_error


def test_a_message_with_no_broker_timestamp_still_maps():
    """timestamp() returns (TIMESTAMP_NOT_AVAILABLE, -1) when the producer set none."""
    messages = [FakeMessage("fleet.public.pings", 0, 12, b"{}", timestamp=(0, -1))]
    rows, _ = consumer.to_rows(messages)
    assert rows[0].timestamp is None
