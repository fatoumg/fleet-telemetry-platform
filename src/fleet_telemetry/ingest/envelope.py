"""Which bronze table a topic belongs to, and how to decode a message without ever losing one.

Deliberately free of any Kafka import. Two reasons, and the second is the real one:

  * CI installs [dev] only, so a module-scope `import confluent_kafka` would turn every test in
    this file into a collection error;
  * the decisions here -- what counts as malformed, what happens to a message nothing expects --
    are the ones worth testing, and they should be testable without a broker.

THE ONE RULE: this function never raises and never returns nothing. Every input produces a row.
Bronze exists to hold the evidence, and the evidence you most want is the message that broke
something (src/fleet_telemetry/load/__init__.py:6-8).
"""

from __future__ import annotations

import json

# Topic -> bronze table. Topics are `{topic.prefix}.{schema}.{table}`, and the prefix is
# `fleet`, set in docker/debezium/fleet-connector.json.
#
# Three tables rather than six, because the streams have genuinely different shapes: pings are
# append-only and enormous, job_events are append-only and arrive out of order, and the four
# mutable entities carry before/after images that Type 2 dimensions are built from
# (src/fleet_telemetry/ingest/__init__.py:10-14).
TOPIC_TABLES: dict[str, str] = {
    "fleet.public.pings": "raw_ping_events",
    "fleet.public.job_events": "raw_job_events",
    "fleet.public.vehicles": "raw_cdc_entities",
    "fleet.public.drivers": "raw_cdc_entities",
    "fleet.public.depots": "raw_cdc_entities",
    "fleet.public.jobs": "raw_cdc_entities",
}

# (payload as JSON text, raw bytes as text, parse error). Exactly one of the first two is
# non-null; the third is non-null whenever the first is null.
DecodedValue = tuple[str | None, str | None, str | None]


def table_for_topic(topic: str) -> str | None:
    """None for anything unrecognised -- including Connect's own internal topics.

    Returning None rather than a default table is the point. A topic nobody planned for is a
    finding: someone added a table to table.include.list, or the prefix changed. Routing it to
    a plausible-looking destination would hide that in a table where nobody would look.
    """
    return TOPIC_TABLES.get(topic)


def decode(value: bytes | None) -> DecodedValue:
    """Turn message bytes into something bronze can store. Never raises.

    A null value is a tombstone. tombstones.on.delete is false in the connector config so one
    should not arrive, but dropping the message instead of recording it would leave a gap in
    the offset sequence with nothing to explain it -- and offset gaps are how you conclude the
    loader lost data when it did not.
    """
    if value is None:
        return None, "", "tombstone: null message value"

    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError as exc:
        # No lossless text representation exists, so keep the repr rather than nothing. The
        # bytes are still in the topic until retention expires; the repr is what survives.
        return None, repr(value), f"utf-8 decode failed: {exc}"

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        return None, text, str(exc)

    if not isinstance(parsed, dict):
        # jsonb would accept `[1,2,3]` without complaint, and then every generated column is
        # null with no visible cause. Better to record why.
        return None, text, f"payload is a {type(parsed).__name__}, not a JSON object"

    return text, None, None
