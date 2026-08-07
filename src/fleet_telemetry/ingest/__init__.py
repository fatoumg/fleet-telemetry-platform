"""Bronze: consume Debezium change events from Kafka and persist them as received.

Append-only, never edited. Events are stored verbatim -- including malformed ones, which are
kept rather than dropped so that the pipeline's own failure modes stay measurable.

Bronze is not a convenience layer here. A Kafka topic has a retention window, not a memory:
once an event ages out it is gone, and no amount of reprocessing brings it back. Bronze is
the durable record everything downstream is rebuilt from.

Two independent streams land here, because they have genuinely different shapes:

  * high-volume append-only pings, which are never updated;
  * change events for mutable entities, carrying before/after images and an op code, which
    is what allows Gold to reconstruct Type 2 history covering every committed change.
"""
