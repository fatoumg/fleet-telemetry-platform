"""The bronze schema. Owned here, not by dbt and not by docker/warehouse/init.sql.

Three owners of one schema is how a warehouse acquires two sources of truth, so the rule in
docker/warehouse/init.sql:3-5 is absolute: that file creates extensions and schemas, nothing
else. dbt owns silver, gold and marts (dbt/profiles.yml:28). Bronze is ours.

Applied at the start of every ingest run rather than as a manual step, because a loader that
requires someone to remember a setup command is a loader that fails on a fresh clone.

TWO DESIGN POINTS WORTH THE READ.

**Python inserts the envelope and nothing else.** Every column Silver reads is declared here as
GENERATED ALWAYS AS ... STORED -- a projection over `payload`, evaluated by Postgres. That keeps
the promise in load/__init__.py ("no reshaping") while keeping the contract in
dbt/models/staging/_sources.yml honest: those really are columns, so `source()` references and
dbt tests work unchanged.

**No generated column casts.** They are all text, including `source_ts_ms` and `sequence_no`,
which are obviously numeric. A cast inside GENERATED ALWAYS is evaluated on INSERT, so a device
sending ping_id "banana" would raise and take the whole batch with it -- Bronze rejecting
precisely the malformed evidence it exists to preserve. Postgres 16 has no TRY_CAST and no way
to mark a cast non-fatal, so the only safe projection is the one that cannot fail: `#>>` returns
text or NULL, always. Silver casts, where a failure is a test result rather than data loss.

`before` and `after` stay jsonb because `->` cannot fail either, and they are quoted because
BEFORE and AFTER are keywords -- unreserved, so unquoted would work, but quoting removes any
question about how dbt's Jinja renders them.
"""

from __future__ import annotations

import argparse

from psycopg import Connection, connect

from fleet_telemetry import config

BRONZE_TABLES = (
    "raw_ping_events",
    "raw_cdc_entities",
    "raw_job_events",
    "poll_rows",
    "poll_watermarks",
)

# Columns every CDC table carries. The Kafka coordinate is the deduplication key: the OLTP
# already removed device retries with ON CONFLICT (ping_id) DO NOTHING, so a duplicate arriving
# here can only be a broker redelivery, and (partition, offset) identifies a message uniquely
# and immutably. ping_id would be wrong twice over -- it is null for deletes, and it would
# collapse a genuine re-snapshot (op='r' after op='c') into the create it re-reads.
_KAFKA_COLUMNS = """
    _topic           text        NOT NULL,
    _kafka_partition integer     NOT NULL,
    _kafka_offset    bigint      NOT NULL,
    -- Broker-assigned; null when the producer set no timestamp. Not an event time: use
    -- source.ts_ms for that, which is the OLTP commit time from the WAL.
    _kafka_timestamp bigint,
    _ingested_at     timestamptz NOT NULL DEFAULT now(),
    -- payload is null only when the bytes would not parse. raw_payload is null only when they
    -- did. The CHECK below makes "neither" impossible, so no row can arrive carrying nothing.
    payload          jsonb,
    raw_payload      text,
    parse_error      text
"""

DDL = f"""
CREATE TABLE IF NOT EXISTS bronze.raw_ping_events (
{_KAFKA_COLUMNS},
    op          text GENERATED ALWAYS AS (payload ->> 'op') STORED,
    ping_id     text GENERATED ALWAYS AS (payload #>> '{{after,ping_id}}') STORED,
    sequence_no text GENERATED ALWAYS AS (payload #>> '{{after,sequence_no}}') STORED,
    device_ts   text GENERATED ALWAYS AS (payload #>> '{{after,device_ts}}') STORED,
    server_ts   text GENERATED ALWAYS AS (payload #>> '{{after,server_ts}}') STORED,
    CONSTRAINT raw_ping_events_has_evidence
        CHECK (payload IS NOT NULL OR raw_payload IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS raw_ping_events_kafka_uk
    ON bronze.raw_ping_events (_kafka_partition, _kafka_offset);

CREATE TABLE IF NOT EXISTS bronze.raw_cdc_entities (
{_KAFKA_COLUMNS},
    source_table text  GENERATED ALWAYS AS (payload #>> '{{source,table}}') STORED,
    op           text  GENERATED ALWAYS AS (payload ->> 'op') STORED,
    "before"     jsonb GENERATED ALWAYS AS (payload -> 'before') STORED,
    "after"      jsonb GENERATED ALWAYS AS (payload -> 'after') STORED,
    source_ts_ms text  GENERATED ALWAYS AS (payload #>> '{{source,ts_ms}}') STORED,
    CONSTRAINT raw_cdc_entities_has_evidence
        CHECK (payload IS NOT NULL OR raw_payload IS NOT NULL)
);

-- Four topics land here, so the topic is part of the key. Small table: ~2,000 changes over
-- 30 days at target scale (docs/source-system-reference.md, entity churn).
CREATE UNIQUE INDEX IF NOT EXISTS raw_cdc_entities_kafka_uk
    ON bronze.raw_cdc_entities (_topic, _kafka_partition, _kafka_offset);

CREATE TABLE IF NOT EXISTS bronze.raw_job_events (
{_KAFKA_COLUMNS},
    op          text GENERATED ALWAYS AS (payload ->> 'op') STORED,
    job_id      text GENERATED ALWAYS AS (payload #>> '{{after,job_id}}') STORED,
    from_status text GENERATED ALWAYS AS (payload #>> '{{after,from_status}}') STORED,
    to_status   text GENERATED ALWAYS AS (payload #>> '{{after,to_status}}') STORED,
    CONSTRAINT raw_job_events_has_evidence
        CHECK (payload IS NOT NULL OR raw_payload IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS raw_job_events_kafka_uk
    ON bronze.raw_job_events (_kafka_partition, _kafka_offset);

-- --------------------------------------------------------------------------------------
-- The batch poller's output. Deliberately a different shape.
-- --------------------------------------------------------------------------------------
--
-- Note what this table CANNOT have: an op code and a before-image. A poller reads current
-- state, so it cannot know whether a row is new or changed, cannot see what it used to say,
-- and cannot observe a row that no longer exists. Forcing this into a Debezium-shaped envelope
-- would fabricate those fields. The missing columns are the finding.
CREATE TABLE IF NOT EXISTS bronze.poll_rows (
    poll_row_id     bigserial PRIMARY KEY,
    source_table    text        NOT NULL,
    row_image       jsonb       NOT NULL,
    -- The watermark value that caused this row to be selected. Keeping it makes a leaked
    -- watermark diagnosable after the fact rather than merely suspected.
    watermark_value timestamptz NOT NULL,
    _polled_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS poll_rows_source_table_idx ON bronze.poll_rows (source_table);

CREATE TABLE IF NOT EXISTS bronze.poll_watermarks (
    source_table     text        PRIMARY KEY,
    -- pings has no updated_at (docker/oltp/init.sql:214-224), so it watermarks on server_ts.
    -- Recording which column was used stops a later reader assuming they are the same thing.
    watermark_column text        NOT NULL,
    watermark_value  timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL DEFAULT now()
);
"""


def apply(conn: Connection) -> None:
    """Create anything missing. Safe to call on every startup, and it is."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        cur.execute(DDL)
    conn.commit()


def main() -> int:
    """`python -m fleet_telemetry.load.schema` -- also what CI runs before dbt build."""
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    target = config.warehouse()
    print(f"applying bronze schema to {target.safe_dsn()}")
    with connect(target.dsn()) as conn:
        apply(conn)
    print(f"bronze tables present: {', '.join(BRONZE_TABLES)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
