"""What the simulator INTENDED to emit. The reference every correctness claim is checked against.

Phase 3's whole premise is that the simulator knows the truth it generated, so pipeline output can
be diffed against reality rather than inspected for plausibility. Without this table that premise is
unexercised: a pipeline nobody can falsify is indistinguishable from one that happens to look right.
docs/silver-by-hand.md:48 records the only ground-truth comparison ever made -- by hand, once, on a
closed window -- and the design spec calls this "the verification the previous project could not
have" (docs/superpowers/specs/2026-08-07-telemetry-platform-design.md:236-237).

NOT IN load/. That package is scoped to bronze by load/schema.py:1-5, and its apply() creates no
other schema. This is a different layer with the opposite typing rule, so it mirrors that file's
SHAPE without claiming its OWNERSHIP.

EVERY COLUMN HERE IS TYPED, WHICH IS THE EXACT OPPOSITE OF BRONZE'S RULE, DELIBERATELY.
Bronze is all text because a cast inside GENERATED ALWAYS runs on INSERT, so one device sending a
ping_id of "banana" would take the whole batch down -- Bronze rejecting precisely the malformed
evidence it exists to keep (load/schema.py:20-27). Nothing analogous applies here. These values are
constructed by our own code three lines before the insert, so a cast that fails is a bug in the
recorder, and the loudest possible failure at the write is what we want. Do not "align" the two:
doing so would move every such failure out of the simulator, where it is a bug report, and into the
diff, where it is an unexplained missing row.

Created here rather than only in docker/warehouse/init.sql for the reason transform/run.py:68-72
gives: that file runs only on an empty data directory, so DDL added to it never reaches a volume that
already exists -- which is how four REPLICA IDENTITY FULL statements sat unexecuted for five days
(docs/known-issues.md, section 1). It is declared in both: init.sql for a fresh clone, apply() for
every volume that already exists.

THIS IS THE ONLY MODULE THAT GIVES THE SIMULATOR WRITE ACCESS TO THE WAREHOUSE. The simulator has
always read OLTP directly (simulator/run.py:81, 97, 108, 129) but has never written to the analytics
side, and the rule that it must reach the OLTP only through the API still holds absolutely: this
table is out-of-band metadata about a run, not telemetry, and nothing downstream of Bronze reads it
except the tests.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from datetime import datetime
from typing import NamedTuple

from psycopg import Connection, connect

from fleet_telemetry import config

SCHEMA = "truth"
TABLES = ("intended_pings",)

DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.intended_pings (
    -- The device-generated UUID, recorded because it is the ONLY join key into silver.stg_pings.
    -- It is an unseeded uuid4 (simulator/run.py:218), so it cannot be recomputed from the seed and
    -- a run that failed to record it is unverifiable afterwards.
    ping_id            uuid        PRIMARY KEY,

    -- Which simulator process produced this row. Needed because ping_id is the only thing that
    -- differs between two same-seed runs: sequence numbers resume from the database rather than
    -- restarting (simulator/run.py:108), and two backfill runs OVERLAP in device_ts, since each
    -- walks from now-span to now. Without run_id the diff cannot tell one run's arrival frontier
    -- from another's, and that frontier is the whole basis of its bound.
    run_id             uuid        NOT NULL,
    seed               integer     NOT NULL,

    vehicle_id         integer     NOT NULL,
    sequence_no        bigint      NOT NULL,

    -- What the device CLAIMED, i.e. exactly the value put on the wire. Named intended_device_ts
    -- rather than device_ts so a reader is never in doubt about which side of the diff they hold.
    intended_device_ts timestamptz NOT NULL,

    -- The real instant this reading describes. Identical to intended_device_ts on a clean run; the
    -- two diverge the moment the clock-skew flag exists. Recording both is what makes skew
    -- measurable rather than merely visible.
    truth_ts           timestamptz NOT NULL,

    -- False when the simulator formed the intent and deliberately did not send it -- the
    -- sequence-gap pathology. Always true today. The column exists now so that slice needs no
    -- migration and the diff's filter is already written and already proven against clean data.
    emitted            boolean     NOT NULL,

    -- Which pathology flag caused this row to diverge from clean behaviour; null when clean, which
    -- is every row today. Text because a pathology name is a label, not a measurement.
    pathology          text,

    latitude           double precision NOT NULL,
    longitude          double precision NOT NULL,
    speed_kmh          double precision,
    heading_deg        double precision,

    _recorded_at       timestamptz NOT NULL DEFAULT now()
);

-- The declared grain. A duplicate here is a bug in OUR recorder rather than evidence about a
-- device, so unlike pings (docker/oltp/init.sql:236-238) this constraint belongs: it would inflate
-- the denominator of every ground-truth comparison downstream of it.
CREATE UNIQUE INDEX IF NOT EXISTS intended_pings_run_vehicle_seq_uk
    ON {SCHEMA}.intended_pings (run_id, vehicle_id, sequence_no);

-- The diff joins on ping_id, already covered by the primary key, and bounds per run on
-- intended_device_ts. This index serves the bound.
CREATE INDEX IF NOT EXISTS intended_pings_run_device_ts_idx
    ON {SCHEMA}.intended_pings (run_id, intended_device_ts);

-- NO HYPERTABLE, deliberately. TimescaleDB is absent in CI (the PostGIS image only), so an
-- unguarded create_hypertable would pass locally and fail there -- the failure mode
-- docker/warehouse/init.sql:15-17 already warns about. Revisit when the row count justifies it.
"""


def apply(conn: Connection) -> None:
    """Create anything missing. Safe to call on every startup, and it is."""
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        cur.execute(DDL)
    conn.commit()


class IntendedPing(NamedTuple):
    """One reading the simulator formed the intent to emit, whether or not it went out."""

    ping_id: str
    run_id: str
    seed: int
    vehicle_id: int
    sequence_no: int
    intended_device_ts: datetime
    truth_ts: datetime
    emitted: bool
    pathology: str | None
    latitude: float
    longitude: float
    speed_kmh: float | None
    heading_deg: float | None


def from_reading(
    reading: dict,
    *,
    run_id: str,
    seed: int,
    truth_ts: datetime,
    emitted: bool = True,
    pathology: str | None = None,
) -> IntendedPing:
    """Map a take_reading() dict to a truth row, copying wire values VERBATIM.

    Verbatim is the whole contract. The diff asserts equality against silver.stg_pings, so any
    rounding, normalising or re-deriving here would manufacture a difference the pipeline did not
    cause -- and the first instinct on seeing that difference would be to loosen the test.
    simulator/run.py:222-225 already rounds (6dp on coordinates, 1dp on speed and heading); those
    rounded values ARE what the API received, so they are what truth records.
    """
    return IntendedPing(
        ping_id=reading["ping_id"],
        run_id=run_id,
        seed=seed,
        vehicle_id=reading["vehicle_id"],
        sequence_no=reading["sequence_no"],
        intended_device_ts=datetime.fromisoformat(reading["device_ts"]),
        truth_ts=truth_ts,
        emitted=emitted,
        pathology=pathology,
        latitude=reading["latitude"],
        longitude=reading["longitude"],
        speed_kmh=reading.get("speed_kmh"),
        heading_deg=reading.get("heading_deg"),
    )


# The insert's column order, as data rather than buried in a string, so a test can pin it against
# IntendedPing._fields.
#
# THIS GUARD IS NOT PEDANTRY. `latitude` and `longitude` are adjacent columns of the same type, so
# reordering the tuple would exchange them with no error, no type mismatch and no failing cast.
# That is precisely the bug this project already paid for once: ST_MakePoint's arguments swapped
# passed nine of nine scripts with exit 0 and moved fleet total distance by +2.14%
# (dbt/models/staging/stg_pings.sql:157-162). Postgres cannot catch it and neither can review, so
# the parameters below are built by name.
_COLUMNS = (
    "ping_id",
    "run_id",
    "seed",
    "vehicle_id",
    "sequence_no",
    "intended_device_ts",
    "truth_ts",
    "emitted",
    "pathology",
    "latitude",
    "longitude",
    "speed_kmh",
    "heading_deg",
)


def write(conn: Connection, rows: list[IntendedPing]) -> int:
    """Insert a batch. Returns the number of rows that landed. Does NOT commit.

    The caller owns the transaction boundary, following load/writer.py:55-58 -- there because the
    Kafka offset commit must strictly follow the database one, here because the simulator decides
    whether a tick's truth and its POST succeed together.

    ON CONFLICT DO NOTHING on the primary key, so a retried flush is idempotent. Note this is a
    different judgement from the pings table, which withholds its (vehicle_id, sequence_no)
    constraint on purpose: a duplicate there is evidence about a device, whereas a duplicate here
    could only be our own recorder writing twice.
    """
    if not rows:
        return 0

    placeholders = ", ".join(["%s"] * len(_COLUMNS))
    with conn.cursor() as cur:
        cur.executemany(
            f"insert into {SCHEMA}.intended_pings ({', '.join(_COLUMNS)}) "
            f"values ({placeholders}) "
            "on conflict (ping_id) do nothing",
            [tuple(getattr(row, column) for column in _COLUMNS) for row in rows],
        )
        # psycopg 3 accumulates rowcount across an executemany, and ON CONFLICT DO NOTHING reports
        # only the rows that actually landed.
        return cur.rowcount


def postgres_sink(conn: Connection) -> Callable[[list[IntendedPing]], int]:
    """The production sink. Commits, because the simulator has no other transaction to join."""

    def sink(rows: list[IntendedPing]) -> int:
        landed = write(conn, rows)
        conn.commit()
        return landed

    return sink


class TruthRecorder:
    """Accumulates intent and hands it to a sink in batches.

    THE SINK IS INJECTED FOR THE SAME REASON config.py's loaders take an `env` mapping
    (config.py:30-33): without that seam the only way to test the simulator's recording is to stand
    up a warehouse, so the tests that matter most would be the ones least often run.

    Recording happens when the reading is TAKEN, not when the batch is flushed. That is a later
    requirement arriving early: a sequence-gap row is one the simulator formed the intent to send and
    then deliberately dropped, so it never reaches a flush and could not be recorded there.
    """

    def __init__(
        self,
        run_id: str,
        seed: int,
        sink: Callable[[list[IntendedPing]], int],
    ) -> None:
        self.run_id = run_id
        self.seed = seed
        self._sink = sink
        self._buffer: list[IntendedPing] = []
        self.recorded = 0
        self.written = 0

    def record(
        self,
        reading: dict,
        truth_ts: datetime,
        *,
        emitted: bool = True,
        pathology: str | None = None,
    ) -> None:
        self._buffer.append(
            from_reading(
                reading,
                run_id=self.run_id,
                seed=self.seed,
                truth_ts=truth_ts,
                emitted=emitted,
                pathology=pathology,
            )
        )
        self.recorded += 1

    def flush(self) -> int:
        """Hand the buffer to the sink. Returns how many rows landed."""
        if not self._buffer:
            return 0
        landed = self._sink(self._buffer)
        self._buffer = []
        self.written += landed
        return landed


def main() -> int:
    """`python -m fleet_telemetry.truth` -- also what CI runs before dbt build."""
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    target = config.warehouse()
    print(f"applying truth schema to {target.safe_dsn()}")
    with connect(target.dsn()) as conn:
        apply(conn)
    print(f"truth tables present: {', '.join(TABLES)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
