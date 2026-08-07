-- Operational database for the fleet application. Runs once, on an empty data directory.
--
-- This is the SOURCE SYSTEM. It is shaped the way an application database is shaped, not the way
-- an analytics schema is shaped, and the differences are the point of phase 1:
--
--   * normalised, not dimensional -- no fact/dimension split, no surrogate keys for history
--   * optimised for single-row reads and writes, not for scanning millions of rows
--   * it keeps only the CURRENT state of a driver or vehicle; the history is gone the moment a
--     row is updated. Recovering that history is what phase 3's SCD Type 2 work is for
--
-- Two deliberate choices worth understanding before you read further.
--
-- LAT/LON ARE PLAIN NUMBERS HERE, NOT POSTGIS GEOMETRY.
-- The warehouse has PostGIS; this database does not need it. An application recording a GPS
-- reading stores two doubles, because it never asks spatial questions -- "which vehicles are
-- within 2 km of this depot" is an analytics question. Constructing geometry from these columns
-- is a job for Silver. Real source systems are usually plainer than the warehouse that consumes
-- them, and pretending otherwise makes the transformation layer look pointless.
--
-- EVERY TIMESTAMP IS timestamptz, AND THE DATABASE IS UTC.
-- Device clock skew is already a first-class problem here (see `pings`). Adding timezone
-- ambiguity on top of it would make the phase 3 lateness work almost impossible to reason about.

-- --------------------------------------------------------------------------------------
-- updated_at maintenance
-- --------------------------------------------------------------------------------------
--
-- Phase 2 starts by building a naive batch poller: `SELECT ... WHERE updated_at > watermark`.
-- That only works if updated_at is actually maintained, and it must be maintained by the
-- DATABASE rather than by application code -- if the app forgets to set it on one code path,
-- the poller silently skips those rows and you get a data loss bug that looks like nothing.
--
-- A trigger cannot be forgotten. This is the first example in the project of pushing a
-- correctness guarantee down to the layer that cannot be bypassed.

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- --------------------------------------------------------------------------------------
-- depots -- reference data, changes very rarely
-- --------------------------------------------------------------------------------------

CREATE TABLE depots (
    depot_id     integer PRIMARY KEY,
    name         text        NOT NULL UNIQUE,
    latitude     double precision NOT NULL,
    longitude    double precision NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TRIGGER depots_updated_at BEFORE UPDATE ON depots
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Real towns, real coordinates. The fleet runs the south-bank road from the coast to Basse.
INSERT INTO depots (depot_id, name, latitude, longitude) VALUES
    (1, 'Banjul',          13.4549, -16.5790),
    (2, 'Serekunda',       13.4381, -16.6781),
    (3, 'Brikama',         13.2714, -16.6494),
    (4, 'Bakau',           13.4781, -16.6819),
    (5, 'Kerewan',         13.4900, -16.0900),
    (6, 'Farafenni',       13.5667, -15.6000),
    (7, 'Soma',            13.4000, -15.5333),
    (8, 'Basse Santa Su',  13.3167, -14.2167);

-- --------------------------------------------------------------------------------------
-- drivers and vehicles -- MUTABLE. This is where SCD Type 2 will come from.
-- --------------------------------------------------------------------------------------
--
-- Note what these tables do NOT have: any record of what they used to say. Change a driver's
-- home depot and the previous value is simply gone. That is correct for an application -- it
-- only cares about now -- and it is exactly the problem the warehouse has to solve, because
-- "how many trips ran out of Brikama last month" needs to know which depot each driver was
-- attached to AT THE TIME, not today.

CREATE TABLE drivers (
    driver_id      integer PRIMARY KEY,
    full_name      text        NOT NULL,
    phone          text,
    status         text        NOT NULL DEFAULT 'active'
                   CHECK (status IN ('active', 'inactive', 'suspended')),
    home_depot_id  integer     NOT NULL REFERENCES depots (depot_id),
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TRIGGER drivers_updated_at BEFORE UPDATE ON drivers
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE INDEX drivers_updated_at_idx ON drivers (updated_at);

CREATE TABLE vehicles (
    vehicle_id         integer PRIMARY KEY,
    plate              text        NOT NULL UNIQUE,
    capacity           integer     NOT NULL,
    status             text        NOT NULL DEFAULT 'active'
                       CHECK (status IN ('active', 'maintenance', 'retired')),
    -- Nullable and reassignable: a vehicle without a driver today is normal, and the
    -- reassignment history is precisely what Type 2 dimensions exist to preserve.
    current_driver_id  integer     REFERENCES drivers (driver_id),
    home_depot_id      integer     NOT NULL REFERENCES depots (depot_id),
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TRIGGER vehicles_updated_at BEFORE UPDATE ON vehicles
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE INDEX vehicles_updated_at_idx ON vehicles (updated_at);

-- 40 drivers, 40 vehicles, spread across the depots. generate_series keeps the seed data in
-- the schema rather than in a script, so a fresh container is immediately usable.
INSERT INTO drivers (driver_id, full_name, phone, home_depot_id)
SELECT n,
       'Driver ' || lpad(n::text, 3, '0'),
       '+220' || lpad((7000000 + n)::text, 7, '0'),
       1 + (n % 8)
FROM generate_series(1, 40) AS n;

INSERT INTO vehicles (vehicle_id, plate, capacity, current_driver_id, home_depot_id)
SELECT n,
       'BJL-' || lpad(n::text, 4, '0'),
       CASE WHEN n % 4 = 0 THEN 32 ELSE 14 END,   -- a few coaches among the minibuses
       n,
       1 + (n % 8)
FROM generate_series(1, 40) AS n;

-- --------------------------------------------------------------------------------------
-- jobs -- a status state machine
-- --------------------------------------------------------------------------------------

CREATE TABLE jobs (
    job_id                      bigserial PRIMARY KEY,
    vehicle_id                  integer REFERENCES vehicles (vehicle_id),
    pickup_depot_id             integer NOT NULL REFERENCES depots (depot_id),
    dropoff_depot_id            integer NOT NULL REFERENCES depots (depot_id),
    status                      text    NOT NULL DEFAULT 'created'
                                CHECK (status IN ('created', 'assigned', 'picked_up',
                                                  'delivered', 'cancelled')),
    -- The estimate is recorded when the job is created and never revised. mart_job_performance
    -- compares it against what actually happened, so it must be frozen at creation -- an
    -- estimate that gets quietly updated to match reality measures nothing.
    estimated_duration_minutes  integer NOT NULL,
    requested_at                timestamptz NOT NULL DEFAULT now(),
    created_at                  timestamptz NOT NULL DEFAULT now(),
    updated_at                  timestamptz NOT NULL DEFAULT now()
);

CREATE TRIGGER jobs_updated_at BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE INDEX jobs_updated_at_idx ON jobs (updated_at);
CREATE INDEX jobs_vehicle_idx    ON jobs (vehicle_id);

-- --------------------------------------------------------------------------------------
-- job_events -- append-only transition log
-- --------------------------------------------------------------------------------------
--
-- `jobs.status` holds the current state; this table holds how it got there. Keeping both is
-- redundant on purpose: the application reads the column, and the warehouse reads the log.
-- Without the log you cannot answer "how long did this job spend assigned but not collected",
-- because the intermediate states have already been overwritten.

CREATE TABLE job_events (
    job_event_id  bigserial PRIMARY KEY,
    job_id        bigint      NOT NULL REFERENCES jobs (job_id),
    from_status   text,                        -- null for the job's first event
    to_status     text        NOT NULL,
    occurred_at   timestamptz NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX job_events_job_idx        ON job_events (job_id, occurred_at);
CREATE INDEX job_events_created_at_idx ON job_events (created_at);

-- --------------------------------------------------------------------------------------
-- pings -- the high-volume table
-- --------------------------------------------------------------------------------------
--
-- Around 8,600 rows per vehicle per 12-hour day at one ping every five seconds. This is the
-- only table here big enough to make storage and indexing decisions matter.
--
-- THREE IDENTIFIERS, EACH DOING A DIFFERENT JOB:
--
--   ping_id      client-generated UUID, and the primary key. A device that retries a failed
--                upload resends the SAME ping_id, so `ON CONFLICT (ping_id) DO NOTHING` makes
--                ingestion exactly idempotent instead of approximately.
--
--   sequence_no  monotonic per vehicle. NOT redundant with the timestamps: a GAP in the
--                sequence proves pings were lost in transit, whereas an unchanging position
--                with no gap proves the vehicle stopped. Without it, "no data" and "no
--                activity" are indistinguishable, and every traffic number silently
--                understates reality whenever a device drops out.
--
--   vehicle_id   which vehicle. Not unique on its own, obviously.
--
-- Deliberately NOT a unique constraint on (vehicle_id, sequence_no). The application should
-- accept anomalous sequences and let the warehouse detect them; rejecting them at the door
-- would destroy the evidence that a device is misbehaving.
--
-- TWO TIMESTAMPS, ONE OF WHICH LIES:
--
--   device_ts    when the device says it took the reading. NOT TRUSTWORTHY. Phone clocks
--                drift, and a device that has been offline may be badly wrong.
--   server_ts    when the API received it. Trustworthy -- one clock, ours.
--
-- server_ts - device_ts is the observed lateness, and it conflates transmission delay with
-- clock skew. Separating those two is phase 3 work. Measuring the distribution is phase 1 work.

CREATE TABLE pings (
    ping_id      uuid PRIMARY KEY,
    vehicle_id   integer NOT NULL REFERENCES vehicles (vehicle_id),
    sequence_no  bigint  NOT NULL,
    device_ts    timestamptz NOT NULL,
    server_ts    timestamptz NOT NULL DEFAULT now(),
    latitude     double precision NOT NULL,
    longitude    double precision NOT NULL,
    speed_kmh    double precision,
    heading_deg  double precision
);

-- Supports the batch poller in phase 2 (scan by arrival order) and the sequence-gap analysis.
CREATE INDEX pings_server_ts_idx ON pings (server_ts);
CREATE INDEX pings_vehicle_seq_idx ON pings (vehicle_id, sequence_no);
CREATE INDEX pings_vehicle_device_ts_idx ON pings (vehicle_id, device_ts);
