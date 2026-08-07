"""The fleet application: the source system this whole platform consumes.

Seven endpoints. It is deliberately small -- it exists to produce realistically-shaped events,
not to be a good application. No user accounts, no sessions, no UI. If this file grows past a
few hundred lines, scope has leaked.

Run it:

    uvicorn app.main:app --reload --port 8000

Then http://127.0.0.1:8000/docs for the generated API browser.

--------------------------------------------------------------------------------------------
THE SIMULATION AFFORDANCE, AND WHY IT IS GUARDED
--------------------------------------------------------------------------------------------
`server_ts` is meant to be OUR clock -- the one timestamp nobody else can influence. That is
the entire reason it can be trusted while `device_ts` cannot.

But generating a month of history in a few minutes means writing rows whose server_ts is in
the past, which a correct implementation would never allow. So there is an override, and it is
off unless `FLEET_ALLOW_SERVER_TS_OVERRIDE=true`.

This is a real pattern, not a hack: production systems do have privileged backfill paths. The
rule is that they are explicit, disabled by default, and impossible to trigger by accident.
A silent override would quietly destroy the only trustworthy timestamp in the system.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import Depends, FastAPI, Header, HTTPException, status

from app import db
from app.models import (
    JOB_TRANSITIONS,
    DriverUpdate,
    Health,
    Job,
    JobCreate,
    JobStatusUpdate,
    PingBatch,
    PingBatchResult,
    VehicleUpdate,
)
from fleet_telemetry import config


def _allow_server_ts_override() -> bool:
    return os.environ.get("FLEET_ALLOW_SERVER_TS_OVERRIDE", "").lower() == "true"


@asynccontextmanager
async def lifespan(_: FastAPI):
    db.open_pool()
    yield
    db.close_pool()


app = FastAPI(
    title="Fleet API",
    description="Source system for the telemetry platform. Scaffolding, not a product.",
    version="0.1.0",
    lifespan=lifespan,
)


# --------------------------------------------------------------------------------------
# authentication
# --------------------------------------------------------------------------------------


def require_token(authorization: str = Header(default="")) -> None:
    """A single static bearer token.

    This is NOT security. It exists so the simulator behaves like a client -- going through
    the API rather than writing to the database behind the application's back. That matters
    because phase 2's change data capture reads what the *application* committed, and a
    simulator that bypassed the app would be testing a pipeline that does not exist.
    """
    expected = f"Bearer {config.api_token()}"
    if authorization != expected:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or missing bearer token")


# --------------------------------------------------------------------------------------
# health
# --------------------------------------------------------------------------------------


@app.get("/health", response_model=Health)
def health() -> Health:
    """Liveness plus row counts. Unauthenticated so it can be used as a container healthcheck.

    The counts make this useful during phase 1: it is the quickest way to see whether the
    simulator is actually writing anything.
    """
    try:
        with db.connection() as conn, conn.cursor() as cur:
            counts = {}
            for table in ("depots", "drivers", "vehicles", "jobs", "job_events", "pings"):
                cur.execute(f"select count(*) as n from {table}")
                counts[table] = cur.fetchone()["n"]
        return Health(status="ok", database="reachable", counts=counts)
    except Exception as exc:
        return Health(status="degraded", database=f"unreachable: {type(exc).__name__}", counts={})


# --------------------------------------------------------------------------------------
# pings -- the high-volume path
# --------------------------------------------------------------------------------------


@app.post(
    "/pings",
    response_model=PingBatchResult,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_token)],
)
def post_pings(batch: PingBatch) -> PingBatchResult:
    """Accept a batch of GPS readings.

    Three things worth understanding here.

    **202, not 201.** "Accepted" is honest: the rows are stored, but nothing downstream has
    processed them. A 201 would imply the reading is fully handled, which it is not.

    **ON CONFLICT DO NOTHING on ping_id.** This is what makes the endpoint idempotent. A device
    whose upload times out will retry with the same ping_id; without this, the retry would
    double-count the vehicle's activity. `duplicates` is reported rather than hidden, because
    a duplicate rate is a useful signal about network conditions.

    **executemany, one transaction.** 5,000 pings in one round trip and one commit. Inserting
    them one at a time would be roughly two orders of magnitude slower, and a partial failure
    would leave the batch half-written.
    """
    server_ts = None
    if batch.server_ts_override is not None:
        if not _allow_server_ts_override():
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                "server_ts_override requires FLEET_ALLOW_SERVER_TS_OVERRIDE=true; it exists "
                "only for generating historical data",
            )
        server_ts = batch.server_ts_override

    rows = [
        (
            p.ping_id,
            p.vehicle_id,
            p.sequence_no,
            p.device_ts,
            server_ts or datetime.now(UTC),
            p.latitude,
            p.longitude,
            p.speed_kmh,
            p.heading_deg,
        )
        for p in batch.pings
    ]

    with db.connection() as conn, conn.cursor() as cur:
        cur.executemany(
            """
            insert into pings (ping_id, vehicle_id, sequence_no, device_ts, server_ts,
                               latitude, longitude, speed_kmh, heading_deg)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict (ping_id) do nothing
            """,
            rows,
        )
        inserted = cur.rowcount

    return PingBatchResult(received=len(rows), inserted=inserted, duplicates=len(rows) - inserted)


# --------------------------------------------------------------------------------------
# jobs -- the state machine
# --------------------------------------------------------------------------------------


@app.post(
    "/jobs",
    response_model=Job,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_token)],
)
def create_job(payload: JobCreate) -> Job:
    """Create a job and record its first transition.

    The insert and the job_events row happen in ONE transaction. If they did not, a crash
    between them would leave a job with no history, and the warehouse would later be unable to
    say when it was created -- an inconsistency impossible to repair after the fact.
    """
    requested_at = payload.requested_at or datetime.now(UTC)
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into jobs (pickup_depot_id, dropoff_depot_id, estimated_duration_minutes,
                              requested_at)
            values (%s, %s, %s, %s)
            returning job_id, vehicle_id, pickup_depot_id, dropoff_depot_id, status,
                      estimated_duration_minutes, requested_at
            """,
            (
                payload.pickup_depot_id,
                payload.dropoff_depot_id,
                payload.estimated_duration_minutes,
                requested_at,
            ),
        )
        job = cur.fetchone()
        cur.execute(
            "insert into job_events (job_id, from_status, to_status, occurred_at) "
            "values (%s, null, 'created', %s)",
            (job["job_id"], requested_at),
        )
    return Job(**job)


@app.patch("/jobs/{job_id}/status", response_model=Job, dependencies=[Depends(require_token)])
def update_job_status(job_id: int, payload: JobStatusUpdate) -> Job:
    """Move a job to a new status, appending to its transition log.

    `SELECT ... FOR UPDATE` locks the row for the transaction. Without it, two concurrent
    requests could both read status='assigned' and both write a transition away from it,
    producing a job with two conflicting histories. The simulator runs concurrent workers, so
    this is a real race rather than a theoretical one.
    """
    occurred_at = payload.occurred_at or datetime.now(UTC)
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute("select * from jobs where job_id = %s for update", (job_id,))
        job = cur.fetchone()
        if job is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"no job {job_id}")

        current = job["status"]
        allowed = JOB_TRANSITIONS.get(current, set())
        if payload.to_status not in allowed:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"cannot move a job from '{current}' to '{payload.to_status}'; "
                f"allowed: {sorted(allowed) or 'none (terminal state)'}",
            )
        if payload.to_status == "assigned" and payload.vehicle_id is None:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT,
                "vehicle_id is required when assigning a job",
            )

        cur.execute(
            """
            update jobs
               set status = %s,
                   vehicle_id = coalesce(%s, vehicle_id)
             where job_id = %s
            returning job_id, vehicle_id, pickup_depot_id, dropoff_depot_id, status,
                      estimated_duration_minutes, requested_at
            """,
            (payload.to_status, payload.vehicle_id, job_id),
        )
        updated = cur.fetchone()
        cur.execute(
            "insert into job_events (job_id, from_status, to_status, occurred_at) "
            "values (%s, %s, %s, %s)",
            (job_id, current, payload.to_status, occurred_at),
        )
    return Job(**updated)


# --------------------------------------------------------------------------------------
# mutable entities -- the raw material for SCD Type 2
# --------------------------------------------------------------------------------------


def _patch(table: str, key: str, key_value: int, changes: dict) -> dict:
    """Apply a partial update, returning the new row.

    Shared by drivers and vehicles because the logic is identical and duplicating it would
    mean two places to get the `updated_at` behaviour wrong. Column names are interpolated,
    which is safe ONLY because they come from Pydantic model fields, never from user input --
    values always go through parameters.
    """
    supplied = {k: v for k, v in changes.items() if v is not None}
    if not supplied:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "no fields to update")

    assignments = ", ".join(f"{col} = %s" for col in supplied)
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"update {table} set {assignments} where {key} = %s returning *",
            (*supplied.values(), key_value),
        )
        row = cur.fetchone()
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"no {table[:-1]} {key_value}")
    return row


@app.patch("/drivers/{driver_id}", dependencies=[Depends(require_token)])
def update_driver(driver_id: int, payload: DriverUpdate) -> dict:
    """Update a driver. The previous values are overwritten and lost -- that is the point.

    An application only cares about the current state. "Which depot was this driver attached
    to last March" is a question only the warehouse can answer, and only if the change stream
    was captured. That is what phase 3 builds.
    """
    return _patch("drivers", "driver_id", driver_id, payload.model_dump())


@app.patch("/vehicles/{vehicle_id}", dependencies=[Depends(require_token)])
def update_vehicle(vehicle_id: int, payload: VehicleUpdate) -> dict:
    return _patch("vehicles", "vehicle_id", vehicle_id, payload.model_dump())


@app.delete(
    "/vehicles/{vehicle_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_token)],
)
def delete_vehicle(vehicle_id: int) -> None:
    """Hard-delete a vehicle.

    A real fleet system would almost certainly soft-delete this -- set status='retired' and
    keep the row, because deleting anything with history attached is usually a mistake.

    It is a HARD delete here on purpose, and the purpose is pedagogical: phase 2 begins by
    building a batch poller that finds changes with `WHERE updated_at > watermark`. A deleted
    row has no updated_at. It has nothing at all. The poller cannot see it, will never see it,
    and no amount of tuning fixes that -- which is the cleanest possible demonstration of why
    change data capture exists. This endpoint is how you make that happen on demand.
    """
    with db.connection() as conn, conn.cursor() as cur:
        # Pings reference the vehicle, so clear them first rather than letting the foreign key
        # reject the delete. Realistic enough: retiring a vehicle discards its telemetry.
        cur.execute("delete from pings where vehicle_id = %s", (vehicle_id,))
        cur.execute("delete from jobs where vehicle_id = %s", (vehicle_id,))
        cur.execute("delete from vehicles where vehicle_id = %s", (vehicle_id,))
        if cur.rowcount == 0:
            raise HTTPException(status.HTTP_404_NOT_FOUND, f"no vehicle {vehicle_id}")
