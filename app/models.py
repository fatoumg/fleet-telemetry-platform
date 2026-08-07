"""Request and response shapes.

Pydantic validates every incoming request against these before a handler runs, so the handlers
never see a missing field or a latitude of 900.

The interesting decisions here are about **what the API refuses to trust**. A device is not a
trusted client: its clock may be wrong, it may resend, and it may report nonsense. Deciding
which of those the API rejects and which it accepts-and-records is a data-modelling decision,
not a validation detail:

  * Rejected -- physically impossible values (latitude 900, negative speed). Storing them would
    corrupt every downstream aggregate and they cannot be corrected later.
  * Accepted and recorded -- a device timestamp that disagrees with ours, or a sequence number
    that skipped. These are *evidence about the device*, and throwing them away at the door
    destroys the only signal that something is wrong with it.

That distinction is why the warehouse gets to say "this vehicle's clock is four minutes fast"
instead of "this vehicle sent nothing for four minutes".
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field

# A job may only move along these edges. Enforced in the handler, not the database, because
# the app is the thing that understands the workflow -- and because an invalid transition is a
# client bug worth a 409, not a constraint violation worth a 500.
JOB_TRANSITIONS: dict[str, set[str]] = {
    "created": {"assigned", "cancelled"},
    "assigned": {"picked_up", "cancelled"},
    "picked_up": {"delivered", "cancelled"},
    "delivered": set(),
    "cancelled": set(),
}


class Ping(BaseModel):
    """One GPS reading from one vehicle."""

    ping_id: UUID = Field(
        description="Client-generated. A retry MUST reuse it -- that is what makes ingestion "
        "idempotent rather than approximately deduplicated."
    )
    vehicle_id: int
    sequence_no: int = Field(
        ge=0,
        description="Monotonic per vehicle. A gap proves loss; an unchanging position with no "
        "gap proves the vehicle stopped.",
    )
    device_ts: datetime = Field(
        description="The device's own clock. Recorded as given, never corrected here -- the "
        "disagreement with server_ts is the measurement phase 3 needs."
    )
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    speed_kmh: float | None = Field(default=None, ge=0, le=300)
    heading_deg: float | None = Field(default=None, ge=0, lt=360)


class PingBatch(BaseModel):
    """What a device uploads.

    Batched because that is how devices behave: they buffer while out of signal and flush on
    reconnect. Accepting batches is what makes the burst arrival in phase 3 realistic rather
    than simulated by artificially delaying single requests.
    """

    pings: list[Ping] = Field(min_length=1, max_length=5000)
    # Simulation affordance -- see the note in main.py. Ignored unless the server explicitly
    # allows it, which it does not by default.
    server_ts_override: datetime | None = None


class PingBatchResult(BaseModel):
    received: int
    inserted: int
    duplicates: int = Field(
        description="Rows already present by ping_id. A non-zero value is normal and healthy: "
        "it means retries happened and idempotency absorbed them."
    )


class JobCreate(BaseModel):
    pickup_depot_id: int
    dropoff_depot_id: int
    estimated_duration_minutes: int = Field(gt=0, le=600)
    requested_at: datetime | None = None


class JobStatusUpdate(BaseModel):
    to_status: str
    occurred_at: datetime | None = None
    vehicle_id: int | None = Field(default=None, description="Required when moving to 'assigned'.")


class Job(BaseModel):
    job_id: int
    vehicle_id: int | None
    pickup_depot_id: int
    dropoff_depot_id: int
    status: str
    estimated_duration_minutes: int
    requested_at: datetime


class DriverUpdate(BaseModel):
    """Partial update. Every field optional; only what is supplied changes.

    These mutations are the raw material for Slowly Changing Dimensions. The OLTP row simply
    overwrites, losing the old value -- reconstructing what it used to say is phase 3's job.
    """

    full_name: str | None = None
    phone: str | None = None
    status: str | None = Field(default=None, pattern="^(active|inactive|suspended)$")
    home_depot_id: int | None = None


class VehicleUpdate(BaseModel):
    plate: str | None = None
    capacity: int | None = Field(default=None, gt=0)
    status: str | None = Field(default=None, pattern="^(active|maintenance|retired)$")
    current_driver_id: int | None = None
    home_depot_id: int | None = None


class Health(BaseModel):
    status: str
    database: str
    counts: dict[str, int]
