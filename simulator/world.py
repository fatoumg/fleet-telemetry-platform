"""The world the simulator moves vehicles around in.

Deliberately simple physics. Vehicles travel in straight lines between depots at a constant
speed, and positions are interpolated linearly in latitude/longitude.

That is not how the Earth works, and it is worth being clear about rather than quietly wrong:
lines of longitude converge towards the poles, so a degree of longitude is shorter than a
degree of latitude everywhere except the equator. Interpolating linearly therefore bends the
path slightly. Over The Gambia -- about 350 km end to end, at 13 degrees north -- the error is
small enough not to matter for counting vehicles in a cell, and the alternative (proper great
circle interpolation) would add trigonometry that teaches nothing about data engineering.

Distances and bearings DO use the real spherical formulas, because speed and heading are
values the pipeline will later compute from position deltas and check against. Those need to
be right, or a downstream disagreement would be the simulator's fault rather than a finding.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta

EARTH_RADIUS_KM = 6371.0

# A minibus on the south-bank road. Fast enough to cross a depot pair in a plausible time,
# slow enough that a five-second ping interval produces visible movement between readings.
CRUISE_SPEED_KMH = 55.0

# How long a vehicle sits at a depot before setting off again.
DWELL_MINUTES = (4, 20)


@dataclass(frozen=True)
class Depot:
    depot_id: int
    name: str
    latitude: float
    longitude: float


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial compass bearing from point 1 to point 2, degrees clockwise from north."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


@dataclass
class Vehicle:
    """One vehicle's position in the world, and its progress along the current leg.

    `sequence_no` is per vehicle and only ever increases. In phase 1 nothing ever skips it, so
    every sequence is complete -- that is the clean baseline against which phase 3's injected
    gaps will be visible.
    """

    vehicle_id: int
    origin: Depot
    destination: Depot
    departed_at: datetime
    sequence_no: int = 0
    # Set when the vehicle is idling at a depot between legs.
    idle_until: datetime | None = None
    job_id: int | None = None
    job_status: str | None = None
    leg_km: float = field(init=False)
    leg_seconds: float = field(init=False)

    def __post_init__(self) -> None:
        self._recompute_leg()

    def _recompute_leg(self) -> None:
        self.leg_km = haversine_km(
            self.origin.latitude,
            self.origin.longitude,
            self.destination.latitude,
            self.destination.longitude,
        )
        self.leg_seconds = max(self.leg_km / CRUISE_SPEED_KMH * 3600.0, 1.0)

    def progress(self, now: datetime) -> float:
        """Fraction of the current leg completed, clamped to [0, 1]."""
        elapsed = (now - self.departed_at).total_seconds()
        return min(max(elapsed / self.leg_seconds, 0.0), 1.0)

    def position(self, now: datetime) -> tuple[float, float]:
        f = self.progress(now)
        lat = self.origin.latitude + (self.destination.latitude - self.origin.latitude) * f
        lon = self.origin.longitude + (self.destination.longitude - self.origin.longitude) * f
        return lat, lon

    def heading(self) -> float:
        return bearing_deg(
            self.origin.latitude,
            self.origin.longitude,
            self.destination.latitude,
            self.destination.longitude,
        )

    def has_arrived(self, now: datetime) -> bool:
        return self.progress(now) >= 1.0

    def start_new_leg(self, destination: Depot, now: datetime) -> None:
        self.origin = self.destination
        self.destination = destination
        self.departed_at = now
        self.idle_until = None
        self.job_id = None
        self.job_status = None
        self._recompute_leg()


class World:
    """Vehicles, depots, and the rules for moving between them.

    Everything random goes through `self.rng`, which is seeded. Two runs with the same seed
    produce byte-identical output, which is what makes pipeline tests deterministic -- you can
    assert an exact row count, not a range.
    """

    def __init__(self, depots: list[Depot], vehicle_ids: list[int], start: datetime, seed: int):
        if len(depots) < 2:
            raise ValueError("need at least two depots to have anywhere to drive")
        self.depots = depots
        self.rng = random.Random(seed)
        self.vehicles: list[Vehicle] = []
        for vehicle_id in vehicle_ids:
            origin = self.rng.choice(depots)
            destination = self._other_depot(origin)
            self.vehicles.append(
                Vehicle(
                    vehicle_id=vehicle_id,
                    origin=origin,
                    destination=destination,
                    # Stagger departures so vehicles are not all in lockstep, which would make
                    # every aggregate suspiciously periodic.
                    departed_at=start - timedelta(seconds=self.rng.randint(0, 1800)),
                )
            )

    def _other_depot(self, not_this: Depot) -> Depot:
        return self.rng.choice([d for d in self.depots if d.depot_id != not_this.depot_id])

    def advance(self, now: datetime) -> list[Vehicle]:
        """Move time forward, returning vehicles that just completed a leg.

        A completed leg is what triggers the job lifecycle in run.py, so this is the hook the
        caller uses rather than inspecting vehicle state directly.
        """
        arrived: list[Vehicle] = []
        for vehicle in self.vehicles:
            if vehicle.idle_until is not None:
                if now >= vehicle.idle_until:
                    vehicle.start_new_leg(self._other_depot(vehicle.destination), now)
                continue
            if vehicle.has_arrived(now):
                dwell = self.rng.randint(*DWELL_MINUTES)
                vehicle.idle_until = now + timedelta(minutes=dwell)
                arrived.append(vehicle)
        return arrived

    def is_moving(self, vehicle: Vehicle, now: datetime) -> bool:
        return vehicle.idle_until is None and not vehicle.has_arrived(now)

    def speed_kmh(self, vehicle: Vehicle, now: datetime) -> float:
        """Zero while idling. A stationary vehicle still reports -- that is the whole point.

        A parked vehicle that keeps pinging is how the pipeline learns to tell "stopped" from
        "disappeared". If idle vehicles went silent instead, the two would be identical in the
        data and the distinction could never be made.
        """
        return CRUISE_SPEED_KMH if self.is_moving(vehicle, now) else 0.0
