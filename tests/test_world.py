"""The simulator's world model.

Pure unit tests -- no database, no HTTP, no docker. They run in milliseconds and are the first
thing to check when generated data looks wrong, because if the geometry or the determinism is
broken then every downstream number is meaningless.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from simulator.world import Depot, World, bearing_deg, haversine_km

BANJUL = Depot(1, "Banjul", 13.4549, -16.5790)
BASSE = Depot(8, "Basse Santa Su", 13.3167, -14.2167)
START = datetime(2026, 8, 1, 6, 0, tzinfo=UTC)


# ------------------------------------------------------------------------------------
# geometry
# ------------------------------------------------------------------------------------


def test_distance_across_the_gambia_is_plausible():
    """Banjul to Basse is roughly 255 km as the crow flies.

    Pinned to a real-world figure rather than to whatever the function returns, so a broken
    formula fails instead of being blessed.
    """
    km = haversine_km(BANJUL.latitude, BANJUL.longitude, BASSE.latitude, BASSE.longitude)
    assert 240 < km < 270, f"expected ~255 km, got {km:.1f}"


def test_distance_is_symmetric_and_zero_to_itself():
    a = haversine_km(13.4, -16.5, 13.3, -14.2)
    b = haversine_km(13.3, -14.2, 13.4, -16.5)
    assert a == pytest.approx(b)
    assert haversine_km(13.4, -16.5, 13.4, -16.5) == pytest.approx(0.0)


def test_bearing_east_is_about_ninety_degrees():
    """Basse is almost due east of Banjul."""
    b = bearing_deg(BANJUL.latitude, BANJUL.longitude, BASSE.latitude, BASSE.longitude)
    assert 85 < b < 100, f"expected roughly east, got {b:.1f} degrees"


def test_bearing_is_always_in_range():
    for lat, lon in [(13.0, -17.0), (13.9, -13.7), (13.4, -16.6)]:
        b = bearing_deg(13.45, -16.58, lat, lon)
        assert 0.0 <= b < 360.0


# ------------------------------------------------------------------------------------
# movement
# ------------------------------------------------------------------------------------


def test_vehicle_starts_at_origin_and_ends_at_destination():
    world = World([BANJUL, BASSE], [1], START, seed=1)
    v = world.vehicles[0]
    v.origin, v.destination, v.departed_at = BANJUL, BASSE, START
    v._recompute_leg()

    lat, lon = v.position(START)
    assert (lat, lon) == pytest.approx((BANJUL.latitude, BANJUL.longitude))

    lat, lon = v.position(START + timedelta(seconds=v.leg_seconds))
    assert (lat, lon) == pytest.approx((BASSE.latitude, BASSE.longitude))


def test_progress_never_exceeds_one():
    """Clamped, so a vehicle left running past its arrival does not sail past the depot."""
    world = World([BANJUL, BASSE], [1], START, seed=1)
    v = world.vehicles[0]
    assert v.progress(START + timedelta(days=7)) == 1.0


def test_idle_vehicle_reports_zero_speed_but_keeps_pinging():
    """A stopped vehicle must still report.

    If idle vehicles went silent, "stopped" and "lost signal" would look identical in the
    data, and phase 3 could never tell them apart.
    """
    world = World([BANJUL, BASSE], [1], START, seed=1)
    v = world.vehicles[0]
    v.idle_until = START + timedelta(minutes=10)
    assert world.speed_kmh(v, START) == 0.0
    assert not world.is_moving(v, START)


def test_arrival_triggers_a_dwell_then_a_new_leg():
    world = World([BANJUL, BASSE], [1], START, seed=1)
    v = world.vehicles[0]
    v.origin, v.destination, v.departed_at = BANJUL, BASSE, START
    v._recompute_leg()

    arrived_at = START + timedelta(seconds=v.leg_seconds + 1)
    assert world.advance(arrived_at) == [v]
    assert v.idle_until is not None

    world.advance(v.idle_until + timedelta(seconds=1))
    assert v.idle_until is None
    assert v.origin == BASSE, "the new leg should start from where it arrived"


def test_a_vehicle_is_never_sent_to_where_it_already_is():
    world = World([BANJUL, BASSE, Depot(3, "Soma", 13.4, -15.53)], list(range(20)), START, seed=7)
    for v in world.vehicles:
        assert v.origin.depot_id != v.destination.depot_id


# ------------------------------------------------------------------------------------
# determinism -- what makes pipeline tests able to assert exact numbers
# ------------------------------------------------------------------------------------


def test_same_seed_produces_identical_worlds():
    def fingerprint(seed: int) -> list[tuple]:
        w = World([BANJUL, BASSE, Depot(3, "Soma", 13.4, -15.53)], [1, 2, 3], START, seed)
        return [
            (v.vehicle_id, v.origin.depot_id, v.destination.depot_id, v.departed_at)
            for v in w.vehicles
        ]

    assert fingerprint(42) == fingerprint(42)


def test_different_seeds_produce_different_worlds():
    def fingerprint(seed: int) -> list[tuple]:
        w = World([BANJUL, BASSE, Depot(3, "Soma", 13.4, -15.53)], [1, 2, 3], START, seed)
        return [(v.origin.depot_id, v.destination.depot_id) for v in w.vehicles]

    assert fingerprint(1) != fingerprint(999)


def test_world_needs_somewhere_to_drive():
    with pytest.raises(ValueError, match="at least two depots"):
        World([BANJUL], [1], START, seed=1)
