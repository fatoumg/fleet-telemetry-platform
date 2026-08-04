"""Guard the state-vector contract that docs/opensky-api-reference.md claims.

Runs against the captured sample in data/samples/, so it needs no network and burns no credits.
Regenerate the sample with ``python scripts/explore_opensky.py``. These assertions cover the wire
format only -- there is no analytics logic to test yet.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

SAMPLES_DIR = Path(__file__).resolve().parent.parent / "data" / "samples"
EXTENDED_SAMPLE = SAMPLES_DIR / "states_all_bbox.json"
PLAIN_SAMPLE = SAMPLES_DIR / "states_all_bbox_not_extended.json"
TRACK_SAMPLE = SAMPLES_DIR / "tracks_all_live.json"
FLIGHTS_SAMPLE = SAMPLES_DIR / "flights_all.json"
OWN_SAMPLE = SAMPLES_DIR / "states_own.json"

HEX_DIGITS = set("0123456789abcdef")

FLIGHT_KEYS = [
    "icao24",
    "firstSeen",
    "estDepartureAirport",
    "lastSeen",
    "estArrivalAirport",
    "callsign",
    "estDepartureAirportHorizDistance",
    "estDepartureAirportVertDistance",
    "estArrivalAirportHorizDistance",
    "estArrivalAirportVertDistance",
    "departureAirportCandidatesCount",
    "arrivalAirportCandidatesCount",
]


def _load(path: Path) -> dict:
    if not path.exists():
        pytest.skip(f"{path.name} missing; run scripts/explore_opensky.py to capture it")
    return json.loads(path.read_text(encoding="utf-8"))["response"]


@pytest.fixture(scope="module")
def states() -> list[list]:
    payload = _load(EXTENDED_SAMPLE)
    rows = payload["states"]
    assert rows, "sample contains no state vectors"
    return rows


def test_top_level_shape() -> None:
    payload = _load(EXTENDED_SAMPLE)
    assert set(payload) == {"time", "states"}
    assert isinstance(payload["time"], int), "snapshot time is an int here, unlike /tracks/all"


def test_extended_rows_have_eighteen_fields(states: list[list]) -> None:
    assert {len(row) for row in states} == {18}


def test_without_extended_rows_have_seventeen_fields() -> None:
    payload = _load(PLAIN_SAMPLE)
    assert {len(row) for row in payload["states"]} == {17}, (
        "extended=1 is what adds index 17 (category)"
    )


def test_icao24_is_lowercase_hex(states: list[list]) -> None:
    for row in states:
        icao24 = row[0]
        assert isinstance(icao24, str) and icao24
        assert set(icao24) <= HEX_DIGITS, f"{icao24!r} is not lowercase hex"


def test_booleans_are_real_bools(states: list[list]) -> None:
    for row in states:
        assert isinstance(row[8], bool), "on_ground"
        assert isinstance(row[15], bool), "spi"


def test_position_source_and_category_are_in_documented_ranges(states: list[list]) -> None:
    for row in states:
        assert row[16] in (0, 1, 2, 3), f"position_source {row[16]!r} outside 0-3"
        assert isinstance(row[17], int) and 0 <= row[17] <= 20, f"category {row[17]!r} outside 0-20"


def test_coordinates_are_null_or_in_range(states: list[list]) -> None:
    for row in states:
        longitude, latitude = row[5], row[6]
        assert longitude is None or -180.0 <= longitude <= 180.0
        assert latitude is None or -90.0 <= latitude <= 90.0


def test_nullable_fields_are_actually_nullable(states: list[list]) -> None:
    """The doc claims these are nullable; confirm that against real data, not the spec."""
    nullable = {
        7: "baro_altitude",
        11: "vertical_rate",
        12: "sensors",
        13: "geo_altitude",
        14: "squawk",
    }
    observed = {
        name: sum(1 for row in states if row[index] is None) for index, name in nullable.items()
    }
    assert any(observed.values()), f"expected nulls somewhere in {list(nullable.values())}"


def test_numeric_fields_mix_int_and_float(states: list[list]) -> None:
    """JSON numbers arrive as int OR float for the same field -- never assume float."""
    types = {
        type(row[index]).__name__
        for index in (7, 9, 10, 11)
        for row in states
        if row[index] is not None
    }
    assert types <= {"int", "float"}
    assert "float" in types


def test_callsigns_are_space_padded_or_empty(states: list[list]) -> None:
    """Three distinct 'no callsign' shapes exist: null, empty string, and 8-char padding."""
    present = [row[1] for row in states if row[1] is not None]
    assert present, "sample has no callsigns"
    assert any(c != c.strip() for c in present), "expected space-padded callsigns needing .strip()"
    # An empty string is a real, observed value -- distinct from null. So `.strip() or None`
    # is the correct normalisation, not `.strip()` alone.
    assert all(len(c) in (0, 8) for c in present), (
        f"unexpected callsign widths: {sorted({len(c) for c in present})}"
    )


def test_track_timestamps_are_floats_not_ints() -> None:
    """/tracks/all is the one endpoint returning epoch seconds as JSON floats."""
    payload = _load(TRACK_SAMPLE)
    assert set(payload) == {"icao24", "callsign", "startTime", "endTime", "path"}
    assert isinstance(payload["startTime"], float)
    assert isinstance(payload["endTime"], float)
    assert all(len(waypoint) == 6 for waypoint in payload["path"])
    assert all(isinstance(waypoint[0], int) for waypoint in payload["path"]), (
        "waypoint time is an int even though startTime/endTime are floats"
    )


# --- authenticated-only shapes. These skip cleanly on an anonymous capture. ---------------


def test_states_own_returns_null_not_empty_list() -> None:
    """A 200 does not imply iterable data: with no receiver, `states` is null, not []."""
    payload = _load(OWN_SAMPLE)
    if not isinstance(payload, dict):
        pytest.skip("states_own was captured anonymously (403, plain body)")
    assert payload["states"] is None, "expected states=null for an account with no receiver"
    assert payload["time"] == 0
    with pytest.raises(TypeError):
        # Documents precisely why `payload.get("states") or []` is the required idiom.
        len(payload["states"])  # type: ignore[arg-type]


def test_flights_are_objects_with_exactly_the_documented_keys() -> None:
    payload = _load(FLIGHTS_SAMPLE)
    if not isinstance(payload, list):
        pytest.skip("flights_all was captured anonymously (403, plain body)")
    assert payload, "sample has no flights"
    assert all(isinstance(flight, dict) for flight in payload), (
        "/flights/* returns objects, unlike /states and /tracks which return bare arrays"
    )
    observed = {key for flight in payload for key in flight}
    assert observed == set(FLIGHT_KEYS), f"unexpected key set: {sorted(observed)}"


def test_flight_airport_fields_are_null_together() -> None:
    """A null airport means its two distance fields are null too -- they travel as a group."""
    payload = _load(FLIGHTS_SAMPLE)
    if not isinstance(payload, list):
        pytest.skip("flights_all was captured anonymously (403, plain body)")
    for flight in payload:
        for prefix in ("estDeparture", "estArrival"):
            airport = flight[f"{prefix}Airport"]
            horiz = flight[f"{prefix}AirportHorizDistance"]
            vert = flight[f"{prefix}AirportVertDistance"]
            assert (airport is None) == (horiz is None) == (vert is None), (
                f"{prefix}: airport={airport!r} but distances={horiz!r}/{vert!r}"
            )


def test_flight_first_seen_can_be_null_despite_docs() -> None:
    """The docs type firstSeen/lastSeen as plain integers, but firstSeen is observed null.

    It happens when the departure was never seen (e.g. an inbound long-haul), so any naive
    ``lastSeen - firstSeen`` duration calculation raises TypeError on real data.
    """
    arrival = SAMPLES_DIR / "flights_arrival.json"
    payload = _load(arrival)
    if not isinstance(payload, list):
        pytest.skip("flights_arrival was captured anonymously (403, plain body)")
    types = {type(f["firstSeen"]).__name__ for f in payload}
    assert types <= {"int", "NoneType"}
    # Not asserting that a null is always present -- it depends on the window sampled. But when one
    # is present, it must coincide with an unresolved departure airport.
    for flight in payload:
        if flight["firstSeen"] is None:
            assert flight["estDepartureAirport"] is None, (
                "null firstSeen should mean the departure was never observed"
            )


def test_flight_airport_resolution_frequently_fails() -> None:
    """Roughly a third to a half of flights have no resolvable airport -- not an edge case."""
    payload = _load(FLIGHTS_SAMPLE)
    if not isinstance(payload, list):
        pytest.skip("flights_all was captured anonymously (403, plain body)")
    unresolved = sum(1 for f in payload if f["estArrivalAirport"] is None)
    assert unresolved > 0, "expected some unresolved arrival airports"
    assert unresolved < len(payload), "expected some resolved arrival airports"
