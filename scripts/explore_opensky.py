"""Probe the OpenSky Network REST API and report what each endpoint actually returns.

Deliberately uses ``requests`` directly rather than the ``opensky_api`` wrapper: the wrapper
collapses rate-limiting and HTTP errors into a bare ``None`` (see ``_get_json``), which hides
exactly the information this script exists to surface. Only the documented field-name lists are
borrowed from the library, so they stay in sync if it is upgraded.

Anonymous access reaches /states/all and live /tracks/all; every historical endpoint answers 403.
Those 403s are probed on purpose so their shape is captured as evidence rather than assumed.

Usage:
    python scripts/explore_opensky.py            # bbox-scoped, ~7 credits of the 400/day anonymous
    python scripts/explore_opensky.py --global    # adds the unbounded global snapshot (+4 credits)
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from opensky_api import FlightData, StateVector, Waypoint

BASE_URL = "https://opensky-network.org/api"
TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLES_DIR = PROJECT_ROOT / "data" / "samples"
CREDENTIALS_PATH = PROJECT_ROOT / "credentials.json"
TIMEOUT = 30.0

# State resolution: one call per 10 s anonymously, 5 s authenticated.
STATES_MIN_INTERVAL_ANON = 10.0
STATES_MIN_INTERVAL_AUTH = 5.0
# Refuse to start a run that could exhaust the daily allowance.
MIN_CREDITS_TO_START = 40

POSITION_SOURCE = {0: "ADS-B", 1: "ASTERIX", 2: "MLAT", 3: "FLARM"}

AIRCRAFT_CATEGORY = {
    0: "No information at all",
    1: "No ADS-B Emitter Category Information",
    2: "Light (< 15500 lbs)",
    3: "Small (15500 to 75000 lbs)",
    4: "Large (75000 to 300000 lbs)",
    5: "High Vortex Large (e.g. B-757)",
    6: "Heavy (> 300000 lbs)",
    7: "High Performance (> 5g accel and 400 kts)",
    8: "Rotorcraft",
    9: "Glider / sailplane",
    10: "Lighter-than-air",
    11: "Parachutist / Skydiver",
    12: "Ultralight / hang-glider / paraglider",
    13: "Reserved",
    14: "Unmanned Aerial Vehicle",
    15: "Space / Trans-atmospheric vehicle",
    16: "Surface Vehicle - Emergency Vehicle",
    17: "Surface Vehicle - Service Vehicle",
    18: "Point Obstacle (incl. tethered balloons)",
    19: "Cluster Obstacle",
    20: "Line Obstacle",
}

# Switzerland and surrounding Alps: dense traffic, and at ~2 x 4.6 degrees it is inside the
# 25 sq-degree tier, so it costs a single credit.
BBOX_ALPS = {"lamin": 45.8, "lomin": 5.9, "lamax": 47.8, "lomax": 10.5}


# --------------------------------------------------------------------------------------
# authentication
# --------------------------------------------------------------------------------------


def authenticate(session: requests.Session) -> bool:
    """Exchange credentials.json for a bearer token. Returns False to fall back to anonymous.

    Basic auth was removed on 2026-03-18; OAuth2 client-credentials is the only scheme. Secrets are
    never printed -- only their presence and the resulting token lifetime.
    """
    if not CREDENTIALS_PATH.exists():
        print(f"no {CREDENTIALS_PATH.name} found -- running anonymously")
        return False

    try:
        creds = json.loads(CREDENTIALS_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"could not read {CREDENTIALS_PATH.name} ({exc}) -- running anonymously")
        return False

    client_id, client_secret = creds.get("clientId"), creds.get("clientSecret")
    if not client_id or not client_secret:
        print(f"{CREDENTIALS_PATH.name} lacks clientId/clientSecret -- running anonymously")
        return False

    try:
        response = session.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=TIMEOUT,
        )
    except requests.RequestException as exc:
        print(f"token request failed ({exc}) -- running anonymously")
        return False

    if response.status_code != 200:
        print(
            f"token request returned {response.status_code}: {response.text[:200]} "
            f"-- running anonymously"
        )
        return False

    payload = response.json()
    token = payload.get("access_token")
    if not token:
        print("token response had no access_token -- running anonymously")
        return False

    session.headers["Authorization"] = f"Bearer {token}"
    print(
        f"authenticated as client {client_id[:4]}... "
        f"(token type {payload.get('token_type')}, expires in {payload.get('expires_in')}s)"
    )
    return True


# --------------------------------------------------------------------------------------
# probing
# --------------------------------------------------------------------------------------


class Probe:
    """One endpoint call: what to request, what it should cost, and why we care."""

    def __init__(
        self,
        name: str,
        path: str,
        params: dict[str, Any],
        credits: int,
        rationale: str,
        expect: str,
    ) -> None:
        self.name = name
        self.path = path
        self.params = params
        self.credits = credits
        self.rationale = rationale
        self.expect = expect


def build_probes(include_global: bool, authenticated: bool) -> list[Probe]:
    # An interval comfortably inside the documented 2 h cap, on the previous UTC day -- which also
    # satisfies the "previous day or earlier" rule for the airport and per-aircraft endpoints. So a
    # 403 can only mean "not permitted", never "malformed request".
    day_start = 1785110400  # 2026-07-27 00:00:00Z
    expect_flights = "200 JSON array" if authenticated else "403 text/plain"
    probes = [
        Probe(
            "states_all_bbox",
            "/states/all",
            {**BBOX_ALPS, "extended": 1},
            1,
            "Primary anonymous endpoint. Bounded box keeps the cost at 1 credit.",
            "200 JSON",
        ),
        Probe(
            "states_all_bbox_not_extended",
            "/states/all",
            dict(BBOX_ALPS),
            1,
            "Same box without extended=1, to show the row loses its 18th field (category).",
            "200 JSON, 17-element rows",
        ),
        Probe(
            "states_own",
            "/states/own",
            {},
            0,
            "Needs a registered ADS-B receiver. Authenticated without one it returns states=null.",
            "200 with states=null" if authenticated else "403, empty body",
        ),
        Probe(
            "flights_all",
            "/flights/all",
            {"begin": day_start, "end": day_start + 3600},
            4,
            "Historical flights, 1 h window inside the documented 2 h cap.",
            expect_flights,
        ),
        Probe(
            "flights_aircraft",
            "/flights/aircraft",
            {"icao24": "3c6444", "begin": day_start, "end": day_start + 86400},
            4,
            "Per-aircraft flight list over the previous UTC day.",
            expect_flights,
        ),
        Probe(
            "flights_arrival",
            "/flights/arrival",
            {"airport": "EDDF", "begin": day_start, "end": day_start + 86400},
            4,
            "Arrivals at Frankfurt over the previous UTC day.",
            expect_flights,
        ),
        Probe(
            "flights_departure",
            "/flights/departure",
            {"airport": "EDDF", "begin": day_start, "end": day_start + 86400},
            4,
            "Departures from Frankfurt over the previous UTC day.",
            expect_flights,
        ),
    ]
    if authenticated:
        # Only meaningful with credentials: historical state vectors, which is what turns a single
        # snapshot into the time series conflict analysis actually needs.
        probes.append(
            Probe(
                "states_all_bbox_historical",
                "/states/all",
                {**BBOX_ALPS, "extended": 1, "time": int(time.time()) - 1800},
                1,
                "State vectors 30 min in the past -- refused anonymously, allowed up to 1 h authed.",
                "200 JSON with an older 'time'",
            )
        )
    if include_global:
        probes.insert(
            1,
            Probe(
                "states_all_global",
                "/states/all",
                {"extended": 1},
                4,
                "Unbounded global snapshot. Costs 4 credits, hence opt-in via --global.",
                "200 JSON, thousands of rows",
            ),
        )
    # /tracks/all is appended at runtime: its icao24 comes from the first live snapshot.
    return probes


def run_probe(session: requests.Session, probe: Probe) -> dict[str, Any]:
    """Issue one GET. Never raises for a bad status or an unparseable body."""
    url = f"{BASE_URL}{probe.path}"
    started = time.perf_counter()
    try:
        response = session.get(url, params=probe.params, timeout=TIMEOUT)
    except requests.RequestException as exc:
        return {
            "name": probe.name,
            "path": probe.path,
            "params": probe.params,
            "rationale": probe.rationale,
            "expected": probe.expect,
            "error": f"{type(exc).__name__}: {exc}",
            "status_code": None,
            "json": None,
        }

    elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
    content_type = response.headers.get("content-type", "")
    record: dict[str, Any] = {
        "name": probe.name,
        "path": probe.path,
        "params": probe.params,
        "rationale": probe.rationale,
        "expected": probe.expect,
        "url": response.url,
        "status_code": response.status_code,
        "content_type": content_type,
        "credits_remaining": response.headers.get("x-rate-limit-remaining"),
        "elapsed_ms": elapsed_ms,
        "bytes": len(response.content),
        "json": None,
        "text_body": None,
    }

    # Error bodies are text/plain, so .json() would raise. Branch on the declared type and
    # still guard the parse, because an empty 404 body advertises itself as JSON.
    if "json" in content_type:
        try:
            record["json"] = response.json()
        except ValueError as exc:
            record["json_parse_error"] = str(exc)
            record["text_body"] = response.text[:2000]
    else:
        record["text_body"] = response.text[:2000]

    return record


# --------------------------------------------------------------------------------------
# schema inference
# --------------------------------------------------------------------------------------


def _type_name(value: Any) -> str:
    return "null" if value is None else type(value).__name__


def describe_rows(rows: list[list[Any]], keys: list[str], title: str) -> None:
    """Print a per-field profile of an array-of-arrays payload, measured over every row."""
    print(f"\n  {title} -- {len(rows)} rows")
    if not rows:
        print("    (no rows returned; nothing to profile)")
        return

    lengths = Counter(len(row) for row in rows)
    print(f"    row lengths observed: {dict(lengths)} (documented key count: {len(keys)})")

    header = f"    {'idx':>3}  {'field':<22} {'types':<20} {'nulls':>12}  {'range':<28} examples"
    print(header)
    print("    " + "-" * (len(header) - 4))

    width = max(lengths)
    for idx in range(width):
        key = keys[idx] if idx < len(keys) else f"<undocumented {idx}>"
        values = [row[idx] for row in rows if len(row) > idx]
        types = sorted({_type_name(v) for v in values})
        present = [v for v in values if v is not None]
        null_count = len(values) - len(present)
        null_pct = (null_count / len(values) * 100) if values else 0.0
        nulls = f"{null_count} ({null_pct:.1f}%)"

        numeric = [v for v in present if isinstance(v, (int, float)) and not isinstance(v, bool)]
        if numeric:
            rng = f"{min(numeric):.4g} .. {max(numeric):.4g}"
        elif present and all(isinstance(v, bool) for v in present):
            true_count = sum(1 for v in present if v)
            rng = f"True={true_count} False={len(present) - true_count}"
        else:
            rng = f"{len({repr(v) for v in present})} distinct"

        examples = ", ".join(repr(v) for v in present[:3]) or "(all null)"
        print(
            f"    {idx:>3}  {key:<22} {'|'.join(types):<20} {nulls:>12}  {rng:<28} {examples[:60]}"
        )


def describe_enum(rows: list[list[Any]], index: int, name: str, mapping: dict[int, str]) -> None:
    values = [row[index] for row in rows if len(row) > index]
    counts = Counter(values)
    print(f"\n  {name} distribution (index {index}):")
    for value, count in counts.most_common():
        label = mapping.get(value, "<undocumented value>") if value is not None else "null"
        print(f"    {value!s:>5} = {label:<45} {count:>6} rows")


def describe_state_vectors(
    payload: dict[str, Any], label: str, requested_time: int | None = None
) -> None:
    rows = payload.get("states") or []
    snapshot = payload.get("time")
    print(f"\n{'=' * 100}")
    print(f"SCHEMA: {label}")
    print(f"{'=' * 100}")
    print(f"  top level keys: {sorted(payload)}")
    print(f"  time: {snapshot!r} ({_type_name(snapshot)}) -> {_fmt_epoch(snapshot)}")
    if requested_time:
        print(
            f"  requested time: {requested_time} -> served {snapshot} "
            f"(offset {(snapshot or 0) - requested_time:+d}s; the API snaps to its nearest sample)"
        )

    describe_rows(rows, StateVector.keys, "state vector fields")
    if rows and len(rows[0]) > 16:
        describe_enum(rows, 16, "position_source", POSITION_SOURCE)
    if rows and len(rows[0]) > 17:
        describe_enum(rows, 17, "category", AIRCRAFT_CATEGORY)
    else:
        print("\n  category: absent -- rows are 17 wide, so extended=1 was not sent")

    _report_callsign_padding(rows)
    _report_timestamp_skew(rows, snapshot)
    _report_altitude_disagreement(rows)
    _report_dataframe(rows)


def _report_altitude_disagreement(rows: list[list[Any]]) -> None:
    """geo_altitude is not a drop-in substitute for a missing baro_altitude -- quantify the gap."""
    deltas = sorted(
        row[13] - row[7]
        for row in rows
        if len(row) > 13 and row[7] is not None and row[13] is not None
    )
    if not deltas:
        return
    print(
        f"\n  geo_altitude - baro_altitude over {len(deltas)} aircraft reporting both: "
        f"min={deltas[0]:.0f}m median={deltas[len(deltas) // 2]:.0f}m max={deltas[-1]:.0f}m"
    )
    print(
        "    -> the two altitudes disagree by hundreds of metres; separation minima are defined on"
    )
    print("       barometric altitude, so geo_altitude cannot substitute for a missing baro value")

    only_geo = sum(1 for row in rows if len(row) > 13 and row[7] is None and row[13] is not None)
    both_null = sum(1 for row in rows if len(row) > 13 and row[7] is None and row[13] is None)
    print(
        f"    aircraft with geo but no baro: {only_geo}; with neither altitude: {both_null} "
        f"(the latter cannot be tested for vertical separation at all)"
    )


def _report_callsign_padding(rows: list[list[Any]]) -> None:
    """Callsigns arrive space-padded to 8 chars; prove it rather than asserting it."""
    callsigns = [row[1] for row in rows if len(row) > 1]
    present = [c for c in callsigns if c is not None]
    padded = [c for c in present if c != c.strip()]
    blank = [c for c in present if not c.strip()]
    print(
        f"\n  callsign padding: {len(padded)}/{len(present)} non-null callsigns carry "
        f"surrounding whitespace"
    )
    for c in padded[:3]:
        print(f"    {c!r} -> stripped {c.strip()!r} (raw len {len(c)})")
    print(f"    widths observed: {dict(Counter(len(c) for c in present))}")
    print(
        f"    null: {len(callsigns) - len(present)}, blank-but-not-null: {len(blank)} "
        f"-> three distinct 'no callsign' shapes; normalise with `.strip() or None`"
    )


def _report_timestamp_skew(rows: list[list[Any]], snapshot: int | None) -> None:
    """A snapshot is not time-synchronised: each aircraft has its own position timestamp.

    Reported airborne-only as well as overall, because the extreme stale readings turn out to be
    parked aircraft (velocity 0), which are harmless. The airborne figure is the one that matters:
    it bounds how far an aircraft could have moved since its position was last reported.
    """
    if not rows or snapshot is None:
        return
    aged = [(snapshot - row[3], row) for row in rows if len(row) > 3 and row[3] is not None]
    if not aged:
        return

    def summarise(items: list[tuple[int, list[Any]]], label: str) -> None:
        ages = sorted(age for age, _ in items)
        print(
            f"    {label:<22} n={len(ages):<5} min={ages[0]}s "
            f"median={ages[len(ages) // 2]}s max={ages[-1]}s"
        )

    print("\n  time_position staleness vs the envelope 'time' field:")
    summarise(aged, "all aircraft")
    airborne = [(age, row) for age, row in aged if len(row) > 8 and not row[8]]
    if airborne:
        summarise(airborne, "airborne only")
        stale = sum(1 for age, _ in airborne if age > 60)
        print(f"    airborne with age > 60s: {stale} of {len(airborne)}")
        # Bound the positional error using each aircraft's own reported ground speed.
        errors = [
            (age * row[9] / 1000.0, age, row[0])
            for age, row in airborne
            if len(row) > 9 and row[9] is not None
        ]
        if errors:
            worst_km, worst_age, worst_icao = max(errors)
            print(
                f"    worst-case position error: {worst_km:.1f} km "
                f"({worst_icao}, {worst_age}s stale at its reported ground speed)"
            )
    print("    -> state vectors are NOT a synchronised snapshot; positions must be time-aligned")


def _report_dataframe(rows: list[list[Any]]) -> None:
    """Show the pandas view, since that is what downstream analysis will consume."""
    try:
        import pandas as pd
    except ImportError:
        print("\n  (pandas not installed; skipping DataFrame view)")
        return
    if not rows:
        return

    width = max(len(row) for row in rows)
    columns = StateVector.keys[:width]
    frame = pd.DataFrame([row[:width] for row in rows], columns=columns)
    print("\n  pandas dtypes (note the object columns where nulls forced coercion):")
    for column, dtype in frame.dtypes.items():
        print(f"    {column:<22} {dtype}")

    numeric_cols = [
        c
        for c in (
            "longitude",
            "latitude",
            "baro_altitude",
            "geo_altitude",
            "velocity",
            "true_track",
            "vertical_rate",
        )
        if c in frame
    ]
    if numeric_cols:
        summary = frame[numeric_cols].apply(pd.to_numeric, errors="coerce").describe()
        print("\n  numeric summary (values are metric: metres, m/s, degrees):")
        print("\n".join("    " + line for line in summary.to_string().splitlines()))


def describe_track(payload: dict[str, Any], label: str) -> None:
    print(f"\n{'=' * 100}")
    print(f"SCHEMA: {label}")
    print(f"{'=' * 100}")
    print(f"  top level keys: {sorted(payload)}")
    for key in ("icao24", "callsign", "startTime", "endTime"):
        value = payload.get(key)
        note = ""
        if key in ("startTime", "endTime") and isinstance(value, float):
            note = "  <-- float, not int: JSON scientific notation, needs int() before use"
        print(f"    {key:<12} {value!r} ({_type_name(value)}){note}")

    describe_rows(payload.get("path") or [], Waypoint.keys, "waypoint fields")

    path = payload.get("path") or []
    if len(path) > 1:
        gaps = [path[i + 1][0] - path[i][0] for i in range(len(path) - 1)]
        print(
            f"\n  waypoint spacing: min={min(gaps)}s max={max(gaps)}s "
            f"mean={sum(gaps) / len(gaps):.1f}s -- sampling is irregular"
        )


def describe_flights(payload: list[dict[str, Any]], label: str) -> None:
    """Profile the /flights/* response, which -- unlike states and tracks -- is objects, not arrays."""
    print(f"\n{'=' * 100}")
    print(f"SCHEMA: {label}")
    print(f"{'=' * 100}")
    print(f"  {len(payload)} flight objects (a JSON array of OBJECTS, not bare arrays)")
    if not payload:
        print("    (empty array -- no flights in the window)")
        return

    observed_keys = sorted({key for flight in payload for key in flight})
    documented = list(FlightData.keys)
    print(f"  keys observed: {len(observed_keys)}; documented: {len(documented)}")
    extra = [k for k in observed_keys if k not in documented]
    missing = [k for k in documented if k not in observed_keys]
    if extra:
        print(f"  UNDOCUMENTED keys present: {extra}")
    if missing:
        print(f"  documented keys absent from the response: {missing}")

    header = f"    {'field':<36} {'types':<20} {'nulls':>12}  {'range':<26} example"
    print(f"\n{header}")
    print("    " + "-" * (len(header) - 4))
    for key in documented + extra:
        values = [flight.get(key) for flight in payload]
        present = [v for v in values if v is not None]
        types = sorted({_type_name(v) for v in values})
        null_pct = (len(values) - len(present)) / len(values) * 100
        nulls = f"{len(values) - len(present)} ({null_pct:.1f}%)"
        numeric = [v for v in present if isinstance(v, (int, float)) and not isinstance(v, bool)]
        rng = (
            f"{min(numeric):.6g} .. {max(numeric):.6g}"
            if numeric
            else f"{len({repr(v) for v in present})} distinct"
        )
        example = repr(present[0]) if present else "(all null)"
        print(f"    {key:<36} {'|'.join(types):<20} {nulls:>12}  {rng:<26} {example[:30]}")

    print("\n  first flight object, verbatim:")
    for line in json.dumps(payload[0], indent=2).splitlines():
        print(f"    {line}")

    airports = Counter(f.get("estArrivalAirport") for f in payload if f.get("estArrivalAirport"))
    unresolved = sum(1 for f in payload if not f.get("estArrivalAirport"))
    print(
        f"\n  estArrivalAirport: {len(airports)} distinct resolved, {unresolved} unresolved "
        f"(null) of {len(payload)}"
    )


def _fmt_epoch(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "n/a"
    return datetime.fromtimestamp(int(value), tz=UTC).isoformat()


# --------------------------------------------------------------------------------------
# orchestration
# --------------------------------------------------------------------------------------


def pick_live_icao24(payload: dict[str, Any]) -> str | None:
    """Choose an airborne aircraft with a real position, so /tracks/all returns something."""
    for row in payload.get("states") or []:
        icao24, on_ground, longitude, latitude = row[0], row[8], row[5], row[6]
        if icao24 and not on_ground and longitude is not None and latitude is not None:
            return icao24
    return None


def check_starting_credits(session: requests.Session) -> int | None:
    """Cheapest possible call, purely to read the credit header before committing to a run."""
    try:
        response = session.get(
            f"{BASE_URL}/states/all",
            params={"lamin": 47.0, "lomin": 8.0, "lamax": 47.5, "lomax": 8.5},
            timeout=TIMEOUT,
        )
    except requests.RequestException as exc:
        print(f"Could not reach the API to check credits: {exc}")
        return None
    raw = response.headers.get("x-rate-limit-remaining")
    return int(raw) if raw and raw.lstrip("-").isdigit() else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--global",
        dest="include_global",
        action="store_true",
        help="also fetch the unbounded global snapshot (costs 4 credits instead of 1)",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="print schemas but do not write data/samples/",
    )
    parser.add_argument(
        "--anonymous",
        action="store_true",
        help="ignore credentials.json and probe anonymously, to capture the unauthenticated shapes",
    )
    args = parser.parse_args(argv)

    session = requests.Session()
    session.headers["User-Agent"] = "aviation-conflict-analytics/explore"

    print("OpenSky API exploration")
    print(f"base url: {BASE_URL}")
    print(f"started:  {_fmt_epoch(time.time())}")

    if args.anonymous:
        print("--anonymous given: skipping credentials.json")
        authenticated = False
    else:
        authenticated = authenticate(session)
    print(f"mode: {'AUTHENTICATED' if authenticated else 'anonymous'}")

    starting_credits = check_starting_credits(session)
    if starting_credits is None:
        print("\nWarning: no X-Rate-Limit-Remaining header; proceeding without a credit guard.")
    else:
        print(f"credits remaining before run: {starting_credits}")
        if starting_credits < MIN_CREDITS_TO_START:
            print(
                f"\nAborting: only {starting_credits} credits left, below the "
                f"{MIN_CREDITS_TO_START} floor. The allowance resets daily."
            )
            return 1

    probes = build_probes(args.include_global, authenticated)
    planned = sum(p.credits for p in probes) + 1  # +1 for the /tracks/all probe
    print(f"planned probes: {len(probes) + 1}, estimated cost: ~{planned} credits\n")

    interval = STATES_MIN_INTERVAL_AUTH if authenticated else STATES_MIN_INTERVAL_ANON
    records: list[dict[str, Any]] = []
    last_states_call = time.monotonic()  # the credit check above was a /states call
    first_snapshot: dict[str, Any] | None = None

    for probe in probes:
        if probe.path.startswith("/states"):
            _respect_states_throttle(last_states_call, interval)
            last_states_call = time.monotonic()

        record = run_probe(session, probe)
        records.append(record)
        _print_probe_line(record)
        if not args.no_save:
            save_sample(record)

        if probe.name == "states_all_bbox" and isinstance(record["json"], dict):
            first_snapshot = record["json"]

    # /tracks/all, chained off a genuinely airborne aircraft from the live snapshot.
    icao24 = pick_live_icao24(first_snapshot) if first_snapshot else None
    if icao24:
        track_probe = Probe(
            "tracks_all_live",
            "/tracks/all",
            {"icao24": icao24, "time": 0},
            1,
            f"Live track for {icao24}, harvested from the snapshot above.",
            "200 JSON",
        )
        record = run_probe(session, track_probe)
        records.append(record)
        _print_probe_line(record)
        if not args.no_save:
            save_sample(record)
    else:
        print("  skipped /tracks/all: no airborne aircraft with a position in the snapshot")

    _print_summary(records, starting_credits, authenticated)

    state_probes = ("states_all_bbox", "states_all_global", "states_all_bbox_historical")
    for record in records:
        if record["name"] in state_probes and record["json"]:
            describe_state_vectors(
                record["json"],
                f"{record['path']} ({record['name']})",
                requested_time=record["params"].get("time"),
            )
        elif record["name"] == "states_all_bbox_not_extended" and record["json"]:
            rows = record["json"].get("states") or []
            print(f"\n{'=' * 100}")
            print("SCHEMA: /states/all without extended=1")
            print(f"{'=' * 100}")
            print(f"  row lengths observed: {dict(Counter(len(r) for r in rows))}")
            print("  -> confirms extended=1 is what adds index 17 (category)")
        elif record["name"] == "tracks_all_live" and record["json"]:
            describe_track(record["json"], f"{record['path']} ({record['name']})")
        elif record["name"].startswith("flights_") and isinstance(record["json"], list):
            describe_flights(record["json"], f"{record['path']} ({record['name']})")

    if not any(r["name"].startswith("flights_") and r["json"] for r in records):
        _print_unverified_note()

    if not args.no_save:
        save_manifest(records)

    return 0


def _respect_states_throttle(last_call: float, interval: float) -> None:
    waited = time.monotonic() - last_call
    if waited < interval:
        delay = interval - waited
        print(f"  (waiting {delay:.1f}s for the {interval:.0f}s /states resolution limit)")
        time.sleep(delay)


def _print_probe_line(record: dict[str, Any]) -> None:
    status = record.get("status_code")
    if status is None:
        print(f"  {record['name']:<30} ERROR  {record.get('error')}")
        return

    if isinstance(record["json"], dict):
        rows = record["json"].get("states") or record["json"].get("path") or []
        shape = f"{len(rows)} rows"
    elif isinstance(record["json"], list):
        shape = f"{len(record['json'])} items"
    elif record.get("text_body") is not None:
        body = record["text_body"].strip()
        shape = repr(body[:45]) if body else "(empty body)"
    else:
        shape = "-"

    print(
        f"  {record['name']:<30} {status}  {record['bytes']:>8}B  "
        f"{record['elapsed_ms']:>7.1f}ms  credits_left={record['credits_remaining'] or '-':<5} "
        f"{record['content_type'].split(';')[0]:<24} {shape}"
    )


def _print_summary(
    records: list[dict[str, Any]], starting_credits: int | None, authenticated: bool
) -> None:
    print(f"\n{'=' * 100}")
    print(f"REACHABILITY SUMMARY ({'authenticated' if authenticated else 'anonymous'})")
    print(f"{'=' * 100}")
    print(f"  {'endpoint':<26} {'probe':<30} {'got':<6} expected")
    for record in records:
        got = record.get("status_code") or "ERR"
        mark = "ok " if str(got).startswith("2") else "   "
        print(f"{mark} {record['path']:<26} {record['name']:<30} {got:<6} {record['expected']}")

    # A 200 does not mean usable data: /states/own answers 200 with states=null when the account
    # has no receiver, which would raise TypeError on iteration.
    for record in records:
        payload = record.get("json")
        if isinstance(payload, dict) and "states" in payload and payload["states"] is None:
            print(
                f"\n  NOTE: {record['path']} returned 200 but states=null (not []). "
                f"Iterating it raises TypeError -- guard with `payload.get('states') or []`."
            )

    # The header reports a SEPARATE counter per endpoint family, so values are only comparable
    # within a family. Grouping proves it: /states decrements by 1 per bounded call while a
    # /flights call in between reads a completely different (much lower) number.
    groups: dict[str, list[int]] = {}
    for record in records:
        raw = record.get("credits_remaining")
        if raw:
            family = "/" + record["path"].strip("/").split("/")[0]
            groups.setdefault(family, []).append(int(raw))

    if groups:
        print(f"\n  X-Rate-Limit-Remaining, grouped by endpoint family (start={starting_credits}):")
        for family, values in groups.items():
            trend = " -> ".join(str(v) for v in values)
            print(f"    {family:<12} {trend}")
        print("    note: these are SEPARATE counters, not one shared pool -- do not compare across")
        print("          families or treat the header as a single global budget")
    print("    note: 403 responses carry no X-Rate-Limit-Remaining header at all")


def _print_unverified_note() -> None:
    print(f"\n{'=' * 100}")
    print("NOT VERIFIABLE ANONYMOUSLY")
    print(f"{'=' * 100}")
    print("  The /flights/* endpoints answer 403, so their flight-object schema below is taken")
    print("  from the official docs and is DOCUMENTED, NOT OBSERVED:")
    for key in FlightData.keys:
        print(f"    - {key}")
    print("  Obtaining OAuth2 client credentials is what unlocks verifying these.")


def _metadata(record: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in record.items() if k not in ("json", "text_body")}


def save_sample(record: dict[str, Any]) -> None:
    """Persist one response as soon as it arrives.

    Written per-probe rather than in one batch at the end so that an interrupt, a broken pipe, or a
    crash does not discard responses already paid for in credits -- the global snapshot alone costs 4.
    """
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
    payload = record["json"] if record["json"] is not None else record.get("text_body")
    (SAMPLES_DIR / f"{record['name']}.json").write_text(
        json.dumps(
            {"_probe": _metadata(record), "response": payload}, indent=2, ensure_ascii=False
        ),
        encoding="utf-8",
    )


def save_manifest(records: list[dict[str, Any]]) -> None:
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
    (SAMPLES_DIR / "_manifest.json").write_text(
        json.dumps(
            {"captured_at": _fmt_epoch(time.time()), "probes": [_metadata(r) for r in records]},
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"\nWrote {len(records)} samples + manifest to {SAMPLES_DIR}")


if __name__ == "__main__":
    sys.exit(main())
