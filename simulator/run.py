"""Drive the fleet application at volume.

Generates a window of history in a few minutes of wall clock. It walks a **simulated clock**
forward in ping-interval steps, emits what every vehicle would have reported at that instant,
and posts batches to the API.

WHY THE SIMULATED CLOCK NEEDS THE SERVER'S COOPERATION
------------------------------------------------------
`server_ts` is supposed to be the API's own clock -- the one value no client can influence,
which is exactly why it is the trustworthy one. Generating last week's data honestly is
therefore impossible: a correct server would stamp every row with *now*, and the whole window
would collapse into the few minutes the simulator took to run.

So the API has a backfill override, disabled unless `FLEET_ALLOW_SERVER_TS_OVERRIDE=true`.
This is a real pattern rather than a shortcut -- production systems do have privileged import
paths -- and the rule is that they are explicit and off by default.

Run `--live` instead to skip all of that: real time, real server clock, no override, no
special server configuration. Lower volume, but nothing is pretending.

WHAT "CLEAN" MEANS IN PHASE 1
-----------------------------
Every ping is well-formed, every sequence is complete, nothing is duplicated, and no device
clock is wrong. The only variation is a small, realistic **transmission delay** between a
reading being taken and the server receiving it, because a distribution of exactly zero would
make the phase 1 measurement meaningless.

That is the clean baseline. Phase 3 adds the pathologies -- reconnect bursts, clock skew,
retry storms, sequence gaps -- and having measured the clean case first is what makes their
effect visible.

    python -m simulator --hours 2 --vehicles 20
    python -m simulator --live --minutes 5
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import UTC, datetime, timedelta

import httpx

from fleet_telemetry import config
from simulator.world import Depot, World

# A reading takes a moment to reach the server: radio, mobile network, our own queueing.
# Seconds. Phase 1 keeps this small and well-behaved; phase 3 makes it ugly.
TRANSMISSION_DELAY_SECONDS = (1, 8)

# Devices buffer a few readings and send them together rather than one HTTP request per ping.
PINGS_PER_BATCH = 500


def fetch_depots(client: httpx.Client) -> list[Depot]:
    """Read the depots from the database rather than hardcoding them here.

    Two sources of truth for the depot list would drift, and the drift would show up as
    vehicles teleporting to coordinates the warehouse has never heard of.
    """
    from psycopg import connect
    from psycopg.rows import dict_row

    with connect(config.oltp().dsn(), row_factory=dict_row) as conn:
        rows = conn.execute(
            "select depot_id, name, latitude, longitude from depots order by depot_id"
        ).fetchall()
    return [Depot(**row) for row in rows]


def fetch_vehicle_ids(limit: int) -> list[int]:
    from psycopg import connect

    with connect(config.oltp().dsn()) as conn:
        rows = conn.execute(
            "select vehicle_id from vehicles where status = 'active' order by vehicle_id limit %s",
            (limit,),
        ).fetchall()
    return [r[0] for r in rows]


def fetch_next_sequence_numbers() -> dict[int, int]:
    """Where each vehicle's ping counter has already reached.

    A device's sequence number is a property of the device, not of a simulator run: it counts
    up forever and does not restart because we happened to launch the process again. Starting
    every run from zero produced 7,201 duplicate (vehicle_id, sequence_no) pairs on the second
    run -- invisible in the code, obvious the moment the profiler counted repeats.

    That matters beyond tidiness. Phase 3 uses sequence gaps to distinguish "the device lost
    signal" from "the vehicle stopped". A counter that silently rewinds makes that signal
    meaningless.
    """
    from psycopg import connect

    with connect(config.oltp().dsn()) as conn:
        rows = conn.execute(
            "select vehicle_id, max(sequence_no) + 1 as next from pings group by vehicle_id"
        ).fetchall()
    return {vehicle_id: next_no for vehicle_id, next_no in rows}


def reset_pings() -> int:
    """Delete every ping, for a genuinely clean run. Returns how many were removed."""
    from psycopg import connect

    with connect(config.oltp().dsn()) as conn, conn.cursor() as cur:
        cur.execute("delete from pings")
        return cur.rowcount


class Api:
    """Thin client. Raises on anything unexpected -- a silent failure here would look like
    missing data downstream and waste hours."""

    def __init__(self, base_url: str, token: str, allow_override: bool):
        self.client = httpx.Client(
            base_url=base_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=60.0,
        )
        self.allow_override = allow_override
        self.pings_sent = 0
        self.pings_inserted = 0

    def post_pings(self, pings: list[dict], server_ts: datetime | None) -> None:
        body: dict = {"pings": pings}
        if server_ts is not None and self.allow_override:
            body["server_ts_override"] = server_ts.isoformat()
        response = self.client.post("/pings", json=body)
        response.raise_for_status()
        result = response.json()
        self.pings_sent += result["received"]
        self.pings_inserted += result["inserted"]

    def create_job(self, pickup: int, dropoff: int, minutes: int, at: datetime) -> int:
        response = self.client.post(
            "/jobs",
            json={
                "pickup_depot_id": pickup,
                "dropoff_depot_id": dropoff,
                "estimated_duration_minutes": minutes,
                "requested_at": at.isoformat(),
            },
        )
        response.raise_for_status()
        return response.json()["job_id"]

    def move_job(self, job_id: int, to_status: str, at: datetime, vehicle_id: int | None = None):
        payload: dict = {"to_status": to_status, "occurred_at": at.isoformat()}
        if vehicle_id is not None:
            payload["vehicle_id"] = vehicle_id
        response = self.client.patch(f"/jobs/{job_id}/status", json=payload)
        response.raise_for_status()

    def patch_driver(self, driver_id: int, **changes) -> None:
        self.client.patch(f"/drivers/{driver_id}", json=changes).raise_for_status()

    def patch_vehicle(self, vehicle_id: int, **changes) -> None:
        self.client.patch(f"/vehicles/{vehicle_id}", json=changes).raise_for_status()


def simulate(
    api: Api,
    world: World,
    start: datetime,
    end: datetime,
    interval_seconds: int,
    live: bool,
) -> dict[str, int]:
    """Walk the clock from start to end, emitting pings and job transitions."""
    stats = {"pings": 0, "jobs_created": 0, "jobs_delivered": 0, "mutations": 0}
    buffer: list[dict] = []
    # The newest reading in the buffer. server_ts is derived from THIS, not from the wall
    # clock and not from when the previous batch went out.
    #
    # An earlier version stamped each batch with the *previous* flush time, which backdated
    # every row by roughly one batch and produced 169,480 pings whose server_ts preceded their
    # device_ts. Nothing in the code looked wrong; the profiler found it by measuring the
    # lateness distribution and seeing a median of -29 seconds. A batch cannot arrive before
    # its own newest reading was taken.
    buffer_max_device_ts: datetime | None = None
    now = start

    def flush() -> None:
        nonlocal buffer, buffer_max_device_ts
        if not buffer:
            return
        server_ts = None
        if not live and buffer_max_device_ts is not None:
            server_ts = buffer_max_device_ts + timedelta(
                seconds=world.rng.randint(*TRANSMISSION_DELAY_SECONDS)
            )
        api.post_pings(buffer, server_ts)
        stats["pings"] += len(buffer)
        buffer = []
        buffer_max_device_ts = None

    while now < end:
        for vehicle in world.vehicles:
            lat, lon = vehicle.position(now)
            buffer.append(
                {
                    "ping_id": str(uuid.uuid4()),
                    "vehicle_id": vehicle.vehicle_id,
                    "sequence_no": vehicle.sequence_no,
                    "device_ts": now.isoformat(),
                    "latitude": round(lat, 6),
                    "longitude": round(lon, 6),
                    "speed_kmh": round(world.speed_kmh(vehicle, now), 1),
                    "heading_deg": round(vehicle.heading(), 1),
                }
            )
            vehicle.sequence_no += 1
            buffer_max_device_ts = now
            if len(buffer) >= PINGS_PER_BATCH:
                flush()

        # A vehicle that just finished a leg completes its job and is given a new one.
        for vehicle in world.advance(now):
            if vehicle.job_id is not None:
                api.move_job(vehicle.job_id, "delivered", now)
                stats["jobs_delivered"] += 1

        # Start a job for anything that has set off without one.
        for vehicle in world.vehicles:
            if vehicle.job_id is None and world.is_moving(vehicle, now):
                estimate = max(int(vehicle.leg_seconds / 60), 1)
                job_id = api.create_job(
                    vehicle.origin.depot_id, vehicle.destination.depot_id, estimate, now
                )
                api.move_job(job_id, "assigned", now, vehicle_id=vehicle.vehicle_id)
                api.move_job(job_id, "picked_up", now)
                vehicle.job_id = job_id
                stats["jobs_created"] += 1

        # Occasional entity mutations: the raw material for Slowly Changing Dimensions. Roughly
        # one every simulated 20 minutes, which over a month is enough history to model.
        if world.rng.random() < interval_seconds / 1200.0:
            vehicle = world.rng.choice(world.vehicles)
            api.patch_vehicle(
                vehicle.vehicle_id,
                home_depot_id=world.rng.choice(world.depots).depot_id,
            )
            stats["mutations"] += 1

        now += timedelta(seconds=interval_seconds)

    flush()
    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vehicles", type=int, default=20, help="how many vehicles to move")
    parser.add_argument("--hours", type=float, default=2.0, help="simulated hours to generate")
    parser.add_argument("--minutes", type=float, help="simulated minutes (overrides --hours)")
    parser.add_argument("--interval", type=int, default=5, help="seconds between pings")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed; same seed, same data")
    parser.add_argument("--api", default="http://127.0.0.1:8000", help="base URL of the app")
    parser.add_argument(
        "--live",
        action="store_true",
        help="emit against the real clock instead of backfilling history. Needs no special "
        "server configuration, but generates only as much data as time actually passes.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="delete all existing pings first. Without this, runs append and sequence numbers "
        "continue from where the last run stopped.",
    )
    args = parser.parse_args(argv)

    span = timedelta(minutes=args.minutes) if args.minutes else timedelta(hours=args.hours)
    end = datetime.now(UTC)
    start = end - span

    api = Api(args.api, config.api_token(), allow_override=not args.live)
    try:
        health = api.client.get("/health").json()
    except httpx.HTTPError as exc:
        print(f"cannot reach the API at {args.api}: {exc}")
        print("start it with:  uvicorn app.main:app --port 8000")
        return 1
    if health["status"] != "ok":
        print(f"API is up but unhealthy: {health}")
        return 1

    if args.reset:
        removed = reset_pings()
        print(f"--reset: deleted {removed:,} existing pings")

    depots = fetch_depots(api.client)
    vehicle_ids = fetch_vehicle_ids(args.vehicles)
    if not vehicle_ids:
        print("no active vehicles in the database")
        return 1

    world = World(depots, vehicle_ids, start, args.seed)
    # Resume each vehicle's counter where the database left it, so repeated runs append to a
    # continuous sequence instead of colliding with the previous run's numbers.
    resume_from = fetch_next_sequence_numbers()
    for vehicle in world.vehicles:
        vehicle.sequence_no = resume_from.get(vehicle.vehicle_id, 0)
    expected = len(vehicle_ids) * int(span.total_seconds() / args.interval)
    print(
        f"simulating {len(vehicle_ids)} vehicles over {span} at {args.interval}s intervals\n"
        f"  window : {start:%Y-%m-%d %H:%M:%S} -> {end:%Y-%m-%d %H:%M:%S} UTC\n"
        f"  mode   : {'live (real clock)' if args.live else 'backfill (simulated clock)'}\n"
        f"  seed   : {args.seed}\n"
        f"  expect : ~{expected:,} pings"
    )

    started = datetime.now(UTC)
    try:
        stats = simulate(api, world, start, end, args.interval, args.live)
    except httpx.HTTPStatusError as exc:
        print(f"\nAPI rejected a request: {exc.response.status_code} {exc.response.text[:300]}")
        if exc.response.status_code == 403:
            print(
                "\nBackfill needs the server started with FLEET_ALLOW_SERVER_TS_OVERRIDE=true,\n"
                "or run the simulator with --live."
            )
        return 1
    elapsed = (datetime.now(UTC) - started).total_seconds()

    print(
        f"\ndone in {elapsed:.1f}s\n"
        f"  pings sent     : {stats['pings']:,} ({api.pings_inserted:,} inserted, "
        f"{api.pings_sent - api.pings_inserted:,} duplicate)\n"
        f"  jobs created   : {stats['jobs_created']:,}\n"
        f"  jobs delivered : {stats['jobs_delivered']:,}\n"
        f"  entity changes : {stats['mutations']:,}\n"
        f"  throughput     : {stats['pings'] / max(elapsed, 0.001):,.0f} pings/sec"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
