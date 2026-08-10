"""Drive the fleet application, either in bursts or continuously.

Two modes, and the difference is which clock is in charge.

BACKFILL (default) -- generate a window of history in a few minutes of wall clock. It walks a
**simulated clock** forward in ping-interval steps, emits what every vehicle would have
reported at that instant, and posts batches to the API.

    python -m simulator --hours 2 --vehicles 20

FOREVER (`--forever`) -- emit at real speed, indefinitely, until stopped. One tick per
interval, paced against the wall clock. This is the mode the compose `simulator` service runs,
and it is what "the world is happening" looks like: nothing schedules it, it simply does not
stop.

    python -m simulator --forever --vehicles 40 --interval 5
    docker compose -f docker/docker-compose.yml up -d simulator

WHY BACKFILL NEEDS THE SERVER'S COOPERATION
-------------------------------------------
`server_ts` is supposed to be the API's own clock -- the one value no client can influence,
which is exactly why it is the trustworthy one. Generating last week's data honestly is
therefore impossible: a correct server would stamp every row with *now*, and the whole window
would collapse into the few minutes the simulator took to run.

So the API has a backfill override, disabled unless `FLEET_ALLOW_SERVER_TS_OVERRIDE=true`.
This is a real pattern rather than a shortcut -- production systems do have privileged import
paths -- and the rule is that they are explicit and off by default.

`--forever` never sends it, and does not need it: real time needs no pretending. The lateness
it produces is genuine network and processing delay, which is the honest measurement.

WHAT "CLEAN" MEANS IN PHASE 1
-----------------------------
Every ping is well-formed, every sequence is complete, nothing is duplicated, and no device
clock is wrong. In backfill mode the only variation is a small, realistic **transmission
delay** between a reading being taken and the server receiving it, because a distribution of
exactly zero would make the phase 1 measurement meaningless.

That is the clean baseline. Phase 3 adds the pathologies -- reconnect bursts, clock skew,
retry storms, sequence gaps -- and having measured the clean case first is what makes their
effect visible.
"""

from __future__ import annotations

import argparse
import signal
import sys
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta

import httpx

from fleet_telemetry import config
from simulator.world import Depot, World

# A reading takes a moment to reach the server: radio, mobile network, our own queueing.
# Seconds. Phase 1 keeps this small and well-behaved; phase 3 makes it ugly. Backfill only --
# in `--forever` the delay is whatever really happens.
TRANSMISSION_DELAY_SECONDS = (1, 8)

# Devices buffer a few readings and send them together rather than one HTTP request per ping.
PINGS_PER_BATCH = 500

# How far behind schedule `--forever` tolerates before abandoning the backlog. See next_tick().
MAX_TICK_LAG_SECONDS = 30.0

# How often `--forever` prints a line of life. Long-running containers that log nothing are
# indistinguishable from hung ones.
HEARTBEAT_SECONDS = 60.0

# Consecutive failed ticks before `--forever` gives up and exits non-zero. Generous, because a
# restarting API is a normal event; bounded, because a simulator that logs errors for a week
# without exiting is worse than one that dies and shows up as a restart loop.
MAX_CONSECUTIVE_FAILURES = 20


def fetch_depots() -> list[Depot]:
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

    def healthy(self) -> bool:
        try:
            return self.client.get("/health").json()["status"] == "ok"
        except (httpx.HTTPError, KeyError, ValueError):
            return False


def new_stats() -> dict[str, int]:
    return {"pings": 0, "jobs_created": 0, "jobs_delivered": 0, "mutations": 0, "ticks": 0}


# --------------------------------------------------------------------------------------
# one tick -- shared by both modes
# --------------------------------------------------------------------------------------
#
# Backfill and forever differ only in which clock drives them and how batching works. The
# actual behaviour of the world -- what a vehicle reports, when a job moves, when an entity
# mutates -- must be identical, or the two modes would produce differently-shaped data and
# the phase 1 baseline would only describe one of them.


def take_reading(vehicle, world: World, now: datetime) -> dict:
    """One GPS reading, advancing the device's counter.

    The increment belongs here rather than at the call site: a sequence number is consumed by
    the act of taking a reading, and two callers each remembering to bump it separately is one
    forgotten line away from silent duplicates.
    """
    latitude, longitude = vehicle.position(now)
    reading = {
        "ping_id": str(uuid.uuid4()),
        "vehicle_id": vehicle.vehicle_id,
        "sequence_no": vehicle.sequence_no,
        "device_ts": now.isoformat(),
        "latitude": round(latitude, 6),
        "longitude": round(longitude, 6),
        "speed_kmh": round(world.speed_kmh(vehicle, now), 1),
        "heading_deg": round(vehicle.heading(), 1),
    }
    vehicle.sequence_no += 1
    return reading


def advance_jobs(api: Api, world: World, now: datetime, stats: dict[str, int]) -> None:
    """Complete the jobs of vehicles that just arrived, and start one for anything moving."""
    for vehicle in world.advance(now):
        if vehicle.job_id is not None:
            api.move_job(vehicle.job_id, "delivered", now)
            stats["jobs_delivered"] += 1

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


def maybe_mutate(api: Api, world: World, interval_seconds: int, stats: dict[str, int]) -> None:
    """Occasionally reassign a vehicle's home depot.

    This is the raw material for Slowly Changing Dimensions, and roughly one change every
    simulated 20 minutes is enough history to model over a month. The OLTP row simply
    overwrites -- which is precisely the problem phase 2 and 3 exist to solve.
    """
    if world.rng.random() < interval_seconds / 1200.0:
        vehicle = world.rng.choice(world.vehicles)
        api.patch_vehicle(
            vehicle.vehicle_id,
            home_depot_id=world.rng.choice(world.depots).depot_id,
        )
        stats["mutations"] += 1


# --------------------------------------------------------------------------------------
# backfill: a simulated clock, run flat out
# --------------------------------------------------------------------------------------


def simulate(
    api: Api,
    world: World,
    start: datetime,
    end: datetime,
    interval_seconds: int,
    live: bool,
) -> dict[str, int]:
    """Walk the clock from start to end, emitting pings and job transitions."""
    stats = new_stats()
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
            buffer.append(take_reading(vehicle, world, now))
            buffer_max_device_ts = now
            if len(buffer) >= PINGS_PER_BATCH:
                flush()

        advance_jobs(api, world, now, stats)
        maybe_mutate(api, world, interval_seconds, stats)
        stats["ticks"] += 1
        now += timedelta(seconds=interval_seconds)

    flush()
    return stats


# --------------------------------------------------------------------------------------
# forever: the real clock, paced
# --------------------------------------------------------------------------------------


def next_tick(scheduled: float, now: float, interval_seconds: int) -> float:
    """When the tick after `scheduled` is due, in MONOTONIC seconds.

    Two decisions are packed in here, and both were found by measuring rather than reasoning.

    **Monotonic seconds, not wall-clock datetimes.** `time.monotonic()` only ever moves
    forward at one second per second. The wall clock does not: NTP steps it, and a container
    on WSL2 resyncs against the host regularly. Measured inside this project's own simulator
    container, the wall clock jumped **backwards 2.68 s** within a 30-second window. Pacing on
    wall-clock arithmetic therefore made a tick fire ~2.8 s early and the next one ~2.8 s late
    -- pairs of gaps summing to exactly two intervals, which is what the pings table showed
    before this was fixed. Readings are still *stamped* with the wall clock, because a device
    timestamp is a real point in time; only the schedule is monotonic.

    **The previous target plus one interval, never `now` plus one interval.** Adding to `now`
    lets the duration of each tick accumulate, so a loop doing 200 ms of work per tick drifts
    by a minute every five hours and the cadence quietly stops being the cadence.

    The exception is falling badly behind -- a slow API, a paused container, a laptop waking
    from sleep. Catching up would mean firing every missed tick back to back, hammering a
    service that is by definition already struggling, and writing a burst of pings that never
    happened. Past MAX_TICK_LAG_SECONDS we abandon the backlog and restart the cadence from
    now. Dropped ticks show up downstream as sequence gaps, which is the honest record: those
    readings really were never sent.
    """
    target = scheduled + interval_seconds
    if now - target > MAX_TICK_LAG_SECONDS:
        return now + interval_seconds
    return target


def simulate_forever(
    api: Api,
    world: World,
    interval_seconds: int,
    stop: threading.Event,
) -> dict[str, int]:
    """Emit one tick per interval against the real clock until `stop` is set.

    No server_ts override is ever sent, whatever the API would permit: running live means the
    server stamps its own clock, and the resulting lateness is real rather than manufactured.

    A tick that fails is logged and skipped rather than fatal. An API that restarts underneath
    a week-long simulator run is an ordinary event, and the sequence gap left behind is the
    correct record of readings that genuinely never arrived -- exactly what sequence_no is for.
    """
    stats = new_stats()
    # The schedule lives on the monotonic clock; only the readings use the wall clock. See
    # next_tick() for the 2.68 s backward step that made this necessary.
    target = time.monotonic()
    last_heartbeat = target
    consecutive_failures = 0

    while not stop.is_set():
        now = datetime.now(UTC)
        try:
            api.post_pings([take_reading(v, world, now) for v in world.vehicles], None)
            stats["pings"] += len(world.vehicles)
            advance_jobs(api, world, now, stats)
            maybe_mutate(api, world, interval_seconds, stats)
            consecutive_failures = 0
        except httpx.HTTPError as exc:
            consecutive_failures += 1
            print(
                f"tick failed ({consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}): "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                print("too many consecutive failures; giving up", flush=True)
                raise

        stats["ticks"] += 1

        # Monotonic here too: a wall clock that steps backwards would otherwise suppress the
        # heartbeat until it caught up, making a healthy simulator look hung.
        if time.monotonic() - last_heartbeat >= HEARTBEAT_SECONDS:
            print(
                f"{now:%Y-%m-%d %H:%M:%S}Z  ticks={stats['ticks']:,}  "
                f"pings={stats['pings']:,}  jobs={stats['jobs_created']:,}  "
                f"changes={stats['mutations']:,}",
                flush=True,
            )
            last_heartbeat = time.monotonic()

        target = next_tick(target, time.monotonic(), interval_seconds)
        delay = target - time.monotonic()
        if delay > 0:
            # Event.wait rather than sleep: a SIGTERM during the idle gap should stop the
            # simulator immediately, not up to `interval` seconds later. Docker's default
            # grace period is 10s, so a plain sleep would get the process killed mid-tick.
            stop.wait(delay)

    return stats


def install_signal_handlers() -> threading.Event:
    """Turn SIGINT/SIGTERM into a stop flag, so a tick finishes before the process exits.

    Docker sends SIGTERM on `compose stop` and waits out the grace period before SIGKILL.
    Handling it means the current tick completes and the summary prints, instead of the
    container dying mid-HTTP-request.
    """
    stop = threading.Event()

    def handler(signum, _frame):
        if stop.is_set():
            # Second interrupt: the user means it.
            raise KeyboardInterrupt
        print(f"\nreceived {signal.Signals(signum).name}, stopping after this tick", flush=True)
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handler)
    return stop


def wait_for_api(api: Api, stop: threading.Event, timeout_seconds: float = 120.0) -> bool:
    """Block until the API answers healthily, or the timeout expires.

    Compose's `depends_on: service_healthy` already covers the normal startup order. This is
    for the cases it does not: the API restarting under a running simulator, or someone
    starting the two by hand in the wrong order.
    """
    deadline = datetime.now(UTC) + timedelta(seconds=timeout_seconds)
    announced = False
    while not stop.is_set():
        if api.healthy():
            return True
        if datetime.now(UTC) >= deadline:
            return False
        if not announced:
            print(f"waiting for the API at {api.client.base_url} ...", flush=True)
            announced = True
        stop.wait(2.0)
    return False


# --------------------------------------------------------------------------------------
# entry point
# --------------------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vehicles", type=int, default=20, help="how many vehicles to move")
    parser.add_argument("--hours", type=float, help="simulated hours to generate (default 2)")
    parser.add_argument("--minutes", type=float, help="simulated minutes (overrides --hours)")
    parser.add_argument("--interval", type=int, default=5, help="seconds between pings")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed; same seed, same data")
    parser.add_argument(
        "--api",
        default=None,
        help="base URL of the app (default: FLEET_API_URL, else http://127.0.0.1:8000)",
    )
    parser.add_argument(
        "--forever",
        action="store_true",
        help="emit at real speed indefinitely, until SIGINT or SIGTERM. Needs no special "
        "server configuration. This is what the compose simulator service runs.",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="backfill a window but let the server stamp its own clock. Note this still runs "
        "flat out, so every row lands with a lateness of up to the whole window; use "
        "--forever for genuine real-time pacing.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="delete all existing pings first. Without this, runs append and sequence numbers "
        "continue from where the last run stopped.",
    )
    args = parser.parse_args(argv)

    if args.forever and (args.hours or args.minutes or args.live):
        parser.error(
            "--forever runs against the real clock until stopped; "
            "--hours, --minutes and --live do not apply"
        )
    if args.interval < 1:
        parser.error("--interval must be at least 1 second")

    base_url = args.api or config.api_url()
    # Forever mode never backfills, so it never needs -- and must never be able to use -- the
    # override that lets a client dictate server_ts.
    api = Api(base_url, config.api_token(), allow_override=not (args.live or args.forever))
    stop = install_signal_handlers()

    if not wait_for_api(api, stop):
        print(f"cannot reach a healthy API at {base_url}")
        print("start it with:  uvicorn app.main:app --port 8000")
        return 1
    if stop.is_set():
        return 0

    if args.reset:
        removed = reset_pings()
        print(f"--reset: deleted {removed:,} existing pings")

    depots = fetch_depots()
    vehicle_ids = fetch_vehicle_ids(args.vehicles)
    if not vehicle_ids:
        print("no active vehicles in the database")
        return 1

    span = timedelta(minutes=args.minutes) if args.minutes else timedelta(hours=args.hours or 2.0)
    end = datetime.now(UTC)
    start = end if args.forever else end - span

    world = World(depots, vehicle_ids, start, args.seed)
    # Resume each vehicle's counter where the database left it, so repeated runs append to a
    # continuous sequence instead of colliding with the previous run's numbers.
    resume_from = fetch_next_sequence_numbers()
    for vehicle in world.vehicles:
        vehicle.sequence_no = resume_from.get(vehicle.vehicle_id, 0)

    if args.forever:
        rate = len(vehicle_ids) / args.interval
        print(
            f"simulating {len(vehicle_ids)} vehicles at {args.interval}s intervals, "
            f"until stopped\n"
            f"  mode   : forever (real clock, server stamps server_ts)\n"
            f"  seed   : {args.seed}\n"
            f"  rate   : {rate:.1f} pings/sec, ~{rate * 86400:,.0f}/day\n"
            f"  api    : {base_url}",
            flush=True,
        )
    else:
        expected = len(vehicle_ids) * int(span.total_seconds() / args.interval)
        print(
            f"simulating {len(vehicle_ids)} vehicles over {span} at {args.interval}s intervals\n"
            f"  window : {start:%Y-%m-%d %H:%M:%S} -> {end:%Y-%m-%d %H:%M:%S} UTC\n"
            f"  mode   : {'live (server clock)' if args.live else 'backfill (simulated clock)'}\n"
            f"  seed   : {args.seed}\n"
            f"  expect : ~{expected:,} pings"
        )

    started = datetime.now(UTC)
    try:
        if args.forever:
            stats = simulate_forever(api, world, args.interval, stop)
        else:
            stats = simulate(api, world, start, end, args.interval, args.live)
    except KeyboardInterrupt:
        print("\ninterrupted")
        return 130
    except httpx.HTTPStatusError as exc:
        print(f"\nAPI rejected a request: {exc.response.status_code} {exc.response.text[:300]}")
        if exc.response.status_code == 403:
            print(
                "\nBackfill needs the server started with FLEET_ALLOW_SERVER_TS_OVERRIDE=true,\n"
                "or run the simulator with --forever."
            )
        return 1
    except httpx.HTTPError as exc:
        print(f"\ngave up talking to the API: {type(exc).__name__}: {exc}")
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
