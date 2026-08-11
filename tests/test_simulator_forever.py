"""The continuously-running simulator: pacing, shutdown, and the override guarantee.

Offline and fast. The API is a stub that records what it was asked to do, so nothing here
needs docker, a database, or a real second of wall clock -- a test suite that genuinely slept
through five-second intervals would be untestable in practice and therefore never run.

Four properties are worth pinning, and none is visible from reading the loop:

  * the schedule is monotonic seconds, so a wall-clock step cannot move a tick;
  * the cadence does not drift -- each target is derived from the previous *target*, not from
    when the previous tick happened to finish;
  * falling far behind abandons the backlog rather than firing every missed tick at once;
  * forever mode never sends server_ts_override, whatever the API would allow.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from simulator import run as run_module
from simulator.run import MAX_TICK_LAG_SECONDS, Api, next_tick, simulate_forever
from simulator.world import Depot, World

BANJUL = Depot(1, "Banjul", 13.4549, -16.5790)
BASSE = Depot(8, "Basse Santa Su", 13.3167, -14.2167)
START = datetime(2026, 8, 1, 6, 0, tzinfo=UTC)

# An arbitrary monotonic reading. Monotonic values are only meaningful relative to each other,
# which is the point: there is no wall-clock date here to be stepped.
T0 = 10_000.0


# ------------------------------------------------------------------------------------
# pacing
# ------------------------------------------------------------------------------------


def test_the_schedule_is_plain_monotonic_seconds():
    """A regression test for a bug this project actually shipped.

    The schedule used to be wall-clock datetimes. Measured inside the simulator's own
    container, the wall clock stepped BACKWARDS 2.68 s (WSL2 resyncing against the host),
    which made one tick fire ~2.8 s early and the next ~2.8 s late -- visible in the pings
    table as pairs of gaps summing to exactly two intervals.

    Monotonic seconds cannot be stepped, so passing floats here is the fix, and a signature
    that refuses a datetime is what keeps it fixed.
    """
    assert next_tick(T0, T0, 5) == T0 + 5
    with pytest.raises(TypeError):
        next_tick(START, START, 5)


def test_cadence_is_measured_from_the_previous_target_not_from_now():
    """The anti-drift property.

    A tick that took 900 ms must still leave the next one due exactly one interval after the
    last target. Deriving it from `now` instead would push every tick later by however long
    the previous one ran, and the cadence would decay without anything ever looking broken.
    """
    assert next_tick(T0, T0 + 0.9, 5) == T0 + 5


def test_drift_does_not_accumulate_over_many_ticks():
    target = T0
    for _ in range(1000):
        target = next_tick(target, target + 0.3, 5)  # every tick overruns a little
    assert target == pytest.approx(T0 + 5000), "300ms x 1000 ticks leaked into the schedule"


def test_a_small_overrun_still_keeps_the_schedule():
    """One slow tick is absorbed, not treated as a reason to resynchronise."""
    assert next_tick(T0, T0 + 7, 5) == T0 + 5  # overran the 5s interval by 2s


def test_falling_far_behind_abandons_the_backlog():
    """A paused container or a sleeping laptop must not produce a catch-up burst.

    Firing every missed tick back to back would hammer an API that is by definition already
    struggling, and write a flood of pings that never happened.
    """
    now = T0 + MAX_TICK_LAG_SECONDS + 600
    assert next_tick(T0, now, 5) == now + 5


def test_the_lag_threshold_is_the_boundary():
    interval = 5
    target = T0 + interval
    assert next_tick(T0, target + MAX_TICK_LAG_SECONDS - 0.1, interval) == target
    assert next_tick(T0, target + MAX_TICK_LAG_SECONDS + 0.1, interval) != target


# ------------------------------------------------------------------------------------
# the loop
# ------------------------------------------------------------------------------------


class StubApi(Api):
    """Records calls instead of making them, and stops the loop after `ticks` batches.

    Subclasses Api rather than duck-typing it so a signature change to the real client breaks
    this test instead of silently leaving it testing a shape that no longer exists.
    """

    def __init__(self, stop: threading.Event, ticks: int = 1):
        self.batches: list[tuple[list[dict], datetime | None]] = []
        self.jobs_created = 0
        self.patches: list[tuple[int, dict]] = []
        self._stop = stop
        self._ticks = ticks
        self.pings_sent = 0
        self.pings_inserted = 0

    def post_pings(self, pings, server_ts):
        self.batches.append((pings, server_ts))
        self.pings_sent += len(pings)
        self.pings_inserted += len(pings)
        if len(self.batches) >= self._ticks:
            self._stop.set()

    def create_job(self, pickup, dropoff, minutes, at):
        self.jobs_created += 1
        return self.jobs_created

    def move_job(self, job_id, to_status, at, vehicle_id=None):
        return None

    def patch_vehicle(self, vehicle_id, **changes):
        self.patches.append((vehicle_id, changes))


class FakeMonotonic:
    """A monotonic clock the test drives, advancing a hair on every reading.

    The small per-call step stands in for the work a real tick does, so the pacing arithmetic
    is exercised rather than handed identical values.
    """

    def __init__(self, start: float = T0, per_call: float = 0.001):
        self.t = start
        self.per_call = per_call

    def __call__(self) -> float:
        self.t += self.per_call
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class RecordingStop(threading.Event):
    """A stop flag that records the delays it was asked to wait, and never really sleeps.

    It advances the fake clock by the delay instead, which is what sleeping means here. A
    pacing test that slept through real five-second intervals would be too slow to run, and a
    test too slow to run is a test nobody runs.
    """

    def __init__(self, clock: FakeMonotonic):
        super().__init__()
        self.clock = clock
        self.delays: list[float] = []

    def wait(self, timeout=None):
        self.delays.append(timeout)
        if timeout:
            self.clock.advance(timeout)
        return self.is_set()


class SteppingWallClock:
    """A wall clock that advances a second per call, then jumps backwards once.

    Models what WSL2 does to a container: measured inside this project's own simulator, the
    wall clock stepped back 2.68 s within 30 seconds.
    """

    def __init__(self, start: datetime, step_after: int, step_by: float):
        self.start = start
        self.step_after = step_after
        self.step_by = step_by
        self.calls = 0

    def now(self, _tz=None) -> datetime:
        self.calls += 1
        offset = timedelta(seconds=self.calls)
        if self.calls > self.step_after:
            offset -= timedelta(seconds=self.step_by)
        return self.start + offset


@pytest.fixture
def world() -> World:
    return World([BANJUL, BASSE], [1, 2, 3], START, seed=42)


@pytest.fixture
def paced(monkeypatch) -> RecordingStop:
    """A stop flag with the simulator's clock under the test's control.

    Every loop test uses this rather than a bare Event, so nothing in this file sleeps through
    a real interval. The alternative -- letting the loop wait genuinely -- made the suite take
    seconds per test for no extra coverage.
    """
    clock = FakeMonotonic()
    monkeypatch.setattr(run_module, "time", SimpleNamespace(monotonic=clock))
    return RecordingStop(clock)


def test_a_backward_wall_clock_step_does_not_move_a_tick(monkeypatch, world):
    """The end-to-end version of the regression above.

    The wall clock jumps back 2.68 s mid-run. Every scheduled delay must still be one interval,
    because the schedule never consults the wall clock. Before the fix this produced a tick
    ~2.8 s early followed by one ~2.8 s late.
    """
    monkeypatch.setattr(
        run_module, "datetime", SteppingWallClock(START, step_after=3, step_by=2.68)
    )
    clock = FakeMonotonic()
    monkeypatch.setattr(run_module, "time", SimpleNamespace(monotonic=clock))

    stop = RecordingStop(clock)
    simulate_forever(StubApi(stop, ticks=8), world, interval_seconds=5, stop=stop)

    assert stop.delays, "the loop never scheduled a wait"
    for delay in stop.delays:
        assert delay == pytest.approx(5.0, abs=0.05), f"a wall-clock step leaked into {delay}"


def test_forever_stops_when_the_event_is_set(world, paced):
    """Shutdown is cooperative: the flag is checked between ticks, so none is left half-done."""
    api = StubApi(paced, ticks=3)
    stats = simulate_forever(api, world, interval_seconds=1, stop=paced)
    assert stats["ticks"] == 3
    assert stats["pings"] == 9, "three vehicles x three ticks"


def test_forever_never_sends_a_server_ts_override(world, paced):
    """The single most important test in this file.

    Real time needs no pretending. If forever mode ever backdated server_ts it would destroy
    the one timestamp in the system that no client can influence -- and it would do so
    silently, because the rows would still look entirely plausible.
    """
    api = StubApi(paced, ticks=2)
    simulate_forever(api, world, interval_seconds=1, stop=paced)
    assert api.batches, "the loop produced nothing to check"
    for _, server_ts in api.batches:
        assert server_ts is None


def test_each_tick_emits_exactly_one_reading_per_vehicle(world, paced):
    api = StubApi(paced, ticks=2)
    simulate_forever(api, world, interval_seconds=1, stop=paced)
    for pings, _ in api.batches:
        assert sorted(p["vehicle_id"] for p in pings) == [1, 2, 3]


def test_sequence_numbers_advance_by_one_per_tick(world, paced):
    """A gap here would be indistinguishable downstream from a device losing signal."""
    api = StubApi(paced, ticks=4)
    simulate_forever(api, world, interval_seconds=1, stop=paced)

    by_vehicle: dict[int, list[int]] = {}
    for pings, _ in api.batches:
        for ping in pings:
            by_vehicle.setdefault(ping["vehicle_id"], []).append(ping["sequence_no"])

    for vehicle_id, sequence in by_vehicle.items():
        expected = list(range(sequence[0], sequence[0] + len(sequence)))
        assert sequence == expected, f"vehicle {vehicle_id} skipped or repeated a sequence number"


def test_ping_ids_are_unique_across_ticks(world, paced):
    """ping_id is the primary key and the idempotency handle; a collision would drop real data."""
    api = StubApi(paced, ticks=5)
    simulate_forever(api, world, interval_seconds=1, stop=paced)
    ids = [p["ping_id"] for pings, _ in api.batches for p in pings]
    assert len(ids) == len(set(ids))


def test_device_ts_is_never_in_the_future(world, paced):
    """Forever mode stamps device_ts from the real clock, so it must already have happened."""
    api = StubApi(paced, ticks=2)
    simulate_forever(api, world, interval_seconds=1, stop=paced)
    now = datetime.now(UTC)
    for pings, _ in api.batches:
        for ping in pings:
            assert datetime.fromisoformat(ping["device_ts"]) <= now
