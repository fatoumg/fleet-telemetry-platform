"""The backfill path. No database, no HTTP, no docker.

simulate() had no test at all before this file, which is worth noticing given what lives in it: the
transmission-delay draw, the server_ts_override logic, and the batch flush whose earlier version
backdated 169,480 rows (simulator/run.py:282-291). tests/test_simulator_forever.py covers only
next_tick and simulate_forever.

The truth recorder is what prompted the file, but the first two tests below would have been worth
having regardless -- they pin the batch/server_ts relationship that the 169,480-row bug broke.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from simulator import run as run_module
from simulator.world import Depot, World

from fleet_telemetry import truth as truth_module

BANJUL = Depot(1, "Banjul", 13.4549, -16.5790)
BASSE = Depot(2, "Basse", 13.3167, -14.2167)
START = datetime(2026, 8, 28, 6, 0, tzinfo=UTC)


class StubApi:
    """Records batches instead of posting them.

    Deliberately NOT subclassing the real Api: this file needs no httpx client, and
    tests/test_simulator_forever.py:101-132 already covers signature drift by subclassing. Keeping
    one of each means a changed Api signature still breaks loudly somewhere.
    """

    def __init__(self) -> None:
        self.batches: list[tuple[list[dict], datetime | None]] = []
        self.pings_sent = 0
        self.pings_inserted = 0

    def post_pings(self, pings, server_ts):
        self.batches.append(([dict(p) for p in pings], server_ts))
        self.pings_sent += len(pings)
        self.pings_inserted += len(pings)
        return len(pings), 0

    def create_job(self, *args, **kwargs):
        return 1

    def move_job(self, *args, **kwargs):
        return None

    def patch_vehicle(self, *args, **kwargs):
        return None


@pytest.fixture
def world():
    return World([BANJUL, BASSE], [1, 2, 3], START, seed=42)


def _recorder(into: list, run_id: str = "66666666-6666-4666-8666-666666666666"):
    return truth_module.TruthRecorder(
        run_id=run_id,
        seed=42,
        sink=lambda rows: into.extend(rows) or len(rows),
    )


def _posted(api: StubApi) -> list[dict]:
    return [ping for batch, _ in api.batches for ping in batch]


# --------------------------------------------------------------------------------------
# properties that predate the truth harness
# --------------------------------------------------------------------------------------


def test_server_ts_never_precedes_the_newest_reading_in_its_batch(world):
    """The 169,480-row regression, pinned. A batch cannot arrive before its own newest reading was
    taken, and the earlier version stamped batches with the PREVIOUS flush time."""
    api = StubApi()
    run_module.simulate(api, world, START, START + timedelta(minutes=5), 5, live=False)

    assert api.batches
    for batch, server_ts in api.batches:
        newest = max(datetime.fromisoformat(p["device_ts"]) for p in batch)
        assert server_ts is not None
        assert server_ts >= newest


def test_live_mode_sends_no_server_ts_at_all(world):
    """--live means the server stamps its own clock, so the override must never be attached."""
    api = StubApi()
    run_module.simulate(api, world, START, START + timedelta(minutes=1), 5, live=True)

    assert api.batches
    assert all(server_ts is None for _, server_ts in api.batches)


# --------------------------------------------------------------------------------------
# the truth recorder
# --------------------------------------------------------------------------------------


def test_every_emitted_ping_has_exactly_one_truth_row(world):
    """The property the whole harness exists for. If this can drift, the diff is measuring the
    recorder rather than the pipeline."""
    api = StubApi()
    written: list = []

    run_module.simulate(
        api, world, START, START + timedelta(minutes=1), 5, live=False, truth=_recorder(written)
    )

    posted = _posted(api)
    assert len(posted) > 0
    assert len(written) == len(posted)
    assert {r.ping_id for r in written} == {p["ping_id"] for p in posted}


def test_truth_records_the_wire_values_not_recomputed_ones(world):
    """The diff compares for equality, so a value re-derived here would manufacture a difference
    the pipeline did not cause."""
    api = StubApi()
    written: list = []

    run_module.simulate(
        api, world, START, START + timedelta(seconds=30), 5, live=False, truth=_recorder(written)
    )

    by_id = {r.ping_id: r for r in written}
    posted = _posted(api)
    assert posted
    for ping in posted:
        row = by_id[ping["ping_id"]]
        assert row.latitude == ping["latitude"]
        assert row.longitude == ping["longitude"]
        assert row.speed_kmh == ping["speed_kmh"]
        assert row.heading_deg == ping["heading_deg"]
        assert row.sequence_no == ping["sequence_no"]
        assert row.vehicle_id == ping["vehicle_id"]
        assert row.intended_device_ts == datetime.fromisoformat(ping["device_ts"])


def test_a_clean_run_records_no_pathology_and_emits_everything(world):
    """The baseline the diff is proven against: nothing skewed, nothing dropped."""
    api = StubApi()
    written: list = []

    run_module.simulate(
        api, world, START, START + timedelta(minutes=1), 5, live=False, truth=_recorder(written)
    )

    assert written
    assert all(r.emitted for r in written)
    assert all(r.pathology is None for r in written)
    # truth_ts and intended_device_ts agree until a clock-skew flag exists.
    assert all(r.truth_ts == r.intended_device_ts for r in written)


def test_truth_is_flushed_before_the_batch_is_posted(world):
    """Ordering, asserted rather than assumed.

    A recorded intent with no ping is a finding the diff reports. A ping with no recorded intent is
    an unexplained phantom that no test can attribute. So truth must land first, and at the moment
    each POST happens every ping in it must already be recorded.
    """
    api = StubApi()
    seen_at_post: list[int] = []
    written: list = []

    recorder = truth_module.TruthRecorder(
        run_id="88888888-8888-4888-8888-888888888888",
        seed=42,
        sink=lambda rows: written.extend(rows) or len(rows),
    )

    original = api.post_pings

    def spy(pings, server_ts):
        seen_at_post.append(len(written))
        return original(pings, server_ts)

    api.post_pings = spy

    # A window long enough to force at least two flushes: 3 vehicles, 5s interval, and
    # PINGS_PER_BATCH is 500, so this needs > 500 pings.
    minutes = (500 * 5 / 3) / 60 + 1
    run_module.simulate(
        api, world, START, START + timedelta(minutes=minutes), 5, live=False, truth=recorder
    )

    assert len(api.batches) >= 2, "the window was too short to force a mid-run flush"
    # At each POST, every ping already sent plus the ones in this batch are recorded.
    cumulative = 0
    for count_at_post, (batch, _) in zip(seen_at_post, api.batches, strict=True):
        cumulative += len(batch)
        assert count_at_post >= cumulative


def test_simulate_runs_unchanged_without_a_recorder(world):
    """--truth is opt-in, so the default path must not require a warehouse or a recorder."""
    api = StubApi()
    stats = run_module.simulate(api, world, START, START + timedelta(minutes=1), 5, live=False)
    assert stats["pings"] > 0
    assert stats["pings"] == api.pings_sent
