"""The fleet API, against a real database.

Marked `integration` because these need the OLTP container running:

    docker compose -f docker/docker-compose.yml up -d oltp
    pytest -m integration

Skip them with `pytest -m "not integration"`.

Deliberately NOT mocked. The behaviour under test here is mostly the *database's* -- ON
CONFLICT deduplication, SELECT FOR UPDATE locking, the updated_at trigger. A mocked connection
would assert that we call psycopg correctly, which is not the same thing as asserting the data
ends up right, and it is precisely where a mocked test would pass while production broke.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from fleet_telemetry import config

pytestmark = pytest.mark.integration

AUTH = {"Authorization": f"Bearer {config.api_token()}"}


# Test rows live in id ranges the seed data and the simulator never touch, so teardown can
# identify them exactly. Mutating the seeded fleet instead would leave real-looking history
# behind in a table the profiler reads.
TEST_SEQ_BASE = 900_000
TEST_DRIVER_IDS = (9001, 9002)
TEST_VEHICLE_ID = 9101


@pytest.fixture(scope="module")
def client():
    """A test client with the real lifespan, so the connection pool actually opens.

    Cleans up after itself, and that is correctness rather than housekeeping.

    These tests share the OLTP database with the simulator and the profiler, and they
    deliberately insert nonsense: a device clock three hours fast, a server_ts of 2020, sequence
    numbers starting at 900,000. Left behind, that poisons every measurement -- the first run of
    this suite dragged the profiled lateness minimum to -208,270,649 seconds and invented three
    sequence gaps covering 895,683 phantom pings.

    A profile is only worth reading if the database contains nothing but what the source system
    actually produced.
    """
    psycopg = pytest.importorskip("psycopg")
    try:
        psycopg.connect(config.oltp().dsn(), connect_timeout=3).close()
    except Exception as exc:
        pytest.skip(f"OLTP database not reachable ({type(exc).__name__}); start docker compose")

    # Anything created from here on has an id above this mark.
    with psycopg.connect(config.oltp().dsn()) as conn, conn.cursor() as cur:
        cur.execute("select coalesce(max(job_id), 0) from jobs")
        job_watermark = cur.fetchone()[0]
        # Disposable drivers for the mutation tests.
        #
        # An earlier version mutated seeded drivers and tried to restore updated_at afterwards.
        # It could not: writing updated_at fires the very trigger that maintains it, so the
        # restore was silently overwritten with now() and three drivers were left looking
        # permanently changed. That the trigger cannot be bypassed by ordinary SQL is exactly
        # why phase 2's poller can trust it -- so work with it rather than against it.
        for driver_id in TEST_DRIVER_IDS:
            cur.execute(
                "insert into drivers (driver_id, full_name, phone, home_depot_id) "
                "values (%s, %s, %s, 1) on conflict (driver_id) do nothing",
                (driver_id, f"Test Driver {driver_id}", "+2200000000"),
            )

    from app.main import app

    with TestClient(app) as c:
        yield c

    with psycopg.connect(config.oltp().dsn()) as conn, conn.cursor() as cur:
        cur.execute("delete from pings where sequence_no >= %s", (TEST_SEQ_BASE,))
        cur.execute("delete from job_events where job_id > %s", (job_watermark,))
        cur.execute("delete from jobs where job_id > %s", (job_watermark,))
        cur.execute("delete from drivers where driver_id = any(%s)", (list(TEST_DRIVER_IDS),))
        cur.execute("delete from vehicles where vehicle_id = %s", (TEST_VEHICLE_ID,))


def _ping(
    vehicle_id: int = 1, seq: int = TEST_SEQ_BASE, ping_id: str | None = None, **overrides
) -> dict:
    body = {
        "ping_id": ping_id or str(uuid.uuid4()),
        "vehicle_id": vehicle_id,
        "sequence_no": seq,
        "device_ts": datetime.now(UTC).isoformat(),
        "latitude": 13.4549,
        "longitude": -16.5790,
        "speed_kmh": 42.0,
        "heading_deg": 90.0,
    }
    body.update(overrides)
    return body


# ------------------------------------------------------------------------------------
# health and auth
# ------------------------------------------------------------------------------------


def test_health_reports_reachable_and_counts(client):
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert body["database"] == "reachable"
    # Seeded by init.sql; if these are missing the container did not initialise.
    assert body["counts"]["depots"] == 8
    assert body["counts"]["vehicles"] >= 1


@pytest.mark.parametrize(
    ("method", "path"),
    [("post", "/pings"), ("post", "/jobs"), ("patch", "/drivers/1"), ("delete", "/vehicles/1")],
)
def test_writes_require_a_token(client, method, path):
    # httpx's delete() takes no body, so the request is built explicitly rather than via the
    # per-verb helpers.
    assert client.request(method.upper(), path, json={}).status_code == 401


def test_health_needs_no_token(client):
    assert client.get("/health").status_code == 200


# ------------------------------------------------------------------------------------
# pings: idempotency is the property that matters
# ------------------------------------------------------------------------------------


def test_resending_the_same_ping_id_inserts_nothing(client):
    """At-least-once delivery is a fact of life; this is what stops it double-counting."""
    ping = _ping(seq=900_001)

    first = client.post("/pings", json={"pings": [ping]}, headers=AUTH).json()
    assert first == {"received": 1, "inserted": 1, "duplicates": 0}

    again = client.post("/pings", json={"pings": [ping]}, headers=AUTH).json()
    assert again == {"received": 1, "inserted": 0, "duplicates": 1}


def test_a_batch_mixing_new_and_duplicate_rows_reports_both(client):
    seen = _ping(seq=900_002)
    client.post("/pings", json={"pings": [seen]}, headers=AUTH)

    result = client.post(
        "/pings", json={"pings": [seen, _ping(seq=900_003), _ping(seq=900_004)]}, headers=AUTH
    ).json()
    assert result == {"received": 3, "inserted": 2, "duplicates": 1}


def test_pings_are_accepted_not_created(client):
    """202, because storing a reading is not the same as having processed it."""
    r = client.post("/pings", json={"pings": [_ping(seq=900_005)]}, headers=AUTH)
    assert r.status_code == 202


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("latitude", 900.0),
        ("latitude", -91.0),
        ("longitude", 181.0),
        ("speed_kmh", -5.0),
        ("heading_deg", 360.0),
    ],
)
def test_physically_impossible_values_are_rejected(client, field, value):
    """Rejected, not stored: these cannot be corrected downstream and would poison aggregates."""
    r = client.post("/pings", json={"pings": [_ping(**{field: value})]}, headers=AUTH)
    assert r.status_code == 422


def test_a_disagreeing_device_clock_is_accepted_and_recorded(client):
    """The opposite policy to the one above, and the distinction is the point.

    A device timestamp far from ours is evidence about that device, not corrupt data. Rejecting
    it would destroy the only signal that the device's clock is wrong.
    """
    ping = _ping(seq=900_006, device_ts=(datetime.now(UTC) + timedelta(hours=3)).isoformat())
    assert client.post("/pings", json={"pings": [ping]}, headers=AUTH).status_code == 202


def test_an_empty_batch_is_rejected(client):
    assert client.post("/pings", json={"pings": []}, headers=AUTH).status_code == 422


# ------------------------------------------------------------------------------------
# the server_ts override
# ------------------------------------------------------------------------------------


def test_server_ts_override_is_refused_by_default(client, monkeypatch):
    """The one timestamp nobody outside can influence must stay that way unless asked."""
    monkeypatch.delenv("FLEET_ALLOW_SERVER_TS_OVERRIDE", raising=False)
    r = client.post(
        "/pings",
        json={
            "pings": [_ping(seq=900_007)],
            "server_ts_override": "2020-01-01T00:00:00Z",
        },
        headers=AUTH,
    )
    assert r.status_code == 403
    assert "FLEET_ALLOW_SERVER_TS_OVERRIDE" in r.json()["detail"]


def test_server_ts_override_works_when_explicitly_enabled(client, monkeypatch):
    monkeypatch.setenv("FLEET_ALLOW_SERVER_TS_OVERRIDE", "true")
    r = client.post(
        "/pings",
        json={
            "pings": [_ping(seq=900_008)],
            "server_ts_override": "2020-01-01T00:00:00Z",
        },
        headers=AUTH,
    )
    assert r.status_code == 202


# ------------------------------------------------------------------------------------
# jobs: the state machine
# ------------------------------------------------------------------------------------


def _new_job(client) -> int:
    r = client.post(
        "/jobs",
        json={
            "pickup_depot_id": 1,
            "dropoff_depot_id": 8,
            "estimated_duration_minutes": 280,
        },
        headers=AUTH,
    )
    assert r.status_code == 201
    return r.json()["job_id"]


def test_a_new_job_starts_as_created_and_unassigned(client):
    r = client.post(
        "/jobs",
        json={"pickup_depot_id": 1, "dropoff_depot_id": 2, "estimated_duration_minutes": 30},
        headers=AUTH,
    ).json()
    assert r["status"] == "created"
    assert r["vehicle_id"] is None


def test_the_happy_path_transitions(client):
    job_id = _new_job(client)
    for to_status, extra in [
        ("assigned", {"vehicle_id": 2}),
        ("picked_up", {}),
        ("delivered", {}),
    ]:
        r = client.patch(
            f"/jobs/{job_id}/status", json={"to_status": to_status, **extra}, headers=AUTH
        )
        assert r.status_code == 200, r.text
        assert r.json()["status"] == to_status


@pytest.mark.parametrize("illegal", ["delivered", "picked_up"])
def test_skipping_a_step_is_rejected(client, illegal):
    job_id = _new_job(client)
    r = client.patch(f"/jobs/{job_id}/status", json={"to_status": illegal}, headers=AUTH)
    assert r.status_code == 409
    assert "cannot move a job" in r.json()["detail"]


def test_a_terminal_job_cannot_move_again(client):
    job_id = _new_job(client)
    client.patch(f"/jobs/{job_id}/status", json={"to_status": "cancelled"}, headers=AUTH)
    r = client.patch(
        f"/jobs/{job_id}/status", json={"to_status": "assigned", "vehicle_id": 2}, headers=AUTH
    )
    assert r.status_code == 409
    assert "terminal state" in r.json()["detail"]


def test_assigning_without_a_vehicle_is_rejected(client):
    job_id = _new_job(client)
    r = client.patch(f"/jobs/{job_id}/status", json={"to_status": "assigned"}, headers=AUTH)
    assert r.status_code == 422


def test_unknown_job_is_a_404(client):
    r = client.patch("/jobs/99999999/status", json={"to_status": "cancelled"}, headers=AUTH)
    assert r.status_code == 404


# ------------------------------------------------------------------------------------
# mutable entities
# ------------------------------------------------------------------------------------


def test_updating_a_driver_advances_updated_at(client):
    """The trigger, not the application, maintains updated_at.

    Phase 2's batch poller finds changes with `WHERE updated_at > watermark`. If a code path
    ever forgot to set it, the poller would silently skip those rows -- so it has to be
    guaranteed by the database, where it cannot be bypassed.
    """
    before = client.patch(
        f"/drivers/{TEST_DRIVER_IDS[0]}", json={"status": "active"}, headers=AUTH
    ).json()
    after = client.patch(
        f"/drivers/{TEST_DRIVER_IDS[0]}", json={"status": "inactive"}, headers=AUTH
    ).json()
    assert after["updated_at"] > before["updated_at"]
    assert after["status"] == "inactive"


def test_a_partial_update_leaves_other_fields_alone(client):
    original = client.patch(
        f"/drivers/{TEST_DRIVER_IDS[1]}", json={"status": "active"}, headers=AUTH
    ).json()
    patched = client.patch(
        f"/drivers/{TEST_DRIVER_IDS[1]}", json={"phone": "+2207654321"}, headers=AUTH
    ).json()
    assert patched["phone"] == "+2207654321"
    assert patched["full_name"] == original["full_name"]
    assert patched["home_depot_id"] == original["home_depot_id"]


def test_an_empty_update_is_rejected(client):
    assert client.patch(f"/drivers/{TEST_DRIVER_IDS[0]}", json={}, headers=AUTH).status_code == 422


def test_an_invalid_status_is_rejected_by_the_schema(client):
    r = client.patch(f"/drivers/{TEST_DRIVER_IDS[0]}", json={"status": "on holiday"}, headers=AUTH)
    assert r.status_code == 422


def test_unknown_driver_is_a_404(client):
    assert (
        client.patch("/drivers/999999", json={"status": "active"}, headers=AUTH).status_code == 404
    )


def test_deleting_a_vehicle_leaves_nothing_behind(client):
    """The delete that phase 2's batch poller will be unable to see.

    Uses a vehicle created for the purpose, so the fleet the simulator drives is untouched.
    """
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(config.oltp().dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            "insert into vehicles (vehicle_id, plate, capacity, home_depot_id) "
            "values (%s, 'TEST-9101', 8, 1) on conflict (vehicle_id) do nothing",
            (TEST_VEHICLE_ID,),
        )

    assert client.delete(f"/vehicles/{TEST_VEHICLE_ID}", headers=AUTH).status_code == 204
    assert client.delete(f"/vehicles/{TEST_VEHICLE_ID}", headers=AUTH).status_code == 404

    with psycopg.connect(config.oltp().dsn()) as conn, conn.cursor() as cur:
        cur.execute("select count(*) from vehicles where vehicle_id = %s", (TEST_VEHICLE_ID,))
        assert cur.fetchone()[0] == 0


def test_the_override_env_var_is_read_per_request_not_at_import(client, monkeypatch):
    """Guards against caching the flag at module load, which would make the test above lie."""
    monkeypatch.setenv("FLEET_ALLOW_SERVER_TS_OVERRIDE", "false")
    body = {"pings": [_ping(seq=900_010)], "server_ts_override": "2020-01-01T00:00:00Z"}
    assert client.post("/pings", json=body, headers=AUTH).status_code == 403
    monkeypatch.setenv("FLEET_ALLOW_SERVER_TS_OVERRIDE", "true")
    assert client.post("/pings", json=body, headers=AUTH).status_code == 202


def test_os_environ_is_the_source_of_truth():
    """Sanity check on the helper itself, independent of any request."""
    from app.main import _allow_server_ts_override

    os.environ.pop("FLEET_ALLOW_SERVER_TS_OVERRIDE", None)
    assert _allow_server_ts_override() is False
    os.environ["FLEET_ALLOW_SERVER_TS_OVERRIDE"] = "TRUE"
    assert _allow_server_ts_override() is True
    os.environ.pop("FLEET_ALLOW_SERVER_TS_OVERRIDE", None)
