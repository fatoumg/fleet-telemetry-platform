"""Probe the ACLED API: what it returns, in what format, and with what nullability.

Implements the documented flow from https://acleddata.com/api-documentation/getting-started
exactly:

    POST /oauth/token   grant_type=password, client_id=acled, scope=authenticated
    GET  /api/acled/read?_format=json&...   Authorization: Bearer <token>
    POST /oauth/token   grant_type=refresh_token          (when the 24 h token expires)

Same shape as explore_opensky.py: probe, save the raw response under a `_probe` envelope,
then profile the schema from real data rather than from the spec.

As of 2026-08-03 this account authenticates but is not authorised -- /oauth/token returns
200 and /oauth/userinfo accepts the token, while every /api/*/read returns
403 {"message":"Access denied"}. That is an account entitlement, not a request defect; it
was reproduced with curl, with cookie-session auth, and with the documentation's own
example. So the 403 path below reports a diagnosis instead of a stack trace, and the whole
script becomes a one-command retest once ACLED enables access.

    python scripts/explore_acled.py
    python scripts/explore_acled.py --no-save
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from aviation_conflict import config

TOKEN_URL = "https://acleddata.com/oauth/token"
USERINFO_URL = "https://acleddata.com/oauth/userinfo"
BASE_URL = "https://acleddata.com/api"
SAMPLE_DIR = Path(__file__).resolve().parent.parent / "data" / "samples"
TIMEOUT = 60

# Fields the data model leans on, so the profiler can call out anything missing outright.
# Names are from the ACLED endpoint reference, not guessed.
FIELDS_OF_INTEREST = (
    "event_id_cnty",
    "event_date",
    "time_precision",
    "disorder_type",
    "event_type",
    "sub_event_type",
    "latitude",
    "longitude",
    "geo_precision",
    "fatalities",
    "iso",
    "country",
    "timestamp",
)


# --------------------------------------------------------------------------------------
# authentication
# --------------------------------------------------------------------------------------


def request_token(
    session: requests.Session, creds: config.AcledCredentials
) -> dict[str, Any] | None:
    """Exchange email/password for a 24 h access token. Secrets are never printed."""
    response = session.post(
        TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "username": creds.username,
            "password": creds.password,
            "grant_type": "password",
            "client_id": "acled",
            "scope": "authenticated",
        },
        timeout=TIMEOUT,
    )
    if response.status_code != 200:
        print(f"  token request failed: HTTP {response.status_code} {response.text[:160]}")
        return None

    payload = response.json()
    print(
        f"  token_type={payload.get('token_type')} expires_in={payload.get('expires_in')}s "
        f"access_token=({len(payload.get('access_token', ''))} chars) "
        f"refresh_token={'present' if payload.get('refresh_token') else 'absent'}"
    )
    return payload


def refresh_token(session: requests.Session, refresh: str) -> dict[str, Any] | None:
    """Trade a refresh token (14 day life) for a fresh access token."""
    response = session.post(
        TOKEN_URL,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"refresh_token": refresh, "grant_type": "refresh_token", "client_id": "acled"},
        timeout=TIMEOUT,
    )
    if response.status_code != 200:
        print(f"  refresh failed: HTTP {response.status_code}")
        return None
    print("  refresh grant OK -- new access token issued")
    return response.json()


def check_userinfo(session: requests.Session, token: str) -> None:
    """Confirm the token is honoured somewhere.

    This is what separates "bad token" from "token fine, account not entitled". Without it a
    403 on /api/acled/read is ambiguous.
    """
    response = session.get(
        USERINFO_URL, headers={"Authorization": f"Bearer {token}"}, timeout=TIMEOUT
    )
    if response.status_code == 200:
        body = response.json()
        print(f"  /oauth/userinfo -> 200, token resolves to uid {body.get('sub')}")
    else:
        print(f"  /oauth/userinfo -> {response.status_code} (token may be invalid)")


# --------------------------------------------------------------------------------------
# probes
# --------------------------------------------------------------------------------------


def build_probes() -> list[dict[str, Any]]:
    """Small, legal queries that together characterise the endpoint.

    Deliberately tiny: the default row cap is 5,000 and the per-account call quota is
    unconfirmed (acledR warns of "2/year for a standard account"), so this spends as few
    calls as possible while still measuring the schema.
    """
    return [
        {
            "name": "acled_read_smoke",
            "path": "/acled/read",
            "params": {"_format": "json", "limit": 5},
            "rationale": "Minimal call. Establishes the response envelope and field list.",
        },
        {
            "name": "acled_read_conflict_zone",
            "path": "/acled/read",
            "params": {
                "_format": "json",
                "country": "Iraq",
                "event_date": "2026-01-01|2026-01-31",
                "event_date_where": "BETWEEN",
                "limit": 200,
            },
            "rationale": "A real analysis slice: one country, one month. Feeds null-rate stats.",
        },
        {
            "name": "acled_read_csv",
            "path": "/acled/read",
            "params": {"_format": "csv", "country": "Iraq", "limit": 5},
            "rationale": "Confirms the CSV export path and that it agrees with JSON.",
        },
        {
            "name": "deleted_read",
            "path": "/deleted/read",
            "params": {"_format": "json", "limit": 5},
            "rationale": "Deletions matter for SCD Type 2 -- a retracted event must not linger.",
        },
    ]


def run_probe(session: requests.Session, token: str, probe: dict[str, Any]) -> dict[str, Any]:
    url = BASE_URL + probe["path"]
    started = datetime.now(UTC)
    try:
        response = session.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=probe["params"],
            timeout=TIMEOUT,
        )
    except requests.RequestException as exc:
        return {**probe, "error": f"{type(exc).__name__}: {exc}"}

    content_type = response.headers.get("content-type", "")
    record: dict[str, Any] = {
        "_probe": {
            **probe,
            "url": response.url,
            "status_code": response.status_code,
            "content_type": content_type,
            "bytes": len(response.content),
            "elapsed_ms": round(response.elapsed.total_seconds() * 1000, 1),
            "captured_at": started.isoformat(timespec="seconds"),
        }
    }
    if "json" in content_type:
        try:
            record["response"] = response.json()
        except ValueError:
            record["response"] = response.text
    else:
        record["response"] = response.text
    return record


# --------------------------------------------------------------------------------------
# profiling
# --------------------------------------------------------------------------------------


def _type_name(value: Any) -> str:
    return "null" if value is None else type(value).__name__


def profile_rows(rows: list[dict[str, Any]], label: str) -> None:
    """Measure types and null rates from the data, the way the OpenSky doc did."""
    if not rows:
        print(f"  {label}: no rows to profile")
        return

    print(f"\n  {label}: {len(rows)} rows, {len(rows[0])} fields")
    keys = list(rows[0].keys())
    width = max(len(k) for k in keys)
    print(f"    {'field':<{width}}  {'types':<22} {'null%':>6}  example")
    for key in keys:
        values = [row.get(key) for row in rows]
        types = Counter(_type_name(v) for v in values)
        nulls = sum(1 for v in values if v is None or v == "")
        example = next((repr(v)[:34] for v in values if v not in (None, "")), "-")
        marker = " <-- of interest" if key in FIELDS_OF_INTEREST else ""
        print(
            f"    {key:<{width}}  {','.join(sorted(types)):<22} "
            f"{nulls / len(values) * 100:5.1f}%  {example}{marker}"
        )

    missing = [f for f in FIELDS_OF_INTEREST if f not in keys]
    if missing:
        print(f"    MISSING fields the data model expects: {', '.join(missing)}")

    # Precision codes decide whether the +/-12 h and 25 km windows mean anything at all,
    # so report their distribution rather than assuming everything is precision 1.
    for field in ("time_precision", "geo_precision"):
        if field in keys:
            dist = Counter(row.get(field) for row in rows)
            print(
                f"    {field} distribution: {dict(sorted(dist.items(), key=lambda kv: str(kv[0])))}"
            )

    for field in ("event_type", "disorder_type"):
        if field in keys:
            dist = Counter(row.get(field) for row in rows)
            print(f"    {field} values ({len(dist)}): {dict(dist.most_common(8))}")


def diagnose_denial(record: dict[str, Any]) -> None:
    """Explain a 403 rather than letting it look like a bug in this script."""
    probe = record["_probe"]
    print(f"\n  {probe['name']}: HTTP {probe['status_code']} -- {str(record['response'])[:80]}")
    print("    The token is valid (see /oauth/userinfo above); this endpoint denies the")
    print("    account. Reproduced with curl, cookie-session auth, and the documented")
    print("    example. Only ACLED can grant data access -- contact access@acleddata.com.")


# --------------------------------------------------------------------------------------
# persistence
# --------------------------------------------------------------------------------------


def save(record: dict[str, Any]) -> None:
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    path = SAMPLE_DIR / f"acled_{record['_probe']['name']}.json"
    path.write_text(json.dumps(record, indent=1), encoding="utf-8")
    print(f"    saved {path.relative_to(SAMPLE_DIR.parent.parent)}")


def save_manifest(records: list[dict[str, Any]]) -> None:
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    (SAMPLE_DIR / "_manifest_acled.json").write_text(
        json.dumps(
            {
                "captured_at": datetime.now(UTC).isoformat(timespec="seconds"),
                "probes": [r["_probe"] for r in records],
            },
            indent=1,
        ),
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-save", action="store_true", help="probe without writing samples")
    args = parser.parse_args(argv)

    creds = config.acled()
    if creds is None:
        print("no ACLED credentials -- set ACLED_USERNAME and ACLED_PASSWORD in .env")
        print("see .env.example")
        return 1

    session = requests.Session()
    print("authenticating")
    token_payload = request_token(session, creds)
    if token_payload is None:
        return 1
    access_token = token_payload["access_token"]
    check_userinfo(session, access_token)

    records: list[dict[str, Any]] = []
    denied = 0
    print("\nprobing")
    for probe in build_probes():
        record = run_probe(session, access_token, probe)
        if "error" in record:
            print(f"  {probe['name']}: {record['error']}")
            continue
        records.append(record)

        status = record["_probe"]["status_code"]
        if status == 403:
            denied += 1
            diagnose_denial(record)
            continue

        meta = record["_probe"]
        print(f"  {meta['name']}: {status} {meta['content_type'][:24]} {meta['bytes']}B")
        if not args.no_save:
            save(record)

        body = record["response"]
        if isinstance(body, dict):
            print(f"    envelope keys: {sorted(body.keys())}")
            rows = body.get("data")
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                profile_rows(rows, meta["name"])
        elif isinstance(body, str) and meta["name"].endswith("csv"):
            header = body.splitlines()[0] if body else ""
            print(f"    csv header ({len(header.split(','))} cols): {header[:150]}")

    if records and not args.no_save:
        save_manifest(records)

    print()
    if denied == len(records) and denied:
        print(f"all {denied} endpoints returned 403 -- account authenticated but not authorised")
        print("nothing to fix here; re-run this script once ACLED enables API data access")
        return 2
    print(f"{len(records) - denied}/{len(records)} probes returned data")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
