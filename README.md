# aviation-conflict-analytics

Measures whether geopolitical violence changes commercial aviation behaviour — do aircraft
avoid, or fly higher over, airspace where conflict is happening? — while isolating weather and
day-of-week as confounders.

Three independent sources are joined on a shared `(grid_cell, hour)` coordinate system:

| Source | Provides | Access |
| --- | --- | --- |
| [OpenSky Network](https://opensky-network.org/) | Aircraft state vectors | working |
| [ACLED](https://acleddata.com/) | Conflict events | **blocked** — see below |
| [ERA5](https://cds.climate.copernicus.eu/) (ECMWF/Copernicus) | Weather, surface and 250 hPa | working |

Current status: **scaffolding.** Source contracts are established and verified against live
APIs; no pipeline is built yet. Start with the design spec.

## Read these first

| Path | What it is |
| --- | --- |
| [docs/superpowers/specs/2026-08-03-platform-design.md](docs/superpowers/specs/2026-08-03-platform-design.md) | **The design.** Stack, scope decision, 10 corrections to the source data model, build phases |
| [docs/opensky-api-reference.md](docs/opensky-api-reference.md) | Every OpenSky field with measured type, unit and null rate, plus 11 gotchas |

Two findings from the spec worth knowing before reading any code:

- **OpenSky sees no traffic over active war zones.** Measured density per million km²:
  Switzerland 1,856; Israel/Lebanon 303; Syria/N. Iraq 46; **Donbas, Yemen and Sudan 0.** Where
  observed and baseline are both zero, the headline metric is `0/0`. The platform therefore
  targets the *periphery* of conflict and *acute onset* events, not chronic war zones.
- **The hardest column has no source system.** `baseline_flight_count` estimates a
  counterfactual — traffic that would have occurred absent the conflict. Nothing can verify it,
  so a wrong baseline produces a clean, plausible, wrong answer. Much of the design exists to
  make that failure visible.

## Layout

```text
docker/            warehouse: postgres + postgis + timescaledb
src/aviation_conflict/
  config.py        credential resolution (env -> .env -> credentials.json)
  ingest/          Bronze: API -> data/bronze/ (append-only)
  load/            Bronze files -> bronze.* tables
  grid/            dim_grid_cells + neighbour adjacency
dbt/               Silver, Gold, marts -- all business logic lives here
dags/              Airflow DAGs (phase 6)
scripts/           API exploration; the source of truth on how each API behaves
tests/
```

Division of labour: **Extract/Load in Python, Transform in SQL.** Python only lands raw bytes;
every business rule is a dbt model, so it stays diffable and testable.

## Setup

```bash
pip install -e ".[dev]"          # or: uv sync --extra dev
cp .env.example .env             # then fill in credentials
python -m aviation_conflict.config   # shows what resolved, without printing secrets
pytest
```

Bring up the warehouse:

```bash
docker compose -f docker/docker-compose.yml up -d
```

The compose file defaults match `.env.example`, so it runs with no `.env` at all. `init.sql`
creates the PostGIS and TimescaleDB extensions plus one schema per medallion layer
(`bronze`, `silver`, `gold`, `marts`).

## Credentials

`.env` is git-ignored; `.env.example` documents how to obtain each credential. Resolution order
is **environment variable → `.env` → `credentials.json`** (the last is a legacy OpenSky
fallback and can be deleted).

Secrets are never printed — `config.describe()` reports presence and length only, and a test
asserts no secret appears in its output.

- **OpenSky** — OAuth2 client credentials. 4,000 credits/day, 1 h of history at 5 s resolution.
  Anonymous access works but gives 400 credits/day and **no history**.
- **ERA5** — a Copernicus CDS token, plus per-account acceptance of the `cc-by` licence.
  Without the licence, requests are accepted and then fail at download.
- **ACLED** — myACLED email and password (API keys are retired). **Currently blocked:** the
  account authenticates (`/oauth/token` 200, `/oauth/userinfo` 200) but every `/api/*/read`
  returns `403 Access denied`. Reproduced with curl, cookie-session auth, both response
  formats, all three endpoints, and ACLED's own documented example — so it is an account
  entitlement, not a request defect. `scripts/explore_acled.py` implements the full documented
  flow and exits `2` until access is granted, making it a one-command retest.

## Exploration scripts

These established the source contracts and remain the place to re-verify them.

```bash
python scripts/explore_opensky.py            # ~3 of 400 anonymous credits
python scripts/explore_opensky.py --global   # adds the unbounded snapshot (+4 credits)
python scripts/explore_acled.py              # exits 2 while access is blocked
```

Captured responses land in `data/samples/` (git-ignored). They are raw, enveloped with the URL,
status and capture time — the Bronze pattern in miniature.

`temp.py` and `temp.ipynb` are the original scratch spike, git-ignored and superseded.

## Why Bronze is not optional here

`/states/all` returns only the current snapshot. Anonymous access has no history; credentials
give one hour. **A snapshot you did not capture is gone forever.** Bronze is the only copy that
will ever exist, and SCD Type 2 on `dim_conflicts` is impossible without it, since detecting a
revised casualty count requires both API responses.

This is why capture is phase 0, ahead of the warehouse and the orchestrator: every day without
scheduled capture is data that cannot be recovered.
