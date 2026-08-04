# Aviation Conflict Analytics Platform — Design

Date: 2026-08-03
Status: approved, pending implementation plan
Supersedes: nothing. Implements and corrects `Aviation_Conflict_Analytics_Data_Modeling.pdf`.

---

## 1. Purpose

Measure whether geopolitical violence changes commercial aviation behaviour, while isolating
weather and day-of-week as confounders.

Core metric, from the source spec §6:

```text
ADI (%) = (observed_flight_count − baseline_flight_count) / baseline_flight_count × 100
```

The engineering difficulty is not the metric. It is that `baseline_flight_count` is an estimate
of a counterfactual — what traffic *would* have been absent the conflict — which no source
system supplies and no test can verify. Most of this design exists to make that estimate
honest and to make its failure modes visible rather than silent.

## 2. Scope decision

**Analyse the periphery of conflict and acute onset events. Not chronic war zones.**

This overrides the source spec's implied global scope, on measured evidence. Authenticated
OpenSky snapshots on 2026-08-03 at 13:38Z and 14:37Z, aircraft per million km²:

| Region | Aircraft | Density /Mkm² |
|---|---:|---:|
| Switzerland (control) | 144 | 1,856 |
| Israel / Lebanon | 33 | 303 |
| Syria + N. Iraq | 15 | 46 |
| E. Ukraine / Donbas | 0 | 0 |
| Yemen | 0 | 0 |
| Sudan | 0 | 0 |

Verified not to be an artifact three ways: a wide box (5–45°N, 25–60°E) returned 541 aircraft,
and filtering that same snapshot client-side reproduced the zeros; the snapshot was 16:37–17:37
local, i.e. peak hours; and a second snapshot an hour earlier matched.

Consequence: where `observed = 0` and `baseline = 0`, ADI is `0/0` — undefined. The airspace
this platform was designed to study has no traffic left to divert; the effect saturated before
our observation window opened. Raw samples are preserved in
`data/samples/states_*.json` with `_manifest_conflict_coverage.json`.

Three things remain measurable, and the model targets them:

1. **Displacement** — traffic relocating *into* cells adjacent to conflict. Positive ADI.
2. **Acute onset** — airspace transitioning from busy to closed, where there is traffic to lose.
3. **Overflight altitude** — Syria/N. Iraq retains corridor traffic at cruise, so altitude
   behaviour is measurable where volume is not.

## 3. Stack

Separating what the source spec dictates from what this design invents, so future readers know
which choices are negotiable.

| Layer | Choice | Origin |
|---|---|---|
| Database | PostgreSQL 16 | **spec** — `BIGSERIAL`, `DATE_TRUNC`, `REFERENCES`, `NUMERIC(p,s)` |
| Spatial | PostGIS | **spec** §4/§7 — `GEOMETRY(POINT,4326)`, GIST, `ST_DWithin` |
| Time-series partitioning | TimescaleDB | **spec** §4 — "TimescaleDB hypertable partitioned by `flight_ts`" |
| Architecture | Bronze / Silver / Gold | **spec** §1 |
| Modelling | Kimball, SCD Type 2 | **spec** §5 |
| Transform | dbt (`dbt-postgres`) | implied — spec §8's `raw_→stg_→dim_/fact_→mart_` is dbt's own convention |
| Orchestration | **Apache Airflow 3** | invented — spec is silent |
| Extract/Load | Python 3.13 | invented |
| Packaging | uv + `pyproject.toml` | invented |
| Quality | pytest, ruff, pre-commit, GitHub Actions | invented |

Division of labour is the standard **EL in Python, T in SQL, orchestration separate**. Python
only lands raw bytes. Every business rule lives in dbt where it is diffable, testable and
documented.

Deployment is Docker Compose: one Postgres container for the analytics warehouse
(`timescale/timescaledb-ha`, which bundles PostGIS), plus Airflow's own webserver, scheduler
and **separate** metadata database. Airflow metadata must not share the analytics warehouse.

Airflow specifics:
- TaskFlow API (`@dag` / `@task`) rather than classic operators.
- dbt invoked as a single `dbt build` task initially. `astronomer-cosmos` renders each dbt model
  as its own Airflow task and is the eventual upgrade, but it is not needed to ship — YAGNI.
- Secrets: `.env` mounted into the containers and read through `aviation_conflict.config`, so
  there is one credential source rather than duplicating into Airflow Connections.

## 4. Repository layout

```text
├── docker/
│   ├── docker-compose.yml            # warehouse + airflow
│   └── warehouse/init.sql            # CREATE EXTENSION postgis, timescaledb
├── src/aviation_conflict/            # installable package
│   ├── config.py                     # moved from scripts/
│   ├── ingest/                       # Bronze: API → files
│   │   ├── opensky.py
│   │   ├── acled.py
│   │   └── era5.py
│   ├── load/                         # Bronze files → bronze.* tables
│   └── grid/                         # dim_grid_cells generation
├── dags/                             # Airflow DAGs
├── dbt/
│   ├── models/staging/               # Silver
│   ├── models/gold/                  # dim_*, fact_*
│   ├── models/marts/                 # mart_*
│   ├── seeds/                        # iso numeric→alpha2 lookup
│   └── tests/                        # singular tests (grain assertions)
├── scripts/explore_*.py              # API recon; source of truth on source behaviour
├── tests/
├── data/bronze/                      # git-ignored
└── docs/
```

`scripts/config.py` moves to `src/aviation_conflict/config.py` so ingest modules can import it;
the `explore_*.py` scripts import from the package. That is the only refactor to existing code.

## 5. Data flow

```text
OpenSky ─┐
ACLED   ─┼→ data/bronze/*.json.gz ─→ bronze.* ─→ silver.stg_* ─→ gold.dim_*/fact_* ─→ marts.*
ERA5    ─┘   (Python, append-only)   (raw JSONB)   (dbt)            (dbt)               (dbt)
```

Bronze is persisted as **files and** loaded to `bronze.*` raw tables: files are the durable
record, tables make them queryable. Bronze is append-only and never edited.

Bronze is not a convenience here. `/states/all` returns the current snapshot only — anonymous
access has no history and credentials give one hour. An uncaptured snapshot is unrecoverable.
Bronze is the only copy that will ever exist, and SCD Type 2 on `dim_conflicts` is impossible
without it, since detecting a revised `fatalities` requires both responses.

Silver upserts on verified business keys, which makes re-runs idempotent:

| Silver model | Business key |
|---|---|
| `stg_opensky_states` | `(icao24, time_position)` |
| `stg_acled` | `event_id_cnty` |
| `stg_weather` | `(era5_grid_cell, observation_ts)` |

Layer boundary rule: **Silver fixes what nobody can reasonably disagree about; Gold holds every
judgment call.** Analysis parameters are the volatile ones, so they live as late as possible.
Changing the 25 km buffer must not require reprocessing flight rows.

## 6. Verified source contracts

Measured, not taken from the spec. Full detail in `docs/opensky-api-reference.md`.

### OpenSky — verified
- Rows are **bare positional arrays**, no field names. Element 5 is longitude, element 6 is
  latitude — the reverse of the spec's "latitude, longitude" phrasing.
- `velocity` is **ground speed**, not airspeed as spec §3 states. Ground speed is contaminated
  by wind, the confounder being controlled for.
- Two altitudes: `baro_altitude` and `geo_altitude`, observed 114 m apart on one aircraft and a
  median 572 m apart across the sample.
- Null rates: `baro_altitude` 19.0%, `geo_altitude` 24.8%, `squawk` 16.8%, `sensors` 100%.
- `callsign` is 8-char space-padded; one sample returned an empty string.
- `origin_country` derives from the ICAO24 range, not the operator.
- Update cadence is **irregular** — observed gaps of 8 s, 39 s and 82 s on one aircraft, not the
  spec's "1–5 minute" claim.
- Auth: OAuth2 client-credentials. 4,000 credits/day; 1 h history at 5 s resolution.

### ACLED — contract established, access blocked
- Live endpoint is `https://acleddata.com/api/acled/read`. The legacy host `api.acleddata.com`
  no longer resolves.
- Auth is an OAuth **password** grant (`grant_type=password`, `client_id=acled`,
  `scope=authenticated`). API keys are retired. Access token 24 h, refresh token 14 days.
- `event_date` is a **DATE** ("Year-Month-Day"), not the spec's `TIMESTAMP`.
- `time_precision` ∈ {1,2,3}: exact day / within a week / within a month.
- `geo_precision` ∈ {1,2,3}: exact / nearest town / nearest large region.
- Field is `fatalities`, not the spec's `deaths`.
- `iso` is a **3-digit numeric** ISO-3166 code, not the spec's `CHAR(2)`.
- `timestamp` is the unix time of last upload — the SCD Type 2 change-detection key.
- Default 5,000 rows/request with pagination. ~87 calls for a two-year backfill.
- **Blocked:** account uid 211294 authenticates (`/oauth/token` 200, `/oauth/userinfo` 200) but
  every `/api/*/read` returns `403 {"message":"Access denied"}`. Reproduced with curl, with
  cookie-session auth via `/user/login`, with `_format=json` and `csv`, across the `acled`,
  `deleted` and `cast` endpoints, and with the documentation's own example verbatim. This is an
  account entitlement, not a request defect. Awaiting `access@acleddata.com`.
- Unverified: `acledR` warns of *"2/year for a standard account"*. If literal, API ingestion is
  not viable and bulk export is required instead. Must be confirmed.

### ERA5 — verified end-to-end
- **Not NOAA.** ERA5 is ECMWF's, via the Copernicus Climate Data Store. Spec §3/§8 misattribute
  it. Verified by authenticating against `cds.climate.copernicus.eu` and downloading a file.
- Format is **GRIB or NetCDF only** — no JSON/CSV. Requests are queued jobs, not paginated reads.
- `visibility_km` **has no source.** All 262 single-level variables were searched; the only
  matches are `uv_visible_albedo_*`, which is radiation albedo. The column is dropped.
- Four `dim_weather` columns are **derived, not ingested**:

  | Spec column | ERA5 variable | Conversion |
  |---|---|---|
  | `wind_speed_ms` | `10m_u_component_of_wind`, `10m_v_component_of_wind` | `sqrt(u²+v²)` |
  | `wind_direction_deg` | same two | `atan2`, plus a convention decision |
  | `temp_c` | `2m_temperature` (Kelvin) | `− 273.15` |
  | `precip_mm` | `total_precipitation` (metres, accumulated) | `× 1000` |

- **Winds aloft are available.** `reanalysis-era5-pressure-levels` offers 16 variables across 37
  levels including **250 hPa** (≈ FL350): `u_component_of_wind`, `v_component_of_wind`,
  `temperature`, `vertical_velocity`, `geopotential`, `fraction_of_cloud_cover`.
- Latency is **~6 days**, not the spec's 1–3 months. Catalogue extent ran to 2026-07-28 on
  2026-08-03, `cads:update_frequency: Daily`. Whether recent values are preliminary ERA5T and
  later revised is **unverified** and matters for reproducibility, not latency.
- Licence `cc-by` must be accepted per-account or downloads fail after the request is accepted.

## 7. Corrections to the source data model

Each is a deliberate departure from the PDF, with its reason.

### Silver — objective normalisation
| Change | Reason |
|---|---|
| Strip padded `callsign`; lowercase `icao24` | Wire format artifacts |
| `velocity` → `ground_speed_ms` | It is ground speed. The spec's name invites a wrong analysis |
| Keep **both** `baro_altitude_m` and `geo_altitude_m` | 572 m median disagreement exceeds flight-level separation; collapsing to one `altitude` destroys information |
| ERA5 unit conversions per §6 | Derived values, not renames |
| `iso` numeric → alpha-2 via dbt seed | Spec's `CHAR(2)` does not match the source |
| **Carry** `time_precision`, `geo_precision` | Never silently drop imprecise events; Gold decides |

### Gold and marts — model changes
| Change | Reason |
|---|---|
| **Drop `dim_weather.visibility_km`** | No source exists in ERA5 |
| **Add 250 hPa pressure-level weather** | Surface wind at 10 m is near-irrelevant to an aircraft at 10,600 m. As specified, `fact_weather_match` would control for weather the aircraft never met, then report the residual as a conflict effect |
| **Split `mart_adi`** into `fact_cell_day_traffic` at `(grid_cell, day)` plus `bridge_conflict_cell_day` | Spec grain is `(conflict, grid_cell, day)` but no measure varies with `conflict_id`. Five ACLED events in one cell-day duplicate the same flight count five times; any `SUM` inflates 5× |
| **`mart_baseline_traffic` grain → `(grid_cell_id, day_of_week, window_end_date)`** | Spec grain `(grid_cell, day_of_week)` allows 36k × 7 = 252k rows maximum, one slot per cell per weekday forever. A "rolling 30-day average" has nowhere to roll |
| **ADI denominator guard** — baseline stays `NUMERIC`; ADI returns `NULL` plus a reason code below a minimum baseline (default 20 flights) | Spec casts baseline to `INT` in `mart_adi`. A true baseline of 0.4 rounds to 0 → division by zero. A baseline of 1 swings ADI ±100% on one aircraft. Low-traffic airspace is exactly where conflicts occur |
| **`observed_flight_count` = `COUNT(DISTINCT icao24)`** | A helicopter loitering an hour emits ~300 state vectors; an airliner crossing the same cell emits ~10. Counting rows makes the helicopter look like 30× the traffic. The spec never states the counting rule |
| **New `receiver_coverage` per cell-day** | The observation-bias threat. `observed_flight_count` is "what the volunteer receiver network heard", not "what flew". Conflict degrades coverage — receivers lose power, GPS jamming corrupts positions, transponders go dark — and that bias points the *same direction* as the effect, inflating results. A coverage collapse must read as "no data", not "no flights" |

### Scope-specific additions
| Addition | Reason |
|---|---|
| `dim_grid_cells` neighbour adjacency | Makes "cells adjacent to conflict" a join, not runtime geometry |
| `conflict_onset` classification — first significant event in a cell after a quiet period | Separates acute events from chronic zones, where the effect predates observation |
| Control-cell selection | Supports difference-in-differences: comparing a cell to its own past contaminates the result with anything that changed globally. Subtracting a matched control cancels it |

### Deliberately retained
The ±12 h exposure window and 25 km buffer are kept as spec'd, as **Gold-layer parameters**, not
constants. Spec §9 defends 25 km from a 10–100 km threat range, which concedes it is a judgment
call. They will change; changing them must cost one mart rebuild, not a Silver reprocess.

Caveat to record: for `time_precision` 2–3 events, timing is known only to the week or month,
so a ±12 h window around them is not well founded. Those events must be excluded from
window-sensitive analysis, and the exclusion counted, not silently applied.

## 8. Sampling design and credit budget

`observed_flight_count` accuracy depends on polling relative to cell transit time.

A 1°×1° cell is 111 km north–south, but only ~76 km east–west at 47°N and ~91 km at 33°N —
longitude cells narrow with latitude. An airliner at 900 km/h crosses 76 km in **~5 minutes**,
and a corner clip takes seconds.

So **no polling interval captures every aircraft**, and this is not solvable by polling faster
within our credit budget. The undercount is real and must be stated rather than engineered away.

What makes it tolerable: ADI is a *ratio* of observed to baseline, and the baseline is computed
from the same source at the same cadence. A systematic undercount largely cancels. That
robustness holds **only while the sampling interval is constant** — so the interval is recorded
per capture in Bronze, and any change to it invalidates cross-period comparison. This is a
correctness constraint, not a tuning knob.

**Default: 5-minute polling.** Cost, using OpenSky's area-banded pricing (≤25 deg² = 1 credit,
25–400 deg² = 2; exactly-25 treated conservatively as 2):

| Region | deg² | Credits |
|---|---:|---:|
| Switzerland (control) | 9.2 | 1 |
| E. Ukraine | 20 | 1 |
| Israel/Lebanon | 10.5 | 1 |
| Syria + N. Iraq | 32 | 2 |
| Yemen | 25 | 2 |
| Sudan | 30 | 2 |
| **per sweep** | | **9** |

288 sweeps/day × 9 = **2,592 credits/day** against a 4,000/day allowance. Fits, with headroom
for backfill and ad-hoc queries. Region set and interval are configuration, and the budget must
be re-checked when either changes.

## 9. Testing

| Scope | Approach |
|---|---|
| Python | pytest — config resolution, response parsing, unit conversions |
| dbt generic | `not_null`, `unique`, `relationships`, `accepted_values` |
| **Grain assertions** | Singular test per fact and mart: unique on its declared grain |
| **No silent nulls** | ADI is either a number or carries a populated reason code |
| Coverage sanity | Alert when a cell's `receiver_coverage` drops sharply — distinguishes "no flights" from "no data" |
| CI | GitHub Actions: ruff, pytest, `dbt build` against a throwaway Postgres service |

Grain assertions matter most. Fan-out silently inflates every downstream number and is invisible
without an explicit uniqueness test — it is the bug class §7 removed from `mart_adi`, and the
test is what stops it returning.

Secrets must never reach logs: `config.describe()` reports presence and length only, and a test
asserts no secret appears in its output.

## 10. Build phases

Thin vertical slices. Something works end to end at Phase 2, not Phase 6.

| Phase | Deliverable | Blocked by |
|---|---|---|
| **0** | OpenSky capture on a schedule → `data/bronze/`. Standalone script + OS scheduler, no warehouse | — |
| 1 | Docker Compose warehouse; `bronze.*` loading; `dim_grid_cells` with adjacency | — |
| 2 | dbt Silver for OpenSky + grain tests. First end-to-end slice | 1 |
| 3 | ERA5 ingestion → Silver, including 250 hPa | 1 |
| 4 | ACLED → Silver + SCD Type 2 | **ACLED access** |
| 5 | Gold facts, then marts with corrected grains | 2, 3, 4 |
| 6 | Airflow wraps all of it; CI | 5 |

**Phase 0 is first and urgent.** OpenSky history is one hour with credentials and zero without.
Every day without scheduled capture is data that cannot be recovered — the only irreversible
constraint in the project. It deliberately precedes Airflow, because standing up a scheduler
should not gate the start of data collection.

## 11. Open items

| Item | Owner | Blocks |
|---|---|---|
| ACLED API data access (uid 211294) | ACLED support | Phase 4 |
| Confirm ACLED call quota — is "2/year" literal? | ACLED support | Whether Phase 4 is API or bulk export |
| Is recent ERA5 preliminary ERA5T and later revised? | verify via CDS | Reproducibility of weather-adjusted baselines |
| `pip install xarray netCDF4` | us | Phase 3 |
| Rewrite `README.md` | us | — |

`README.md` currently describes a different project — "aircraft proximity and loss-of-separation
events", i.e. two aircraft flying too close. This platform measures geopolitical conflict impact.
Different question, different data, different maths.
