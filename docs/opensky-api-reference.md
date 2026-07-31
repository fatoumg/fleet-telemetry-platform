# OpenSky Network REST API — what it returns and in what format

Base URL: `https://opensky-network.org/api`

Every figure here was **captured live on 2026-07-28**, both anonymously and with OAuth2 credentials.
Regenerate with `python scripts/explore_opensky.py` (add `--anonymous` to reproduce the
unauthenticated column). Nothing in this document is copied from the spec without being checked
against a real response — where the API and its own documentation disagree, that is called out.

Sample sizes: `/states/all` over a bounding box covering Switzerland and the Alps (137–145 aircraft
per call) and one global snapshot (**12,996** aircraft); `/flights/all` over a 1-hour window (**863**
flights); `/flights/arrival` and `/flights/departure` at Frankfurt (EDDF) over 24 hours (**664** and
**590** flights); `/tracks/all` live.

---

## 1. Reachability

| Endpoint | Anonymous | Authenticated |
| --- | --- | --- |
| `GET /states/all` | **200** | **200** |
| `GET /states/all?time=…` (past) | refused | **200** — up to 1 h back |
| `GET /tracks/all?icao24=…&time=0` | **200** | **200** |
| `GET /states/own` | **403**, *empty body, no `content-type`* | **200** but `states: null` — see G12 |
| `GET /flights/all` | **403** `text/plain` — `You cannot access historical flights` | **200** JSON array |
| `GET /flights/aircraft` | **403**, same | **200** JSON array |
| `GET /flights/arrival` | **403**, same | **200** JSON array |
| `GET /flights/departure` | **403**, same | **200** JSON array |

So anonymous access reaches live state vectors and live tracks, and nothing else. Credentials unlock
the four `/flights/*` endpoints and up to an hour of state history.

The `/flights/*` probes deliberately used a legal window well inside the documented caps, so the
anonymous 403 is unambiguously about permission, not a malformed request.

Note the two anonymous 403 flavours differ: `/states/own` returns a genuinely empty body with **no**
`content-type` header at all, while `/flights/*` returns a plain-text message. Code that branches on
the body will meet both.

### Authenticating

Put a `credentials.json` in the project root (it is git-ignored) containing `clientId` and
`clientSecret`; `scripts/explore_opensky.py` picks it up automatically. Observed token lifetime:
**1800 s**, `token_type: Bearer`. See §2 for the exchange.

---

## 2. Authentication

Basic auth with username/password was **removed on 2026-03-18**. The only supported scheme is the
OAuth2 client-credentials flow:

```bash
curl -X POST "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=$CLIENT_ID" -d "client_secret=$CLIENT_SECRET"
```

Pass the result as `Authorization: Bearer <token>`. Tokens expire after **30 minutes**
(`expires_in: 1800`, confirmed); an expired token yields `401`. Accounts created since mid-March 2025
*must* use this flow.

**What credentials unlock** — all confirmed on this account:

- All four `/flights/*` endpoints.
- **State history**: `?time=` up to 1 hour in the past, at **5-second** resolution (anonymous is
  current-time only at 10 s). This is the one that matters most for conflict work, because it turns a
  single snapshot into a time series without you having to poll and persist. Requesting
  `now − 1800 s` returned a snapshot stamped at exactly that second.
- **4,000 credits/day** on this (standard) account, confirmed by the header reading 3999 on a fresh
  run, vs 400 anonymous. Active feeders (a receiver with ≥30% uptime) get 8,000.

Note that `/states/own` is *not* unlocked by credentials alone — it needs a registered ADS-B
receiver, and without one it returns a 200 with `states: null` (G12).

---

## 3. `/states/all` — state vectors

### Response envelope

```json
{ "time": 1785255585, "states": [ [ /* 18 elements */ ], ... ] }
```

Top-level keys are exactly `time` and `states`. `time` is an **`int`** epoch-seconds value.

`states` is an **array of bare arrays** — position is the only thing identifying a field. There are
no field names on the wire.

### Fields — observed

Nullability and type columns are measured over the 137-aircraft sample, not copied from the spec.

| # | Field | Type(s) observed | Unit | Null rate | Meaning |
| --- | --- | --- | --- | --- | --- |
| 0 | `icao24` | `str` | — | 0% | Transponder ICAO 24-bit address, **lowercase hex**. The only reliable aircraft key. |
| 1 | `callsign` | `str` | — | 0%¹ | 8-char space-padded, e.g. `'T7AFZ   '`. See gotcha G1. |
| 2 | `origin_country` | `str` | — | 0% | Inferred from the ICAO24 range, **not** the operator's country. |
| 3 | `time_position` | `int` | epoch s | 0% | When *this aircraft's* position was last updated. See G4. |
| 4 | `last_contact` | `int` | epoch s | 0% | Last message of any kind from the aircraft. |
| 5 | `longitude` | `float` | degrees | 0%² | WGS-84. |
| 6 | `latitude` | `float` | degrees | 0%² | WGS-84. |
| 7 | `baro_altitude` | `float` \| `int` \| `null` | **metres** | **19.0%** | Barometric altitude. |
| 8 | `on_ground` | `bool` | — | 0% | Surface position report. 31 of 137 were on the ground. |
| 9 | `velocity` | `float` \| `int` | **m/s** | 0%² | Ground speed, **not** airspeed. Observed 0–278.7. |
| 10 | `true_track` | `float` \| `int` | degrees | 0%² | Clockwise from true north (north = 0°). |
| 11 | `vertical_rate` | `float` \| `int` \| `null` | **m/s** | **22.6%** | Positive = climbing. Observed −11.38 to +16.91. |
| 12 | `sensors` | `null` | — | **100%** | Receiver serials. Always null unless you filter by serial. |
| 13 | `geo_altitude` | `float` \| `null` | **metres** | **24.8%** | GNSS altitude. Runs 114–678 m *above* `baro_altitude` (median 572 m). Not a substitute — see G11. |
| 14 | `squawk` | `str` \| `null` | — | **16.8%** | Transponder code, as a string (leading zeros matter: `'0533'`). |
| 15 | `spi` | `bool` | — | 0% | Special-purpose indicator. All `False` in the sample. |
| 16 | `position_source` | `int` | enum | 0% | All 137 were `0` (ADS-B). |
| 17 | `category` | `int` | enum | 0% | Requires `extended=1`. **Near-useless in practice** — see G3. |

¹ Zero *nulls*, but one aircraft returned an **empty string**. See G1.
² Zero nulls in this sample, but the spec permits null and it does occur — filter defensively.

### Query parameters

| Param | Type | Notes |
| --- | --- | --- |
| `time` | int | Epoch seconds. Anonymous: current time only. |
| `icao24` | str | Repeatable for multiple aircraft. |
| `lamin` / `lomin` / `lamax` / `lomax` | float | Bounding box. **Cheaper — use it.** |
| `extended` | int | `1` adds field 17 (`category`). Verified: without it rows are **17** wide, with it **18**. |

### `position_source` (index 16)

| Value | Meaning |
| --- | --- |
| 0 | ADS-B |
| 1 | ASTERIX |
| 2 | MLAT |
| 3 | FLARM |

### `category` (index 17)

| Value | Meaning | Observed in sample |
| --- | --- | --- |
| 0 | No information at all | **133 of 137** |
| 1 | No ADS-B Emitter Category Information | 2 |
| 2 | Light (< 15500 lbs) | — |
| 3 | Small (15500–75000 lbs) | — |
| 4 | Large (75000–300000 lbs) | 1 |
| 5 | High Vortex Large (e.g. B-757) | — |
| 6 | Heavy (> 300000 lbs) | — |
| 7 | High Performance (>5g, >400 kt) | — |
| 8 | Rotorcraft | — |
| 9 | Glider / sailplane | — |
| 10 | Lighter-than-air | — |
| 11 | Parachutist / Skydiver | — |
| 12 | Ultralight / hang-glider / paraglider | — |
| 13 | Reserved | 1 |
| 14 | Unmanned Aerial Vehicle | — |
| 15 | Space / Trans-atmospheric vehicle | — |
| 16 | Surface Vehicle — Emergency | — |
| 17 | Surface Vehicle — Service | — |
| 18 | Point Obstacle (incl. tethered balloons) | — |
| 19 | Cluster Obstacle | — |
| 20 | Line Obstacle | — |

---

## 4. `/tracks/all` — flight track

Anonymous-accessible **only** with `time=0` (live). Requires `icao24` (lowercase hex).

```json
{
  "icao24": "500494",
  "callsign": "T7AFZ   ",
  "startTime": 1785252258.0,
  "endTime": 1785255603.0,
  "path": [ [1785252258, 51.8697, -0.3967, 304, 251, false], ... ]
}
```

Top-level keys: `icao24`, `callsign`, `startTime`, `endTime`, `path`.

### Waypoint fields — observed

| # | Field | Type(s) observed | Unit | Null rate |
| --- | --- | --- | --- | --- |
| 0 | `time` | `int` | epoch s | 0% |
| 1 | `latitude` | `float` | degrees | 0% |
| 2 | `longitude` | `float` | degrees | 0% |
| 3 | `baro_altitude` | `int` | **metres** | 0% |
| 4 | `true_track` | `int` | degrees | 0% |
| 5 | `on_ground` | `bool` | — | 0% |

The live track covers the **whole flight so far**, not just recent history — the sampled track ran
55 minutes and 134 waypoints from near London to Switzerland. Waypoint spacing is **irregular**:
min 7 s, max 84 s, mean 25.2 s. Any velocity or rate derived from consecutive waypoints must divide
by the actual `time` delta.

---

## 5. `/flights/*` — flight summaries

Requires credentials. **Unlike `/states` and `/tracks`, the response is a JSON array of
*objects*** — named keys, not positional arrays. Exactly the 12 documented keys were present, with no
undocumented extras, across all four endpoints.

### Flight object fields — observed

Null rates below are from `/flights/all` (863 flights, 1-hour window).

| Property | Type observed | Unit | Null rate | Notes |
| --- | --- | --- | --- | --- |
| `icao24` | `str` | — | 0% | 850 distinct across 863 flights. |
| `firstSeen` | `int` \| **`null`** | epoch s | 0% here, **0.2% in arrivals** | Docs say non-null integer. **They're wrong** — see G13. |
| `estDepartureAirport` | `str` \| `null` | ICAO code | **44.1%** | Frequently unresolvable. |
| `lastSeen` | `int` | epoch s | 0% | |
| `estArrivalAirport` | `str` \| `null` | ICAO code | **37.3%** | Frequently unresolvable. |
| `callsign` | `str` \| `null` | — | 0.1% | Space-padded to 8 chars, same as state vectors (G1). |
| `estDepartureAirportHorizDistance` | `int` \| `null` | **metres** | 44.1% | Observed 500–14,926. |
| `estDepartureAirportVertDistance` | `int` \| `null` | **metres** | 44.1% | Observed 2–1,999. |
| `estArrivalAirportHorizDistance` | `int` \| `null` | **metres** | 37.3% | Observed 114–29,973. |
| `estArrivalAirportVertDistance` | `int` \| `null` | **metres** | 37.3% | Observed 0–2,429. |
| `departureAirportCandidatesCount` | `int` | count | 0% | Observed 0–32. |
| `arrivalAirportCandidatesCount` | `int` | count | 0% | Observed 0–74. Non-zero for 511 of 863. |

The five departure fields are null **as a group**, and likewise the arrival fields: a null
`est*Airport` always coincides with null `*HorizDistance` and `*VertDistance`. Verified on every
flight in the sample and asserted in the test suite.

### Airport resolution is unreliable — plan for it

This is the headline caveat for anything airport-based:

| Endpoint | n | Window | `estDepartureAirport` null | `estArrivalAirport` null |
| --- | --- | --- | --- | --- |
| `/flights/all` | 863 | 1 h | **44.1%** | **37.3%** |
| `/flights/arrival` (EDDF) | 664 | 24 h | 18.8% | 0% |
| `/flights/departure` (EDDF) | 590 | 24 h | 0% | 9.5% |
| `/flights/aircraft` (`3c6444`) | 7 | 24 h | 0% | 0% |

The airport you *query for* is always populated (0% null, and it always equalled `EDDF` — never a
mismatch in 1,254 flights). It is the **other** end of the flight that goes missing. And even a
populated value may be wrong: `arrivalAirportCandidatesCount` reached **74**, meaning the API found
74 other plausible airports nearby.

Flight durations (`lastSeen − firstSeen`) were sane: median 19 min for the 1-hour `/flights/all`
window (truncated by the window), 101 min median for EDDF arrivals, max 1,489 min (24.8 h) — the
latter suggesting `firstSeen`/`lastSeen` sometimes span more than one real flight leg.

### Window limits and cost

Documented: `/flights/all` ≤ 2 hours; `/flights/aircraft`, `/flights/arrival`, `/flights/departure`
≤ 2 days, previous day or earlier only. A 24-hour window on the previous UTC day was accepted by all
three. Each `/flights/*` call cost **exactly 30 credits** (confirmed: 3880 → 3850 → 3820 → 3790 →
3760), so they are **30× more expensive** than a bounded `/states/all` call. At 4,000/day that is
about 133 calls.

---

## 6. Credits and rate limits

Allowance: **400 credits/day** anonymous, **4,000** on this standard account (header read 3999 on a
fresh run), 8,000 for active feeders.

`/states/all` cost scales with bounding-box area:

| Box area | Credits |
| --- | --- |
| ≤ 25 sq° (or filtered by serial) | 1 |
| 25–100 sq° | 2 |
| 100–400 sq° | 3 |
| > 400 sq°, or unbounded/global | 4 |

Confirmed: the ~9 sq° Alps box cost exactly 1 credit per call; the global snapshot cost 4. So a
bounded box is 4× cheaper — and returned 137 relevant aircraft instead of 12,996 mostly-irrelevant
ones. Observed cost per `/flights/*` call: **30 credits**.

### The `X-Rate-Limit-Remaining` header is per-endpoint-family

This is easy to misread. **Each endpoint family has its own counter, and they all report through the
same header name.** Evidence from one run:

```text
/states   3994 -> 3993 -> 3992          (1 credit per bounded call, strictly monotonic)
/flights  3850 -> 3820 -> 3790 -> 3760  (30 credits per call)
/tracks   3988 -> 3984                  (its own counter again)
```

A `/states` call issued straight after the `/flights` calls read **3992**, not 3760 — because it is a
different counter. Consequences:

- **Don't treat the header as a single global budget**, and don't compare values across families.
- **Don't compute "credits spent" by subtracting across a mixed run** — it produces nonsense
  (an early version of the probe script reported spending −3 credits this way).
- A dedicated `/states` polling loop *does* see a clean monotonic countdown, so budgeting a
  single-family workload from the header is sound.
- **403 responses omit the header entirely** — you cannot read your balance from a refusal.

Exceeding an allowance returns `429 Too Many Requests`. State resolution is 10 s anonymous,
5 s authenticated.

---

## 7. Gotchas

Each one is visible in the probe output; several cost real debugging time.

**G1 — `callsign` has three distinct "no callsign" shapes.** It can be `null`, an **empty string**
(observed: 1 of 137), or 8-char space-padded (`'T7AFZ   '`). Normalise with `.strip() or None`;
`.strip()` alone leaves you with `''`, which will silently fail an equality join rather than raise.

**G2 — `/tracks/all` returns `startTime`/`endTime` as JSON *floats*** in scientific notation
(`1.785254909E9`), unlike every other timestamp in the API. `int()` them before use. Confusingly the
per-waypoint `time` inside the same response *is* an `int`.

**G3 — `category` is present but nearly empty.** Even with `extended=1`, 133 of 137 aircraft
reported `0` ("No information at all"). Only 1 gave a usable weight class. Do not build wake-turbulence
or aircraft-type logic on this field; it needs an external ICAO24-to-type registry.

**G4 — a snapshot is *not* time-synchronised.** Each aircraft carries its own `time_position`, which
lagged the envelope `time` by a median of 1 s and a maximum of 1091 s. That extreme is misleading on
its own, though: the worst offenders are aircraft **parked on the ground with `velocity` 0**, which
haven't moved at all. The figure that matters is airborne-only — worst observed **309 s stale**,
which at that aircraft's own reported ground speed is **~17 km of positional uncertainty**, and
**7 of 106** airborne aircraft were more than 60 s stale. Against a 3–5 NM (5.6–9.3 km) horizontal
separation threshold, 17 km of error is larger than the thing being measured. Filter on
`time - time_position`, or dead-reckon forward using `velocity`/`true_track`.

**G5 — numeric fields mix `int` and `float` for the same field.** `baro_altitude` came back as
`12192` (int) and `5775.96` (float) in one response. Never assume `float`; use `float(x)` or let
pandas coerce.

**G6 — error bodies are `text/plain`, not JSON.** Calling `.json()` on a 403 raises `ValueError`.
Branch on `content-type`, and note `/states/own` returns an empty body with *no* `content-type`.

**G7 — a `404` with an empty body means "no flights found"**, not a bad request.

**G8 — the path is `/tracks/all`**, though the REST docs' endpoint table calls it `/tracks`.

**G9 — the official `opensky_api` Python client returns `None` for both rate-limiting and HTTP
failure.** `_get_json` ends in a bare `return None`, so a 403, a timeout, and a client-side throttle
are indistinguishable. This is exactly what made `get_flights_from_interval` look broken in the
original spike when it was really a permissions 403. The client also self-throttles `get_states` to
one call per 10 s anonymously. **Use `requests` directly when you need to know why something failed.**

**G10 — everything is metric.** Altitudes and both airport distances in metres, `velocity` and
`vertical_rate` in m/s. Aviation separation standards are in feet and nautical miles, so conversion
is mandatory: `m × 3.28084 = ft`, `m/s × 1.94384 = kt`, `m/s × 196.85 = ft/min`.

**G11 — `geo_altitude` is not a fallback for a missing `baro_altitude`.** Across the 103 aircraft
reporting both, `geo_altitude` ran **114–678 m higher** (median 572 m). Vertical separation minima
are defined on *pressure* altitude, and 572 m is ~1900 ft — nearly twice the 1000 ft standard. Filling
a null `baro_altitude` from `geo_altitude` would manufacture false separation violations. Aircraft
with no barometric altitude simply cannot be tested for vertical separation.

**G12 — a `200` does not mean iterable data: `states` can be `null`.** Authenticated `/states/own` on
an account with no ADS-B receiver returns HTTP 200 with body `{"time": 0, "states": null}` — `null`,
not `[]`. `for s in payload["states"]` raises `TypeError`, and `len()` on it does too. Always use
`payload.get("states") or []`. Note `time` is `0` rather than a real timestamp here.

**G13 — `firstSeen` can be `null`, contradicting the docs.** The official field table types
`firstSeen` and `lastSeen` as plain non-nullable integers. Observed: 1 of 664 EDDF arrivals had
`firstSeen: null` (`DLH758`, an inbound whose departure was never seen — `estDepartureAirport` was
null too). A naive `lastSeen - firstSeen` duration therefore raises `TypeError` on real data; it did
so while this document was being written. `lastSeen` was never null in 2,124 flights sampled, but
given `firstSeen` breaks its contract, guard both.

**G14 — the `/flights/*` response is objects, everything else is arrays.** `/states/all` and
`/tracks/all` return positional bare arrays; `/flights/*` returns named-key objects. Two different
parsing strategies in one API.

---

## 8. Implications for conflict analytics

Consequences of the above for the eventual separation work:

1. **Unit conversion is not optional.** Standard separation minima are 1000 ft vertical and 3–5 NM
   horizontal; the API speaks metres and m/s (G10).
2. **Time alignment comes before geometry.** Airborne position staleness reached 309 s, worth ~17 km
   of positional uncertainty at the aircraft's own speed — larger than the 3–5 NM threshold being
   tested (G4). Filter on age, then dead-reckon. Most aircraft are fresh (median 1 s), so this is a
   tail problem, which makes it easy to miss and important to handle.
3. **Nulls must be filtered explicitly.** ~19% of rows lack `baro_altitude` and ~23% lack
   `vertical_rate`. Those aircraft cannot be tested for vertical separation, and `geo_altitude` must
   *not* be substituted (G11) — it sits a median 572 m higher, which would fabricate violations.
4. **History is available, but only one hour of it.** With credentials, `?time=` reaches 1 hour back
   at 5 s resolution — enough to reconstruct a short time series on demand, and enough to replay a
   detected encounter. Anything longer-term still requires polling `/states/all` and persisting it
   yourself. The 1-hour ceiling is the main architectural constraint: a conflict detector can analyse
   the recent past ad hoc, but a historical study needs its own datastore.
5. **Budget with bounding boxes, and mind the separate counters.** At 1 credit per bounded call and
   4,000/day, one region can be polled continuously at **5 s for ~5.5 hours**, or every 30 s for a
   full day. Global snapshots cost 4× for data you would mostly discard. Because each family has its
   own counter (§6), `/flights/*` spending does not eat into the `/states` polling budget — but at 30
   credits a call, `/flights/*` is only good for ~133 calls/day.
6. **`icao24` is the only trustworthy join key.** `callsign` is unreliable (G1) and reused across
   flights; `category` is empty (G3).
7. **`/flights/*` is for context, not detection.** It gives no positions — only endpoints and
   timestamps — so it cannot contribute to separation geometry. Its value is labelling an encounter
   after the fact ("which flights were these?"), and even then 37–44% of airport fields are null
   (§5). Treat airport attribution as best-effort.
8. **`/tracks/all` is the cheapest source of true trajectories.** Unlike a state-vector snapshot it
   gives a full flight path in one call, with `on_ground` per waypoint. But spacing is irregular
   (7–84 s) and it carries no velocity, so speeds must be differentiated from consecutive waypoints
   using the actual time delta.

---

## Sources

- [OpenSky REST API docs](https://openskynetwork.github.io/opensky-api/rest.html)
- [`docs/free/rest.rst`](https://github.com/openskynetwork/opensky-api/blob/master/docs/free/rest.rst)
  and [`flight-response.rst`](https://github.com/openskynetwork/opensky-api/blob/master/docs/free/flight-response.rst)
- [openskynetwork/opensky-api](https://github.com/openskynetwork/opensky-api) — `opensky_api.py` v1.4.0
- Live probes captured by [scripts/explore_opensky.py](../scripts/explore_opensky.py), 2026-07-28
