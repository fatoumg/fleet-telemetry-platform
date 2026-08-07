# Archive — aviation conflict analytics

Superseded on 2026-08-07 by
[the fleet telemetry platform design](../superpowers/specs/2026-08-07-telemetry-platform-design.md).

Nothing here is current. It is kept because the measurements were real and the reasoning is
worth not repeating.

| File | What it is |
| --- | --- |
| [2026-08-03-aviation-conflict-design.md](2026-08-03-aviation-conflict-design.md) | The abandoned design. Its §7 corrections to the source data model, §9 testing approach and §5 layer-boundary rule all carried forward |
| [opensky-api-reference.md](opensky-api-reference.md) | Every OpenSky field with measured type, unit and null rate, plus 14 gotchas. Accurate as of 2026-07-28 and still the best reference on that API in this repo |
| [opensky-conflict-coverage-evidence.json](opensky-conflict-coverage-evidence.json) | Raw probe manifest behind the density figures below |

## Why it was abandoned

Four failures, one cause: **no control over the data supply.**

| Attempt | Outcome |
| --- | --- |
| ACLED conflict events | Authenticates, then `403 Access denied` on every `/api/*/read`. Account entitlement, not a request defect |
| OpenSky over conflict zones | Donbas, Yemen, Sudan all returned **0 aircraft**, against Switzerland's 1,856/Mkm². ADI is `0/0` |
| OpenSky over The Gambia | 0 aircraft over the country, 2 across all West Africa. Not absent traffic — absent volunteer receivers |
| Global Fishing Watch | Another token, another wait |

The density figures come from authenticated snapshots on 2026-08-03, preserved in the evidence
file. They were verified against a wide-area box and a second snapshot an hour apart, so they are
not a time-of-day artifact.

The bulk sample captures (~5.5 MB of raw `/states/all` and `/flights/*` responses) were deleted;
they are regenerable from the exploration scripts in this repo's git history, at
`scripts/explore_opensky.py` as of commit `7125a54`.
