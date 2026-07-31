# aviation-conflict-analytics

Analysis of aircraft proximity and loss-of-separation events using live ADS-B data from the
[OpenSky Network](https://opensky-network.org/).

Current status: **API exploration.** No conflict-detection logic exists yet. The work so far
establishes what data OpenSky actually returns and in what format, which is a prerequisite for
designing the separation analysis.

## What's here

| Path | Purpose |
| --- | --- |
| [docs/opensky-api-reference.md](docs/opensky-api-reference.md) | **Start here.** Which endpoints are reachable, every field with its type/unit/nullability, and 11 documented gotchas. |
| [scripts/explore_opensky.py](scripts/explore_opensky.py) | Probes each endpoint, saves raw responses, and profiles the schemas from real data. |
| [tests/test_state_vector_schema.py](tests/test_state_vector_schema.py) | Guards the wire-format contract the reference doc claims. Offline. |
| `data/samples/` | Captured raw responses (git-ignored; regenerate with the script). |

## Running the exploration

Needs `requests`, `pandas`, and `opensky-api` (the last only for its field-name constants).

```bash
python scripts/explore_opensky.py          # bounded box, ~3 credits of the 400/day anonymous
python scripts/explore_opensky.py --global # adds the unbounded global snapshot (+4 credits)
python scripts/explore_opensky.py --no-save
```

Then run the schema guard, which uses the captured samples and needs no network:

```bash
python -m pytest tests/ -v
```

## Access

Runs **anonymously** — no credentials configured. That reaches `/states/all` and live `/tracks/all`;
all four `/flights/*` endpoints and `/states/own` return `403`. Anonymous access is capped at 400
credits/day with 10-second resolution and **no history**.

Registering for OAuth2 client credentials would add 1 hour of state history at 5-second resolution
plus the historical endpoints — see the Authentication section of the
[reference doc](docs/opensky-api-reference.md) for the flow and the full trade-off.

## Notes

`temp.py` and `temp.ipynb` are the original scratch spike and are git-ignored; the notebook carries
~2.6 MB of embedded output. Their findings are superseded by the reference doc.
