"""Register and inspect the Debezium connector over the Kafka Connect REST API.

Not a shell script, for two reasons: this repository is Windows-first, and the connector config
contains a password that must come from the one place that knows where settings come from
(src/fleet_telemetry/config.py) rather than being committed.

stdlib urllib rather than httpx, because httpx lives in the [app] extra and this is [warehouse]
work -- an import that happens to resolve on a developer's machine and not in a container is
exactly the mistake pyproject.toml:24-29 already records.

    python -m fleet_telemetry.ingest.connector --register
    python -m fleet_telemetry.ingest.connector --status
    python -m fleet_telemetry.ingest.connector --delete

WHY EACH KEY IN docker/debezium/fleet-connector.json IS WHAT IT IS.

  plugin.name = pgoutput
      Postgres 16's built-in logical decoding output plugin. decoderbufs and wal2json need a
      shared library installed into the database image; pgoutput needs nothing, which is why
      docker/docker-compose.yml can use postgres:16-alpine unmodified.

  database.hostname = oltp, database.port = 5432
      NOT config.oltp().host. This config is evaluated by Connect, inside the compose network,
      where services address each other by service name on the container port. The 127.0.0.1
      and 55433 a developer uses do not exist in there. Same trap as the api service's
      environment block in docker-compose.yml.

  slot.name = fleet_debezium
      One named slot, within the max_replication_slots=4 the oltp service sets. Naming it means
      `select * from pg_replication_slots` is readable, and a stale slot can be dropped
      deliberately rather than found by accident after the disk fills.

  publication.autocreate.mode = filtered
      Debezium creates a publication covering only the tables in table.include.list. The
      default, `all_tables`, publishes every table in the database including ones added later.

  snapshot.mode = initial
      On first start, read every existing row and emit it as op='r'. That is ~172,800 ping rows
      at the phase 1 baseline and it takes a minute. Worth it: without the snapshot, bronze
      begins mid-history and nothing downstream can be rebuilt from it.

  tombstones.on.delete = false
      Debezium's default follows a delete with a null-value message so log-compacted topics can
      drop the key. These topics are not compacted, and the tombstone carries nothing the
      preceding op='d' event lacks. The consumer still handles one if it appears -- recorded,
      never dropped -- because "should not happen" is not a guarantee.

  heartbeat.interval.ms = 10000
      The one nobody expects. Debezium only advances the slot's confirmed LSN when it emits
      something. With a captured table that goes quiet, the slot stops advancing while the WAL
      keeps growing, and the symptom is a disk filling with a healthy-looking connector. A
      heartbeat forces the advance.

  topic.creation.default.replication.factor = 1, topic.creation.default.partitions = 1
      One partition per table, deliberately. Ordering in Kafka is per partition, so a second
      partition would let two changes to the same row arrive out of order -- and a Type 2
      dimension built from a reordered change stream is wrong in a way that looks plausible.
      One partition also makes (partition, offset) a total order per topic, which is what
      bronze's deduplication index relies on. Replication factor 1 because there is one broker.

      `topic.creation.enable` does NOT belong here, though it reads like it does: it is a
      WORKER property. Setting it in connector config makes Connect validate the whole
      topic.creation group and fail the connector with
        Missing required configuration "topic.creation.default.replication.factor"
      which names a key you never set and does not mention the one you did.

  NO ExtractNewRecordState SMT
      The popular `unwrap` transform flattens the envelope to just the after-image and turns
      deletes into tombstones. That discards the before-image and the op code -- the two fields
      Type 2 dimensions are built from (dbt/models/staging/_sources.yml:53-65). Bronze keeps
      the envelope; Silver unwraps it.
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from string import Template
from typing import Any

from fleet_telemetry import config

CONFIG_PATH = config.PROJECT_ROOT / "docker" / "debezium" / "fleet-connector.json"


def load_config(path: Path | None = None) -> dict[str, Any]:
    """Read the connector definition and substitute the credential placeholders.

    Template.substitute rather than an f-string: the file is valid JSON on disk, so it can be
    linted and diffed, and a missing variable raises rather than silently producing the literal
    string "${OLTP_PASSWORD}" as a password -- which would present as an authentication failure
    against a database that is demonstrably up.
    """
    oltp = config.oltp()
    raw = (path or CONFIG_PATH).read_text(encoding="utf-8")
    filled = Template(raw).substitute(OLTP_USER=oltp.user, OLTP_PASSWORD=oltp.password)
    return json.loads(filled)


def _request(method: str, path: str, body: dict | None = None) -> Any:
    url = f"{config.kafka().connect_url.rstrip('/')}{path}"
    data = json.dumps(body).encode() if body is not None else None
    # The URL is built from config, not from user input, and points at a local Connect API.
    request = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = response.read().decode()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{method} {url} -> {exc.code}: {exc.read().decode()}") from exc
    return json.loads(payload) if payload else {}


def register() -> dict:
    """Create or update the connector. Idempotent -- PUT of the config, not POST of the whole."""
    definition = load_config()
    return _request("PUT", f"/connectors/{definition['name']}/config", definition["config"])


def status(retries: int = 10, delay: float = 1.5) -> dict:
    """Current connector and task state.

    Retries a 404. Creating a connector is asynchronous: the PUT returns as soon as the config
    is accepted, and the status endpoint answers "No status found for connector fleet-oltp"
    until the herder has actually started it. Asking once immediately after --register raises
    on a connector that is in fact being created perfectly well.
    """
    for attempt in range(retries):
        try:
            return _request("GET", f"/connectors/{load_config()['name']}/status")
        except RuntimeError as exc:
            if "404" not in str(exc) or attempt == retries - 1:
                raise
            time.sleep(delay)
    raise RuntimeError("unreachable")


def delete() -> None:
    """Removes the connector but NOT the replication slot.

    Debezium leaves the slot behind on purpose, so a connector can be recreated and resume. The
    consequence is that deleting a connector you do not intend to recreate leaves Postgres
    retaining WAL forever. Drop it deliberately:

        select pg_drop_replication_slot('fleet_debezium');
    """
    _request("DELETE", f"/connectors/{load_config()['name']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage the Debezium connector.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--register", action="store_true")
    group.add_argument("--status", action="store_true")
    group.add_argument("--delete", action="store_true")
    args = parser.parse_args()

    if args.register:
        register()
        print(json.dumps(status(), indent=2))
    elif args.status:
        print(json.dumps(status(), indent=2))
    else:
        delete()
        print("connector deleted; the replication slot is still there -- see the docstring")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
