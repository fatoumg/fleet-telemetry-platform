"""The naive batch poller: `SELECT ... WHERE updated_at > watermark`, on a loop.

This is phase 2 step 1 and it is meant to be replaced. It works, and building it first is what
makes the CDC configuration in step 2 mean anything -- every key in that connector answers a
failure reproduced here (design spec section 13, lines 370-376).

Three failures to find by experiment, not by reading:

  1. It never sees a DELETE. A deleted row has no updated_at to exceed the watermark, and no
     query over current state can return a row that is not there.
  2. It misses a row changed twice between two polls. It reads state, not changes, so two
     commits collapse into one observation.
  3. It competes with the application for the OLTP database, on the same connection budget.

WATERMARK COLUMNS DIFFER PER TABLE, AND THAT IS NOT A DETAIL.
`pings` has no updated_at (docker/oltp/init.sql:214-224) -- it is append-only, so there is
nothing to update -- and watermarks on `server_ts` instead. The mutable tables carry updated_at
maintained by a database TRIGGER rather than by application code, deliberately: if the app
forgot to set it on one path the poller would silently skip those rows, which is a data-loss
bug that looks like nothing (docker/oltp/init.sql:28-34).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

# Table -> the column a watermark can be built from. depots is included even though it changes
# almost never: "changes rarely" and "never changes" differ, and only one of them is safe to
# leave out.
POLLED_TABLES: dict[str, str] = {
    "depots": "updated_at",
    "drivers": "updated_at",
    "vehicles": "updated_at",
    "jobs": "updated_at",
    "job_events": "created_at",
    "pings": "server_ts",
}


def next_watermark(rows: Sequence[dict[str, Any]], column: str, current: datetime) -> datetime:
    """The highest value seen, never lower than where we already were.

    Two guards, both earned:

    An empty result returns `current` rather than `now()`. Advancing on an empty poll is the
    classic watermark leak -- a transaction that committed with an earlier timestamp but became
    visible afterwards is then permanently behind the mark and never read.

    The result is clamped at `current` because the clock can go backwards. The profiler measured
    the container stepping back roughly 2.7 seconds every 30 under WSL2
    (docs/source-system-reference.md, section 8). With a strict `>` comparison a retreating
    watermark would skip every row committed in between.
    """
    highest = current
    for row in rows:
        value = row[column]
        if value is not None and value > highest:
            highest = value
    return highest
