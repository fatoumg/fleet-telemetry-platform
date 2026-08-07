"""Configuration for the local platform: two databases, a broker, and an API token.

One place that knows where settings come from, so nothing else has to. Resolution order for
every value:

    1. a real environment variable  (CI, container, `export`)
    2. `.env` in the project root   (local development)
    3. a built-in default           (local docker-compose)

Rules 1 and 2 collapse into one lookup: `load_dotenv` runs at import and does not overwrite
variables that are already set, so by the time anything here reads the environment the
precedence is settled.

Two things differ from the credential loader this replaces, and both are deliberate.

**Defaults now exist.** The previous version returned ``None`` for a missing credential,
because there is no sensible default for someone else's API secret -- you either hold it or
you do not. Local infrastructure is the opposite case: the compose file and this module ship
the same defaults, so `docker compose up` followed by `pytest` works on a fresh clone with no
`.env` at all. There is nothing to configure before the project runs.

**A default password is therefore load-bearing, and dangerous outside local use.** So every
value tracks whether it was explicitly set or fell back, and ``describe()`` prints that
distinction. A password reading "default" in a deployed environment is a finding, not a
detail.

Secrets are never printed. Passwords render as presence plus length; DSNs render with the
password replaced.

Every loader takes optional ``env`` and ``root`` arguments. Production callers omit them and
get the process environment; tests pass explicit values and stay hermetic. Without that seam
the only way to test resolution is to mutate ``os.environ``, which leaks a developer's real
`.env` into the suite.

Run it directly to see what is currently configured:

    python -m fleet_telemetry.config
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv

# src/fleet_telemetry/config.py -> src/fleet_telemetry -> src -> project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ENV_PATH = PROJECT_ROOT / ".env"

load_dotenv(ENV_PATH)


# --------------------------------------------------------------------------------------
# defaults
# --------------------------------------------------------------------------------------
#
# These mirror docker/docker-compose.yml exactly. If one side changes, change both -- a
# silent mismatch presents as an authentication failure, which reads as a credentials bug
# and sends you looking in the wrong place. That already cost a day once, on the port.
#
# The two databases get deliberately different names, users and ports. Connecting to the
# wrong one should fail loudly rather than quietly succeeding against the wrong data.

OLTP_DEFAULTS = {
    "host": "127.0.0.1",
    "port": "55433",
    "database": "fleet",
    "user": "fleet",
    "password": "fleet",
}

WAREHOUSE_DEFAULTS = {
    "host": "127.0.0.1",
    # 55432, far from 5432: a locally installed PostgreSQL holds 5432, and on Windows a
    # container may bind a port a host service already has. See docker-compose.yml.
    "port": "55432",
    "database": "telemetry",
    "user": "telemetry",
    "password": "telemetry",
}

KAFKA_DEFAULT_BOOTSTRAP = "127.0.0.1:19092"

# The app authenticates with a static bearer token. It is not real security; it exists so the
# simulator behaves like a client rather than writing to the database behind the app's back.
API_TOKEN_DEFAULT = "local-dev-token"


# --------------------------------------------------------------------------------------
# shapes
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class DatabaseConfig:
    """Connection details for one Postgres instance."""

    host: str
    port: int
    database: str
    user: str
    password: str
    # Names of the fields that fell back to a default rather than being set explicitly.
    defaulted: frozenset[str] = frozenset()

    def dsn(self) -> str:
        """A libpq URI. Contains the password -- never log this.

        `safe=""` matters: quote() leaves `/` alone by default, so a username or password
        containing one would slip through unescaped and silently produce a URI pointing at a
        different host or database rather than failing.
        """
        return (
            f"postgresql://{quote(self.user, safe='')}:{quote(self.password, safe='')}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    def safe_dsn(self) -> str:
        """The same URI with the password redacted. Safe for logs and error messages."""
        return (
            f"postgresql://{quote(self.user, safe='')}:***@{self.host}:{self.port}/{self.database}"
        )


@dataclass(frozen=True)
class KafkaConfig:
    """Broker endpoint. Redpanda locally; wire-compatible with Kafka."""

    bootstrap_servers: str
    defaulted: bool = False


# --------------------------------------------------------------------------------------
# lookup helpers
# --------------------------------------------------------------------------------------


def _get(env: Mapping[str, str] | None, name: str) -> str | None:
    """Read a variable, treating blank and whitespace-only as unset.

    `.env.example` ships keys with values, but a hand-edited `.env` can easily end up with an
    empty one, which should mean "unset" rather than "the empty string".
    """
    source = os.environ if env is None else env
    value = source.get(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


# Field name -> environment-variable suffix. Only `database` differs, and it has to: the
# compose file and dbt/profiles.yml both use `WAREHOUSE_DB`, so deriving `WAREHOUSE_DATABASE`
# from the field name would silently ignore the value everything else in the project sets.
# That exact bug shipped once and was caught only because describe() reports which fields fell
# back to a default -- without that, the symptom would have been dbt and Python quietly
# connecting to different databases.
_ENV_SUFFIX = {"database": "DB"}


def env_var_for(prefix: str, field: str) -> str:
    """The environment variable that configures one field, e.g. ("WAREHOUSE", "database") -> WAREHOUSE_DB.

    Public so tests can build names the same way this module does, rather than duplicating the
    mapping and letting the two drift.
    """
    return f"{prefix}_{_ENV_SUFFIX.get(field, field.upper())}"


def _database(
    env: Mapping[str, str] | None, prefix: str, defaults: Mapping[str, str]
) -> DatabaseConfig:
    """Build a DatabaseConfig from `{PREFIX}_HOST`, `{PREFIX}_DB` etc., recording fallbacks."""
    resolved: dict[str, str] = {}
    defaulted: set[str] = set()
    for field, fallback in defaults.items():
        value = _get(env, env_var_for(prefix, field))
        if value is None:
            value = fallback
            defaulted.add(field)
        resolved[field] = value

    port = resolved.pop("port")
    if not port.isdigit():
        raise ValueError(f"{prefix}_PORT must be a number, got {port!r}")

    return DatabaseConfig(port=int(port), defaulted=frozenset(defaulted), **resolved)


# --------------------------------------------------------------------------------------
# loaders
# --------------------------------------------------------------------------------------


def oltp(env: Mapping[str, str] | None = None) -> DatabaseConfig:
    """The operational database the application writes to.

    Deliberately separate from the warehouse: an application bug must not be able to corrupt
    analytics, the two have different backup needs, and pointing analytics at the operational
    database is the anti-pattern the warehouse exists to remove.
    """
    return _database(env, "OLTP", OLTP_DEFAULTS)


def warehouse(env: Mapping[str, str] | None = None) -> DatabaseConfig:
    """The analytics warehouse: Postgres + PostGIS + TimescaleDB. Read by dbt and the loader."""
    return _database(env, "WAREHOUSE", WAREHOUSE_DEFAULTS)


def kafka(env: Mapping[str, str] | None = None) -> KafkaConfig:
    """Broker carrying Debezium change events out of the OLTP database."""
    value = _get(env, "KAFKA_BOOTSTRAP_SERVERS")
    return KafkaConfig(bootstrap_servers=value or KAFKA_DEFAULT_BOOTSTRAP, defaulted=value is None)


def api_token(env: Mapping[str, str] | None = None) -> str:
    """Bearer token the simulator presents to the application."""
    return _get(env, "FLEET_API_TOKEN") or API_TOKEN_DEFAULT


# --------------------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------------------


def mask(secret: str | None) -> str:
    """Render a secret as presence plus length, never its content."""
    if not secret:
        return "MISSING"
    return f"set ({len(secret)} chars)"


def describe(env: Mapping[str, str] | None = None) -> list[tuple[str, bool, str]]:
    """Status of each component as (name, explicitly_configured, human-readable detail).

    The middle element is False when every value fell back to a default. That is correct and
    expected locally; anywhere else it means configuration did not arrive.
    """
    op = oltp(env)
    wh = warehouse(env)
    broker = kafka(env)
    token = api_token(env)
    token_defaulted = _get(env, "FLEET_API_TOKEN") is None

    def detail(db: DatabaseConfig) -> str:
        note = f" [defaults: {', '.join(sorted(db.defaulted))}]" if db.defaulted else ""
        return f"{db.safe_dsn()} password={mask(db.password)}{note}"

    return [
        ("OLTP", not op.defaulted, detail(op)),
        ("Warehouse", not wh.defaulted, detail(wh)),
        (
            "Kafka",
            not broker.defaulted,
            broker.bootstrap_servers + (" [default]" if broker.defaulted else ""),
        ),
        ("API token", not token_defaulted, mask(token) + (" [default]" if token_defaulted else "")),
    ]


def main() -> int:
    print(f"project root : {PROJECT_ROOT}")
    print(f".env         : {'found' if ENV_PATH.exists() else 'absent (using defaults)'}")
    print()

    rows = describe()
    width = max(len(name) for name, _, _ in rows)
    for name, explicit, text in rows:
        print(f"  {'set ' if explicit else 'dflt'}  {name:<{width}}  {text}")

    defaulted = [name for name, explicit, _ in rows if not explicit]
    print()
    if defaulted:
        print(f"using built-in defaults for: {', '.join(defaulted)}")
        print("expected for local development; see .env.example to override")
    else:
        print("everything explicitly configured")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
