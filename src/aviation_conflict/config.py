"""Credential loading for the three upstream sources.

One place that knows where secrets come from, so no probe script has to. Resolution order
for every value:

    1. a real environment variable  (CI, container, `export`)
    2. `.env` in the project root   (local development)
    3. `credentials.json`           (OpenSky only -- predates `.env`, still honoured)

Rules 1 and 2 collapse into one lookup: `load_dotenv` runs at import and does not overwrite
variables that are already set, so by the time anything here reads the environment the
precedence is settled.

Nothing here raises on a missing credential. Each loader returns ``None`` so callers can
decide: OpenSky degrades to anonymous access, while ACLED and ERA5 have no anonymous mode
and must fail loudly at their own call site.

Secrets are never printed. ``describe()`` reports presence and length only.

Every loader takes optional ``env`` and ``root`` arguments. Production callers omit them and
get the process environment and the real project root; tests pass explicit values and stay
hermetic. Without that seam the only way to test resolution is to reload this module and
mutate ``os.environ``, which leaks a developer's real `.env` into the suite.

Run it directly to see what is currently configured:

    python -m aviation_conflict.config
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# src/aviation_conflict/config.py -> src/aviation_conflict -> src -> project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
CREDENTIALS_JSON_NAME = "credentials.json"

load_dotenv(ENV_PATH)

CDS_DEFAULT_URL = "https://cds.climate.copernicus.eu/api"


# --------------------------------------------------------------------------------------
# credential shapes
# --------------------------------------------------------------------------------------
#
# Deliberately three separate types rather than one bag of strings: the three sources use
# three different auth schemes, and a function that needs an ACLED password should not be
# handed something that might be an OpenSky client secret.


@dataclass(frozen=True)
class OpenSkyCredentials:
    """OAuth2 client-credentials grant against auth.opensky-network.org."""

    client_id: str
    client_secret: str


@dataclass(frozen=True)
class AcledCredentials:
    """OAuth2 *password* grant against acleddata.com/oauth/token.

    ACLED retired API keys; the account email and password are the credentials.
    """

    username: str
    password: str


@dataclass(frozen=True)
class CdsCredentials:
    """Copernicus Climate Data Store token, as consumed by `cdsapi.Client(url=, key=)`."""

    url: str
    key: str


# --------------------------------------------------------------------------------------
# lookup helpers
# --------------------------------------------------------------------------------------


def _get(env: Mapping[str, str] | None, name: str) -> str | None:
    """Read a variable, treating blank and whitespace-only as unset.

    The `.env.example` template ships every key with an empty value, so a copied-but-unfilled
    `.env` would otherwise look configured.
    """
    source = os.environ if env is None else env
    value = source.get(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _legacy_opensky_json(root: Path | None) -> tuple[str | None, str | None]:
    """Fall back to `credentials.json`, which `explore_opensky.py` shipped with."""
    path = (PROJECT_ROOT if root is None else root) / CREDENTIALS_JSON_NAME
    if not path.exists():
        return None, None
    try:
        creds = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None, None
    if not isinstance(creds, dict):
        return None, None
    return creds.get("clientId"), creds.get("clientSecret")


# --------------------------------------------------------------------------------------
# loaders
# --------------------------------------------------------------------------------------


def opensky(
    env: Mapping[str, str] | None = None, root: Path | None = None
) -> OpenSkyCredentials | None:
    """OpenSky credentials, or None to run anonymously (400 credits/day, no history)."""
    client_id = _get(env, "OPENSKY_CLIENT_ID")
    client_secret = _get(env, "OPENSKY_CLIENT_SECRET")

    if not (client_id and client_secret):
        legacy_id, legacy_secret = _legacy_opensky_json(root)
        client_id = client_id or legacy_id
        client_secret = client_secret or legacy_secret

    if not (client_id and client_secret):
        return None
    return OpenSkyCredentials(client_id=client_id, client_secret=client_secret)


def acled(
    env: Mapping[str, str] | None = None, root: Path | None = None
) -> AcledCredentials | None:
    """ACLED credentials. There is no anonymous mode -- /api/acled/read returns 403."""
    username = _get(env, "ACLED_USERNAME")
    password = _get(env, "ACLED_PASSWORD")
    if not (username and password):
        return None
    return AcledCredentials(username=username, password=password)


def cds(env: Mapping[str, str] | None = None, root: Path | None = None) -> CdsCredentials | None:
    """Copernicus CDS credentials.

    Only *downloads* need these -- the catalogue endpoints under /api/catalogue/v1 are
    public, which is how the ERA5 variable list was verified without an account.
    """
    key = _get(env, "CDS_API_KEY")
    if not key:
        return None
    return CdsCredentials(url=_get(env, "CDS_API_URL") or CDS_DEFAULT_URL, key=key)


# --------------------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------------------


def mask(secret: str | None) -> str:
    """Render a secret as presence plus length, never its content."""
    if not secret:
        return "MISSING"
    return f"set ({len(secret)} chars)"


def mask_email(address: str | None) -> str:
    """Show enough of an email to tell two accounts apart, without publishing it."""
    if not address:
        return "MISSING"
    local, _, domain = address.partition("@")
    if not domain:
        return f"{local[:1]}***"
    return f"{local[:1]}***@{domain}"


def describe(
    env: Mapping[str, str] | None = None, root: Path | None = None
) -> list[tuple[str, bool, str]]:
    """Status of each source as (name, configured, human-readable detail)."""
    sky = opensky(env, root)
    acl = acled(env, root)
    store = cds(env, root)

    return [
        (
            "OpenSky",
            sky is not None,
            f"client_id={sky.client_id} secret={mask(sky.client_secret)}"
            if sky
            else "not configured -- will run anonymously (400 credits/day, no history)",
        ),
        (
            "ACLED",
            acl is not None,
            f"username={mask_email(acl.username)} password={mask(acl.password)}"
            if acl
            else "not configured -- /api/acled/read will return 403",
        ),
        (
            "Copernicus CDS",
            store is not None,
            f"url={store.url} key={mask(store.key)}"
            if store
            else "not configured -- catalogue readable, downloads will fail",
        ),
    ]


def main() -> int:
    legacy = PROJECT_ROOT / CREDENTIALS_JSON_NAME
    print(f"project root     : {PROJECT_ROOT}")
    print(f".env             : {'found' if ENV_PATH.exists() else 'MISSING'}")
    print(
        f"{CREDENTIALS_JSON_NAME} : "
        f"{'found (legacy OpenSky fallback)' if legacy.exists() else 'absent'}"
    )
    print()

    rows = describe()
    width = max(len(name) for name, _, _ in rows)
    for name, ok, detail in rows:
        print(f"  {'OK' if ok else '--'}  {name:<{width}}  {detail}")

    missing = [name for name, ok, _ in rows if not ok]
    print()
    if missing:
        print(f"unconfigured: {', '.join(missing)}")
        print("see .env.example for how to obtain each one")
    else:
        print("all three sources configured")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
