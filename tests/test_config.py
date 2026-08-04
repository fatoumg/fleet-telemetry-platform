"""Guards on credential resolution.

Two properties matter here and neither is visible from reading the happy path:

  * precedence -- an environment variable must beat the legacy `credentials.json`, or a CI
    override silently does nothing;
  * secrets never leak -- `describe()` output lands in terminals and CI logs.

Offline, and hermetic: every test passes an explicit env mapping and tmp_path root, so the
developer's real `.env` can neither influence a result nor be read by the suite.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from aviation_conflict import config


@pytest.fixture
def legacy_json(tmp_path):
    """Write a `credentials.json` into an isolated root and return that root."""

    def _write(payload: dict | str) -> Path:
        body = payload if isinstance(payload, str) else json.dumps(payload)
        (tmp_path / "credentials.json").write_text(body, encoding="utf-8")
        return tmp_path

    return _write


# ------------------------------------------------------------------------------------
# presence and absence
# ------------------------------------------------------------------------------------


def test_all_sources_absent_return_none(tmp_path):
    assert config.opensky({}, tmp_path) is None
    assert config.acled({}, tmp_path) is None
    assert config.cds({}, tmp_path) is None


def test_blank_values_count_as_unset(tmp_path):
    """`.env.example` ships every key empty; a copied-but-unfilled `.env` is not configured."""
    env = {"ACLED_USERNAME": "", "ACLED_PASSWORD": "   ", "CDS_API_KEY": ""}
    assert config.acled(env, tmp_path) is None
    assert config.cds(env, tmp_path) is None


def test_partial_credentials_return_none(tmp_path):
    """Half a credential pair is unusable -- do not hand callers a half-built object."""
    assert config.opensky({"OPENSKY_CLIENT_ID": "id-only"}, tmp_path) is None
    assert config.opensky({"OPENSKY_CLIENT_SECRET": "secret-only"}, tmp_path) is None
    assert config.acled({"ACLED_USERNAME": "someone@example.org"}, tmp_path) is None


def test_credentials_from_env(tmp_path):
    creds = config.opensky({"OPENSKY_CLIENT_ID": "cid", "OPENSKY_CLIENT_SECRET": "csec"}, tmp_path)
    assert creds == config.OpenSkyCredentials(client_id="cid", client_secret="csec")


# ------------------------------------------------------------------------------------
# precedence
# ------------------------------------------------------------------------------------


def test_env_var_beats_legacy_json(legacy_json):
    root = legacy_json({"clientId": "from-json", "clientSecret": "secret-json"})
    creds = config.opensky(
        {"OPENSKY_CLIENT_ID": "from-env", "OPENSKY_CLIENT_SECRET": "secret-env"}, root
    )
    assert creds is not None
    assert (creds.client_id, creds.client_secret) == ("from-env", "secret-env")


def test_legacy_json_used_when_env_absent(legacy_json):
    """`credentials.json` predates `.env`; explore_opensky.py must keep working."""
    root = legacy_json({"clientId": "from-json", "clientSecret": "secret-json"})
    creds = config.opensky({}, root)
    assert creds is not None
    assert (creds.client_id, creds.client_secret) == ("from-json", "secret-json")


def test_malformed_legacy_json_does_not_raise(legacy_json):
    """A corrupt file should degrade to anonymous, not crash a probe run."""
    assert config.opensky({}, legacy_json("{not json")) is None


def test_non_dict_legacy_json_does_not_raise(legacy_json):
    assert config.opensky({}, legacy_json("[1, 2, 3]")) is None


def test_legacy_json_is_opensky_only(legacy_json):
    """ACLED and CDS have no legacy path -- they must not pick up stray JSON keys."""
    root = legacy_json({"username": "a@b.c", "password": "pw", "key": "k"})
    assert config.acled({}, root) is None
    assert config.cds({}, root) is None


def test_cds_url_defaults_when_only_key_set(tmp_path):
    creds = config.cds({"CDS_API_KEY": "abc123"}, tmp_path)
    assert creds is not None
    assert creds.url == config.CDS_DEFAULT_URL


def test_cds_url_override_respected(tmp_path):
    creds = config.cds(
        {"CDS_API_KEY": "abc123", "CDS_API_URL": "https://example.test/api"}, tmp_path
    )
    assert creds is not None
    assert creds.url == "https://example.test/api"


# ------------------------------------------------------------------------------------
# secrets never leak
# ------------------------------------------------------------------------------------


def test_mask_hides_content_but_reports_length():
    masked = config.mask("supersecretvalue")
    assert "supersecretvalue" not in masked
    assert "16" in masked


def test_mask_handles_missing():
    assert config.mask(None) == "MISSING"
    assert config.mask_email(None) == "MISSING"


def test_mask_email_keeps_domain_only():
    masked = config.mask_email("fatou.gaye@primeforge.io")
    assert "fatou.gaye" not in masked
    assert masked.endswith("@primeforge.io")


def test_mask_email_without_at_sign():
    masked = config.mask_email("notanemail")
    assert "notanemail" not in masked


def test_describe_never_contains_a_secret(tmp_path):
    """The single most important test in this file."""
    secrets = {
        "OPENSKY_CLIENT_SECRET": "os-secret-must-not-appear",
        "ACLED_PASSWORD": "acled-pw-must-not-appear",
        "CDS_API_KEY": "cds-key-must-not-appear",
    }
    env = {
        "OPENSKY_CLIENT_ID": "opensky-client-id",
        "ACLED_USERNAME": "someone@example.org",
        **secrets,
    }
    rendered = " ".join(detail for _, _, detail in config.describe(env, tmp_path))
    for value in secrets.values():
        assert value not in rendered, f"{value!r} leaked into describe() output"
    # the ACLED account local-part is also an identifier worth not publishing
    assert "someone@" not in rendered


def test_describe_reports_configured_flags(tmp_path):
    status = {name: ok for name, ok, _ in config.describe({"CDS_API_KEY": "abc123"}, tmp_path)}
    assert status == {"OpenSky": False, "ACLED": False, "Copernicus CDS": True}


def test_describe_explains_why_unconfigured_sources_matter(tmp_path):
    details = {name: detail for name, _, detail in config.describe({}, tmp_path)}
    assert "anonymously" in details["OpenSky"]
    assert "403" in details["ACLED"]
